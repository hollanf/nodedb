//! The `NodeDb` trait: unified query interface for both Origin and Lite.
//!
//! Application code writes against this trait once. The runtime determines
//! whether queries execute locally (in-memory engines on Lite) or remotely
//! (pgwire to Origin).
//!
//! All methods are `async` — on native this runs on Tokio, on WASM this
//! runs on `wasm-bindgen-futures`.

use std::sync::Arc;

use async_trait::async_trait;

use nodedb_types::document::Document;
use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::protocol::Limits;
use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

/// Event passed to `NodeDb::on_collection_purged` handlers.
///
/// Emitted on the sync client when Origin pushes a `CollectionPurged`
/// wire message and on Lite after local hard-delete completes, so
/// application code can flush UI caches, drop derived indexes, etc.
/// Handler callsites must not block — the dispatch path is on the
/// sync client's receive loop.
#[derive(Debug, Clone)]
pub struct CollectionPurgedEvent {
    pub tenant_id: u64,
    pub name: String,
    /// WAL LSN at which the purge was applied. Handlers can compare
    /// this against locally-observed LSNs for resume/replay logic.
    pub purge_lsn: u64,
}

/// Handler registered via `NodeDb::on_collection_purged`. Fn-ref
/// (not FnMut) so the same handler can fire from multiple threads
/// without interior mutability ceremony at every call site.
pub type CollectionPurgedHandler = Arc<dyn Fn(CollectionPurgedEvent) + Send + Sync + 'static>;

/// Marker bound for `NodeDb` and the futures it returns.
///
/// On native targets the bound is `Send + Sync` — matching the multi-thread
/// Tokio runtime that backs both Origin and the desktop / mobile-FFI Lite
/// callers. On `wasm32` the bound is empty: JS is single-threaded, so
/// requiring `Send` on futures returned by the trait would force every
/// `!Send` engine internal (redb transactions, `Rc<...>`, etc.) to be
/// rewritten for no benefit.
///
/// The `#[async_trait]` attribute on the trait + each impl is correspondingly
/// cfg-swapped between the default (`Send` futures) and `?Send` (no `Send`
/// bound) variants.
#[cfg(not(target_arch = "wasm32"))]
pub trait NodeDbMarker: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> NodeDbMarker for T {}

#[cfg(target_arch = "wasm32")]
pub trait NodeDbMarker {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> NodeDbMarker for T {}

/// Unified database interface for NodeDB.
///
/// Two implementations:
/// - `NodeDbLite`: executes queries against in-memory HNSW/CSR/Loro engines
///   on the edge device. Writes produce CRDT deltas synced to Origin in background.
/// - `NodeDbRemote`: translates trait calls into parameterized SQL and sends
///   them over pgwire to the Origin cluster.
///
/// The developer writes agent logic once. Switching between local and cloud
/// is a one-line configuration change.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeDb: NodeDbMarker {
    // ─── Vector Operations ───────────────────────────────────────────

    /// Search for the `k` nearest vectors to `query` in `collection`.
    ///
    /// Returns results ordered by ascending distance. Optional metadata
    /// filter constrains which vectors are considered.
    ///
    /// On Lite: direct in-memory HNSW search. Sub-millisecond.
    /// On Remote: translated to `SELECT ... ORDER BY embedding <-> $1 LIMIT $2`.
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>>;

    /// Insert a vector with optional metadata into `collection`.
    ///
    /// On Lite: inserts into in-memory HNSW + emits CRDT delta + persists to SQLite.
    /// On Remote: translated to `INSERT INTO collection (id, embedding, metadata) VALUES (...)`.
    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()>;

    /// Delete a vector by ID from `collection`.
    ///
    /// On Lite: marks deleted in HNSW + emits CRDT tombstone.
    /// On Remote: `DELETE FROM collection WHERE id = $1`.
    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    // ─── Graph Operations ────────────────────────────────────────────

    /// Traverse the graph from `start` up to `depth` hops.
    ///
    /// Returns the discovered subgraph (nodes + edges). Optional edge filter
    /// constrains which edges are followed during traversal.
    ///
    /// On Lite: direct CSR pointer-chasing in contiguous memory. Microseconds.
    /// On Remote: `SELECT * FROM graph_traverse($1, $2, $3)`.
    async fn graph_traverse(
        &self,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph>;

    /// Insert a directed edge from `from` to `to` with the given label.
    ///
    /// Returns the generated edge ID.
    ///
    /// On Lite: appends to mutable adjacency buffer + CRDT delta + SQLite.
    /// On Remote: `INSERT INTO edges (src, dst, label, properties) VALUES (...)`.
    async fn graph_insert_edge(
        &self,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId>;

    /// Delete a graph edge by ID.
    ///
    /// On Lite: marks deleted + CRDT tombstone.
    /// On Remote: `DELETE FROM edges WHERE id = $1`.
    async fn graph_delete_edge(&self, edge_id: &EdgeId) -> NodeDbResult<()>;

    // ─── Document Operations ─────────────────────────────────────────

    /// Get a document by ID from `collection`.
    ///
    /// On Lite: direct Loro state read. Sub-millisecond.
    /// On Remote: `SELECT * FROM collection WHERE id = $1`.
    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>>;

    /// Put (insert or update) a document into `collection`.
    ///
    /// The document's `id` field determines the key. If a document with that
    /// ID already exists, it is overwritten (last-writer-wins locally; CRDT
    /// merge on sync).
    ///
    /// On Lite: Loro apply + CRDT delta + SQLite persist.
    /// On Remote: `INSERT ... ON CONFLICT (id) DO UPDATE SET ...`.
    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()>;

    /// Delete a document by ID from `collection`.
    ///
    /// On Lite: Loro delete + CRDT tombstone.
    /// On Remote: `DELETE FROM collection WHERE id = $1`.
    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()>;

    // ─── Named Vector Fields ──────────────────────────────────────────

    /// Insert a vector into a named field within a collection.
    ///
    /// Enables multiple embeddings per collection (e.g., "title_embedding",
    /// "body_embedding") with independent HNSW indexes.
    /// Default: delegates to `vector_insert()` ignoring field_name.
    async fn vector_insert_field(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let _ = field_name;
        self.vector_insert(collection, id, embedding, metadata)
            .await
    }

    /// Search a named vector field.
    ///
    /// Default: delegates to `vector_search()` ignoring field_name.
    async fn vector_search_field(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let _ = field_name;
        self.vector_search(collection, query, k, filter).await
    }

    // ─── Graph Shortest Path ────────────────────────────────────────

    /// Find the shortest path between two nodes.
    ///
    /// Returns the path as a list of node IDs, or None if no path exists
    /// within `max_depth` hops. Uses bidirectional BFS.
    async fn graph_shortest_path(
        &self,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        let _ = (from, to, max_depth, edge_filter);
        Ok(None)
    }

    // ─── Text Search ────────────────────────────────────────────────

    /// Full-text search with BM25 scoring.
    ///
    /// Returns document IDs with relevance scores, ordered by descending score.
    /// Pass [`TextSearchParams::default()`] for standard OR-mode non-fuzzy search.
    async fn text_search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let _ = (collection, query, top_k, params);
        Ok(Vec::new())
    }

    // ─── Batch Operations ───────────────────────────────────────────

    /// Batch insert vectors — amortizes CRDT delta export to O(1) per batch.
    async fn batch_vector_insert(
        &self,
        collection: &str,
        vectors: &[(&str, &[f32])],
    ) -> NodeDbResult<()> {
        for &(id, embedding) in vectors {
            self.vector_insert(collection, id, embedding, None).await?;
        }
        Ok(())
    }

    /// Batch insert graph edges — amortizes CRDT delta export to O(1) per batch.
    async fn batch_graph_insert_edges(&self, edges: &[(&str, &str, &str)]) -> NodeDbResult<()> {
        for &(from, to, label) in edges {
            self.graph_insert_edge(&NodeId::new(from), &NodeId::new(to), label, None)
                .await?;
        }
        Ok(())
    }

    // ─── Connection Metadata ─────────────────────────────────────────────

    /// The protocol version negotiated during the connection handshake.
    ///
    /// Returns `0` when no handshake was performed (e.g. pre-T2-01 servers
    /// or implementations that do not maintain a persistent connection).
    fn proto_version(&self) -> u16 {
        0
    }

    /// The raw capability bitfield advertised by the server.
    ///
    /// Returns `0` when no handshake was performed. Use
    /// `Capabilities::from_raw(self.capabilities())` for named predicates.
    fn capabilities(&self) -> u64 {
        0
    }

    /// The server version string from `HelloAckFrame` (e.g. `"0.1.0-dev"`).
    ///
    /// Returns an empty string when no handshake was performed.
    fn server_version(&self) -> String {
        String::new()
    }

    /// Per-operation limits announced by the server.
    ///
    /// All fields are `None` when no handshake was performed — the caller
    /// should treat `None` as "no server-side cap" for that dimension.
    fn limits(&self) -> Limits {
        Limits::default()
    }

    // ─── SQL Escape Hatch ────────────────────────────────────────────

    /// Execute a raw SQL query with parameters.
    ///
    /// On Lite: requires the `sql` feature flag (compiles in DataFusion parser).
    ///   Returns `NodeDbError::SqlNotEnabled` if the feature is not compiled in.
    /// On Remote: pass-through to Origin via pgwire.
    ///
    /// For most AI agent workloads, the typed methods above are sufficient
    /// and faster. Use this for BI tools, existing ORMs, or ad-hoc queries.
    async fn execute_sql(&self, query: &str, params: &[Value]) -> NodeDbResult<QueryResult>;

    // ─── Collection Lifecycle (soft-delete / undrop / hard-delete) ───

    /// Restore a soft-deleted collection within its retention window.
    ///
    /// Equivalent to `UNDROP COLLECTION <name>`. Fails with 42P01 if
    /// the retention window has elapsed and the row is gone, or with
    /// 42501 if the caller is neither preserved owner nor admin.
    ///
    /// Default impl routes through `execute_sql` so any implementation
    /// that can execute SQL inherits the correct behavior for free.
    async fn undrop_collection(&self, name: &str) -> NodeDbResult<()> {
        let sql = format!("UNDROP COLLECTION {}", quote_ident(name));
        self.execute_sql(&sql, &[]).await?;
        Ok(())
    }

    /// Hard-delete a collection, skipping soft-delete and retention.
    ///
    /// Equivalent to `DROP COLLECTION <name> PURGE`. Admin-only on the
    /// server; the server rejects non-admin callers with 42501.
    /// Bypasses the retention safety net — data is unrecoverable.
    async fn drop_collection_purge(&self, name: &str) -> NodeDbResult<()> {
        let sql = format!("DROP COLLECTION {} PURGE", quote_ident(name));
        self.execute_sql(&sql, &[]).await?;
        Ok(())
    }

    /// List every soft-deleted collection in the current tenant that
    /// is still within its retention window.
    ///
    /// Equivalent to `SELECT tenant_id, name, owner, deactivated_at_ns,
    /// retention_expires_at_ns FROM _system.dropped_collections`.
    /// Returns `Vec<DroppedCollection>` — empty if no soft-deleted rows
    /// exist for the caller's tenant.
    async fn list_dropped_collections(&self) -> NodeDbResult<Vec<DroppedCollection>> {
        let sql = "SELECT tenant_id, name, owner, engine_type, \
                   deactivated_at_ns, retention_expires_at_ns \
                   FROM _system.dropped_collections";
        let result = self.execute_sql(sql, &[]).await?;
        parse_dropped_collection_rows(&result)
    }

    /// Register a handler fired when a collection the caller has
    /// synced is purged on Origin and the local copy is removed.
    ///
    /// Default impl returns `NodeDbError::storage` with a
    /// `"not supported"` detail — implementations that maintain a
    /// sync client (Lite, any future push-capable remote client)
    /// override with registration into their internal handler list.
    /// Stateless clients (pgwire-only `NodeDbRemote`) have nothing
    /// to push, so the default rejection is the correct behavior.
    async fn on_collection_purged(&self, _handler: CollectionPurgedHandler) -> NodeDbResult<()> {
        Err(NodeDbError::storage(
            "on_collection_purged is not supported on this client — \
             requires a push-capable sync connection (NodeDbLite or a \
             sync-enabled remote client)",
        ))
    }
}

/// Quote a SQL identifier. Mirrors the pgwire-side rule used by
/// `remote_parse::quote_identifier`: wrap in double-quotes only if
/// the name contains anything other than `[A-Za-z0-9_]` or starts
/// with a digit. Unquoted fast-path keeps the usual case cheap.
fn quote_ident(name: &str) -> String {
    let needs_quote = name.is_empty()
        || name.chars().next().is_some_and(|c| c.is_ascii_digit())
        || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if needs_quote {
        let escaped = name.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        name.to_string()
    }
}

/// Decode `_system.dropped_collections` rows into
/// `Vec<DroppedCollection>`. Each row is a `Vec<Value>` aligned with
/// the column order declared in the SELECT list above.
fn parse_dropped_collection_rows(result: &QueryResult) -> NodeDbResult<Vec<DroppedCollection>> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        if row.len() < 6 {
            return Err(NodeDbError::storage(format!(
                "dropped_collections row has {} columns; expected 6 \
                 (tenant_id, name, owner, engine_type, deactivated_at_ns, \
                 retention_expires_at_ns)",
                row.len()
            )));
        }
        out.push(DroppedCollection {
            tenant_id: value_as_u64(&row[0])?,
            name: value_as_string(&row[1])?,
            owner: value_as_string(&row[2])?,
            engine_type: value_as_string(&row[3])?,
            deactivated_at_ns: value_as_u64(&row[4])?,
            retention_expires_at_ns: value_as_u64(&row[5])?,
        });
    }
    Ok(out)
}

fn value_as_u64(v: &Value) -> NodeDbResult<u64> {
    match v {
        Value::Integer(i) => Ok(*i as u64),
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|e| NodeDbError::storage(format!("parse u64 from '{s}': {e}"))),
        _ => Err(NodeDbError::storage(format!(
            "expected integer for u64 column, got {v:?}"
        ))),
    }
}

fn value_as_string(v: &Value) -> NodeDbResult<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Null => Ok(String::new()),
        other => Err(NodeDbError::storage(format!(
            "expected string column, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::Capabilities;
    use std::collections::HashMap;

    /// Mock implementation to verify the trait is object-safe and
    /// can be used as `Arc<dyn NodeDb>`.
    struct MockDb;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl NodeDb for MockDb {
        async fn vector_search(
            &self,
            _collection: &str,
            _query: &[f32],
            _k: usize,
            _filter: Option<&MetadataFilter>,
        ) -> NodeDbResult<Vec<SearchResult>> {
            Ok(vec![SearchResult {
                id: "vec-1".into(),
                node_id: None,
                distance: 0.1,
                metadata: HashMap::new(),
            }])
        }

        async fn vector_insert(
            &self,
            _collection: &str,
            _id: &str,
            _embedding: &[f32],
            _metadata: Option<Document>,
        ) -> NodeDbResult<()> {
            Ok(())
        }

        async fn vector_delete(&self, _collection: &str, _id: &str) -> NodeDbResult<()> {
            Ok(())
        }

        async fn graph_traverse(
            &self,
            _start: &NodeId,
            _depth: u8,
            _edge_filter: Option<&EdgeFilter>,
        ) -> NodeDbResult<SubGraph> {
            Ok(SubGraph::empty())
        }

        async fn graph_insert_edge(
            &self,
            from: &NodeId,
            to: &NodeId,
            edge_type: &str,
            _properties: Option<Document>,
        ) -> NodeDbResult<EdgeId> {
            Ok(EdgeId::from_components(
                from.as_str(),
                to.as_str(),
                edge_type,
            ))
        }

        async fn graph_delete_edge(&self, _edge_id: &EdgeId) -> NodeDbResult<()> {
            Ok(())
        }

        async fn document_get(
            &self,
            _collection: &str,
            id: &str,
        ) -> NodeDbResult<Option<Document>> {
            let mut doc = Document::new(id);
            doc.set("title", Value::String("test".into()));
            Ok(Some(doc))
        }

        async fn document_put(&self, _collection: &str, _doc: Document) -> NodeDbResult<()> {
            Ok(())
        }

        async fn document_delete(&self, _collection: &str, _id: &str) -> NodeDbResult<()> {
            Ok(())
        }

        async fn execute_sql(&self, _query: &str, _params: &[Value]) -> NodeDbResult<QueryResult> {
            Ok(QueryResult::empty())
        }
    }

    /// Verify the trait is object-safe (can be used as `dyn NodeDb`).
    #[test]
    fn trait_is_object_safe() {
        fn _accepts_dyn(_db: &dyn NodeDb) {}
        let db = MockDb;
        _accepts_dyn(&db);
    }

    /// Verify the trait can be wrapped in `Arc<dyn NodeDb>`.
    #[test]
    fn trait_works_with_arc() {
        use std::sync::Arc;
        let db: Arc<dyn NodeDb> = Arc::new(MockDb);
        // Just verify it compiles — the Arc<dyn> pattern is the primary API.
        let _ = db;
    }

    #[tokio::test]
    async fn mock_vector_search() {
        let db = MockDb;
        let results = db
            .vector_search("embeddings", &[0.1, 0.2, 0.3], 5, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "vec-1");
        assert!(results[0].distance < 1.0);
    }

    #[tokio::test]
    async fn mock_vector_insert_and_delete() {
        let db = MockDb;
        db.vector_insert("coll", "v1", &[1.0, 2.0], None)
            .await
            .unwrap();
        db.vector_delete("coll", "v1").await.unwrap();
    }

    #[tokio::test]
    async fn mock_graph_operations() {
        let db = MockDb;
        let start = NodeId::new("alice");
        let subgraph = db.graph_traverse(&start, 2, None).await.unwrap();
        assert_eq!(subgraph.node_count(), 0);

        let from = NodeId::new("alice");
        let to = NodeId::new("bob");
        let edge_id = db
            .graph_insert_edge(&from, &to, "KNOWS", None)
            .await
            .unwrap();
        assert_eq!(edge_id.as_str(), "alice--KNOWS-->bob");

        db.graph_delete_edge(&edge_id).await.unwrap();
    }

    #[tokio::test]
    async fn mock_document_operations() {
        let db = MockDb;
        let doc = db.document_get("notes", "n1").await.unwrap().unwrap();
        assert_eq!(doc.id, "n1");
        assert_eq!(doc.get_str("title"), Some("test"));

        let mut new_doc = Document::new("n2");
        new_doc.set("body", Value::String("hello".into()));
        db.document_put("notes", new_doc).await.unwrap();

        db.document_delete("notes", "n1").await.unwrap();
    }

    #[tokio::test]
    async fn mock_execute_sql() {
        let db = MockDb;
        let result = db.execute_sql("SELECT 1", &[]).await.unwrap();
        assert_eq!(result.row_count(), 0);
    }

    /// Verify the full "one API, any runtime" pattern from the TDD.
    #[tokio::test]
    async fn unified_api_pattern() {
        use std::sync::Arc;

        // This is the pattern from NodeDB.md:
        // let db: Arc<dyn NodeDb> = Arc::new(NodeDbLite::open(...));
        //   OR
        // let db: Arc<dyn NodeDb> = Arc::new(NodeDbRemote::connect(...));
        //
        // Application code is identical either way:
        let db: Arc<dyn NodeDb> = Arc::new(MockDb);

        let results = db
            .vector_search("knowledge_base", &[0.1, 0.2], 5, None)
            .await
            .unwrap();
        assert!(!results.is_empty());

        let start = NodeId::new(results[0].id.clone());
        let _subgraph = db.graph_traverse(&start, 2, None).await.unwrap();

        let doc = Document::new("note-1");
        db.document_put("notes", doc).await.unwrap();
    }

    /// Default `proto_version()` returns 0 for impls that do not override.
    #[test]
    fn default_proto_version_is_zero() {
        let db = MockDb;
        assert_eq!(db.proto_version(), 0);
    }

    /// Default `capabilities()` returns 0 for impls that do not override.
    #[test]
    fn default_capabilities_is_zero() {
        let db = MockDb;
        assert_eq!(db.capabilities(), 0);
        // Wrapping in Capabilities gives all-false predicates.
        let caps = Capabilities::from_raw(db.capabilities());
        assert!(!caps.supports_streaming());
        assert!(!caps.supports_graphrag());
    }

    /// Default `server_version()` returns an empty string.
    #[test]
    fn default_server_version_is_empty() {
        let db = MockDb;
        assert!(db.server_version().is_empty());
    }

    /// Default `limits()` returns all-None limits.
    #[test]
    fn default_limits_all_none() {
        let db = MockDb;
        let limits = db.limits();
        assert!(limits.max_vector_dim.is_none());
        assert!(limits.max_top_k.is_none());
        assert!(limits.max_scan_limit.is_none());
        assert!(limits.max_batch_size.is_none());
        assert!(limits.max_crdt_delta_bytes.is_none());
        assert!(limits.max_query_text_bytes.is_none());
        assert!(limits.max_graph_depth.is_none());
    }

    /// Capabilities newtype works as documented.
    #[test]
    fn capabilities_newtype_smoke() {
        use nodedb_types::protocol::{CAP_FTS, CAP_STREAMING};
        let caps = Capabilities::from_raw(CAP_STREAMING | CAP_FTS);
        assert!(caps.supports_streaming());
        assert!(caps.supports_fts());
        assert!(!caps.supports_graphrag());
        assert!(!caps.supports_crdt());
    }
}
