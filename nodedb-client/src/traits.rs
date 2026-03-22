//! The `NodeDb` trait: unified query interface for both Origin and Lite.
//!
//! Application code writes against this trait once. The runtime determines
//! whether queries execute locally (in-memory engines on Lite) or remotely
//! (pgwire to Origin).
//!
//! All methods are `async` — on native this runs on Tokio, on WASM this
//! runs on `wasm-bindgen-futures`.

use async_trait::async_trait;

use nodedb_types::document::Document;
use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
use nodedb_types::value::Value;

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
#[async_trait]
pub trait NodeDb: Send + Sync {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Mock implementation to verify the trait is object-safe and
    /// can be used as `Arc<dyn NodeDb>`.
    struct MockDb;

    #[async_trait]
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
}
