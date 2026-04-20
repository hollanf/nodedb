use std::path::Path;
use std::sync::Arc;

use nodedb_types::TenantId;
use redb::{Database, ReadableTable, TableDefinition};

/// Edge table: composite key `(tid, "collection\x00src\x00label\x00dst")` → properties.
///
/// The tenant id is a first-class key component (not a lexical prefix).
/// Collection is the first segment of the composite string so edges in one
/// collection share a `"{collection}\x00"` prefix — enabling O(|collection|)
/// range drops in `purge_collection` without a schema-level third tuple component.
pub(super) const EDGES: TableDefinition<(u32, &str), &[u8]> = TableDefinition::new("edges");

/// Reverse edge index: `(tid, "collection\x00dst\x00label\x00src")` → empty.
/// Mirrors the forward table structurally for inbound traversals.
pub(super) const REVERSE_EDGES: TableDefinition<(u32, &str), &[u8]> =
    TableDefinition::new("reverse_edges");

pub(super) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "graph".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Composite edge key using `\x00` separator — the string portion of
/// the `(tid, key)` tuple stored in redb.
///
/// Format: `collection\x00src\x00label\x00dst`
///
/// Grouping by collection first enables prefix scans over all edges in a
/// collection via `collection\x00` without a full-tenant scan.
pub(super) fn edge_key(collection: &str, src: &str, label: &str, dst: &str) -> String {
    format!("{collection}\x00{src}\x00{label}\x00{dst}")
}

/// Parse a composite edge key back into `(collection, src, label, dst)`.
pub fn parse_edge_key(key: &str) -> Option<(&str, &str, &str, &str)> {
    let mut parts = key.splitn(4, '\x00');
    let collection = parts.next()?;
    let src = parts.next()?;
    let label = parts.next()?;
    let dst = parts.next()?;
    Some((collection, src, label, dst))
}

// Re-export shared Direction from nodedb-types.
pub use nodedb_types::graph::Direction;

/// Decoded edge record yielded by [`EdgeStore::scan_all_edges_decoded`]:
/// `(tenant, collection, src, label, dst, properties)`.
pub type EdgeRecord = (TenantId, String, String, String, String, Vec<u8>);

/// A single edge with its properties.
#[derive(Debug, Clone)]
pub struct Edge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub properties: Vec<u8>,
}

/// redb-backed edge storage for the Knowledge Graph engine.
///
/// Keys are `(TenantId, composite_key)` tuples — tenant routing is
/// structural, not lexical. Each Data Plane core owns its own
/// `EdgeStore` instance; no cross-core sharing.
pub struct EdgeStore {
    pub(super) db: Arc<Database>,
}

impl EdgeStore {
    /// Open or create the edge store database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        let write_txn = db.begin_write().map_err(|e| redb_err("begin_write", e))?;
        {
            let _ = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let _ = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Insert or update an edge under the caller's tenant. Maintains
    /// forward + reverse indexes atomically.
    pub fn put_edge(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
        properties: &[u8],
    ) -> crate::Result<()> {
        let fwd = edge_key(collection, src, label, dst);
        let rev = edge_key(collection, dst, label, src);
        let t = tid.as_u32();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((t, fwd.as_str()), properties)
                .map_err(|e| redb_err("insert edge", e))?;

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((t, rev.as_str()), &[] as &[u8])
                .map_err(|e| redb_err("insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Delete an edge under the caller's tenant. Removes forward +
    /// reverse entries atomically.
    pub fn delete_edge(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<bool> {
        let fwd = edge_key(collection, src, label, dst);
        let rev = edge_key(collection, dst, label, src);
        let t = tid.as_u32();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let existed = {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let existed = edges
                .remove((t, fwd.as_str()))
                .map_err(|e| redb_err("remove edge", e))?
                .is_some();

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .remove((t, rev.as_str()))
                .map_err(|e| redb_err("remove reverse", e))?;

            existed
        };
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(existed)
    }

    /// Delete ALL edges where `node` is source or destination within
    /// the caller's tenant, across all collections (used during document
    /// deletion cascade). Performs a full-tenant scan since the key
    /// format is `collection\x00src\x00label\x00dst` — no single prefix
    /// covers all occurrences of a node across collections.
    pub fn delete_edges_for_node(&self, tid: TenantId, node: &str) -> crate::Result<()> {
        let t = tid.as_u32();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;

            // Collect all forward keys where this node is the src.
            let out_keys: Vec<String> = edges
                .range((t, "")..(t + 1, ""))
                .map_err(|e| redb_err("out range", e))?
                .filter_map(|r| {
                    r.ok().and_then(|(k, _)| {
                        let composite = k.value().1;
                        // Key: collection\x00src\x00label\x00dst — src is part[1].
                        let mut parts = composite.splitn(4, '\x00');
                        let _collection = parts.next()?;
                        let src = parts.next()?;
                        if src == node {
                            Some(composite.to_string())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            for key in &out_keys {
                edges
                    .remove((t, key.as_str()))
                    .map_err(|e| redb_err("remove out edge", e))?;
                // Rebuild the reverse key: collection\x00dst\x00label\x00src.
                if let Some((collection, src, label, dst)) = parse_edge_key(key) {
                    let rev_key = edge_key(collection, dst, label, src);
                    let _ = rev_t.remove((t, rev_key.as_str()));
                }
            }

            // Collect all reverse-index keys where this node is the "src"
            // in the reversed representation (i.e., original dst).
            let in_keys: Vec<String> = rev_t
                .range((t, "")..(t + 1, ""))
                .map_err(|e| redb_err("in range", e))?
                .filter_map(|r| {
                    r.ok().and_then(|(k, _)| {
                        let composite = k.value().1;
                        let mut parts = composite.splitn(4, '\x00');
                        let _collection = parts.next()?;
                        let rev_src = parts.next()?;
                        if rev_src == node {
                            Some(composite.to_string())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            for key in &in_keys {
                rev_t
                    .remove((t, key.as_str()))
                    .map_err(|e| redb_err("remove in edge", e))?;
                // Rebuild the forward key: collection\x00src\x00label\x00dst
                // where in the reverse key, "src" is actually dst and vice versa.
                if let Some((collection, rev_src, label, rev_dst)) = parse_edge_key(key) {
                    // reverse key is collection\x00dst\x00label\x00src,
                    // so forward key is collection\x00rev_dst\x00label\x00rev_src.
                    let fwd_key = edge_key(collection, rev_dst, label, rev_src);
                    let _ = edges.remove((t, fwd_key.as_str()));
                }
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge cascade", e))?;
        Ok(())
    }

    /// Scan all forward edges belonging to a tenant, returning
    /// `(composite_key, properties)` pairs. The composite key is in
    /// `"collection\x00src\x00label\x00dst"` form — use
    /// [`parse_edge_key`] to destructure.
    pub fn scan_edges_for_tenant(&self, tid: TenantId) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let t = tid.as_u32();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        let mut results = Vec::new();
        let range = table
            .range((t, "")..(t + 1, ""))
            .map_err(|e| redb_err("edge range", e))?;
        for entry in range {
            let entry = entry.map_err(|e| redb_err("edge entry", e))?;
            results.push((entry.0.value().1.to_string(), entry.1.value().to_vec()));
        }
        Ok(results)
    }

    /// Scan every forward edge across all tenants, yielding
    /// `(TenantId, collection, src, label, dst, properties)`. Used exclusively
    /// by the CSR rebuild path — no lexical parsing required, tenant is
    /// read directly from the tuple key.
    pub fn scan_all_edges_decoded(&self) -> crate::Result<Vec<EdgeRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        let mut out = Vec::new();
        let range = table.iter().map_err(|e| redb_err("iter", e))?;
        for entry in range {
            let (k, v) = entry.map_err(|e| redb_err("iter", e))?;
            let (t, composite) = k.value();
            if let Some((collection, src, label, dst)) = parse_edge_key(composite) {
                out.push((
                    TenantId::new(t),
                    collection.to_string(),
                    src.to_string(),
                    label.to_string(),
                    dst.to_string(),
                    v.value().to_vec(),
                ));
            }
        }
        Ok(out)
    }

    /// Insert a raw edge record (for snapshot restore). Takes the
    /// tenant + composite key + properties.
    ///
    /// The composite key must be in the form `collection\x00src\x00label\x00dst`.
    pub fn put_edge_raw(
        &self,
        tid: TenantId,
        composite_key: &str,
        properties: &[u8],
    ) -> crate::Result<()> {
        let t = tid.as_u32();
        let rev_key = match parse_edge_key(composite_key) {
            Some((collection, src, label, dst)) => edge_key(collection, dst, label, src),
            None => {
                return Err(crate::Error::BadRequest {
                    detail: format!("put_edge_raw: malformed composite key {composite_key:?}"),
                });
            }
        };

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((t, composite_key), properties)
                .map_err(|e| redb_err("insert edge", e))?;
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((t, rev_key.as_str()), &[] as &[u8])
                .map_err(|e| redb_err("insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit edge", e))?;
        Ok(())
    }

    /// Get a single edge's properties under the caller's tenant.
    pub fn get_edge(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        let key = edge_key(collection, src, label, dst);
        let t = tid.as_u32();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        match table
            .get((t, key.as_str()))
            .map_err(|e| redb_err("get", e))?
        {
            Some(val) => Ok(Some(val.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Scan forward edges under a tenant with a composite-key prefix.
    /// Used internally by `neighbors_out` and friends. The prefix must
    /// include at least the collection component so scanning is bounded.
    pub(super) fn scan_edges_with_prefix<F>(
        &self,
        tid: TenantId,
        prefix: &str,
        mut make_edge: F,
    ) -> crate::Result<Vec<Edge>>
    where
        F: FnMut(&str, &str, &str, &str) -> Edge,
    {
        let t = tid.as_u32();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        let mut edges = Vec::new();
        let range = table
            .range((t, prefix)..)
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            let (key, val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kt, composite) = key.value();
            if kt != t || !composite.starts_with(prefix) {
                break;
            }
            if let Some((collection, src, label, dst)) = parse_edge_key(composite) {
                let mut edge = make_edge(collection, src, label, dst);
                edge.properties = val.value().to_vec();
                edges.push(edge);
            }
        }

        Ok(edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: TenantId = TenantId::new(1);
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    #[test]
    fn put_and_get_edge() {
        let (store, _dir) = make_store();
        let props = b"msgpack-props";
        store
            .put_edge(T, COLL, "alice", "KNOWS", "bob", props)
            .unwrap();

        let result = store.get_edge(T, COLL, "alice", "KNOWS", "bob").unwrap();
        assert_eq!(result, Some(props.to_vec()));
    }

    #[test]
    fn get_nonexistent_edge() {
        let (store, _dir) = make_store();
        let result = store.get_edge(T, COLL, "alice", "KNOWS", "bob").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_edge() {
        let (store, _dir) = make_store();
        store
            .put_edge(T, COLL, "alice", "KNOWS", "bob", b"")
            .unwrap();
        assert!(store.delete_edge(T, COLL, "alice", "KNOWS", "bob").unwrap());
        assert!(!store.delete_edge(T, COLL, "alice", "KNOWS", "bob").unwrap());
        assert!(
            store
                .get_edge(T, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn neighbors_out_all_labels() {
        let (store, _dir) = make_store();
        store
            .put_edge(T, COLL, "alice", "KNOWS", "bob", b"")
            .unwrap();
        store
            .put_edge(T, COLL, "alice", "KNOWS", "carol", b"")
            .unwrap();
        store
            .put_edge(T, COLL, "alice", "WORKS_WITH", "dave", b"")
            .unwrap();

        let edges = store.neighbors_out(T, COLL, "alice", None).unwrap();
        assert_eq!(edges.len(), 3);

        let dst_ids: Vec<&str> = edges.iter().map(|e| e.dst_id.as_str()).collect();
        assert!(dst_ids.contains(&"bob"));
        assert!(dst_ids.contains(&"carol"));
        assert!(dst_ids.contains(&"dave"));
    }

    #[test]
    fn neighbors_out_filtered_by_label() {
        let (store, _dir) = make_store();
        store
            .put_edge(T, COLL, "alice", "KNOWS", "bob", b"")
            .unwrap();
        store
            .put_edge(T, COLL, "alice", "WORKS_WITH", "carol", b"")
            .unwrap();

        let edges = store
            .neighbors_out(T, COLL, "alice", Some("KNOWS"))
            .unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].dst_id, "bob");
    }

    #[test]
    fn neighbors_in() {
        let (store, _dir) = make_store();
        store
            .put_edge(T, COLL, "alice", "KNOWS", "bob", b"")
            .unwrap();
        store
            .put_edge(T, COLL, "carol", "KNOWS", "bob", b"")
            .unwrap();

        let edges = store.neighbors_in(T, COLL, "bob", Some("KNOWS")).unwrap();
        assert_eq!(edges.len(), 2);
        let src_ids: Vec<&str> = edges.iter().map(|e| e.src_id.as_str()).collect();
        assert!(src_ids.contains(&"alice"));
        assert!(src_ids.contains(&"carol"));
    }

    #[test]
    fn neighbors_both() {
        let (store, _dir) = make_store();
        store
            .put_edge(T, COLL, "alice", "KNOWS", "bob", b"")
            .unwrap();
        store
            .put_edge(T, COLL, "carol", "KNOWS", "alice", b"")
            .unwrap();

        let edges = store
            .neighbors(T, COLL, "alice", Some("KNOWS"), Direction::Both)
            .unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn edge_properties_preserved() {
        let (store, _dir) = make_store();
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("weight".into()),
            rmpv::Value::F64(0.95),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();

        store.put_edge(T, COLL, "a", "CITES", "b", &buf).unwrap();

        let loaded = store.get_edge(T, COLL, "a", "CITES", "b").unwrap().unwrap();
        let decoded: rmpv::Value = rmpv::decode::read_value(&mut loaded.as_slice()).unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn put_overwrites_properties() {
        let (store, _dir) = make_store();
        store.put_edge(T, COLL, "a", "L", "b", b"v1").unwrap();
        store.put_edge(T, COLL, "a", "L", "b", b"v2").unwrap();

        let result = store.get_edge(T, COLL, "a", "L", "b").unwrap().unwrap();
        assert_eq!(result, b"v2");
    }

    #[test]
    fn out_degree_and_in_degree() {
        let (store, _dir) = make_store();
        store.put_edge(T, COLL, "a", "X", "b", b"").unwrap();
        store.put_edge(T, COLL, "a", "X", "c", b"").unwrap();
        store.put_edge(T, COLL, "d", "X", "b", b"").unwrap();

        assert_eq!(store.out_degree(T, COLL, "a", None).unwrap(), 2);
        assert_eq!(store.in_degree(T, COLL, "b", None).unwrap(), 2);
        assert_eq!(store.in_degree(T, COLL, "c", None).unwrap(), 1);
    }

    #[test]
    fn inbound_neighbors_carry_properties() {
        let (store, _dir) = make_store();
        store
            .put_edge(T, COLL, "alice", "CITED", "paper1", b"props-data")
            .unwrap();

        let edges = store
            .neighbors_in(T, COLL, "paper1", Some("CITED"))
            .unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].src_id, "alice");
        assert_eq!(edges[0].properties, b"props-data");
    }

    #[test]
    fn tenants_are_isolated() {
        let (store, _dir) = make_store();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        store
            .put_edge(t1, COLL, "alice", "KNOWS", "bob", b"t1")
            .unwrap();
        store
            .put_edge(t2, COLL, "alice", "KNOWS", "bob", b"t2")
            .unwrap();

        assert_eq!(
            store.get_edge(t1, COLL, "alice", "KNOWS", "bob").unwrap(),
            Some(b"t1".to_vec())
        );
        assert_eq!(
            store.get_edge(t2, COLL, "alice", "KNOWS", "bob").unwrap(),
            Some(b"t2".to_vec())
        );

        let t1_edges = store.scan_edges_for_tenant(t1).unwrap();
        assert_eq!(t1_edges.len(), 1);
        let t2_edges = store.scan_edges_for_tenant(t2).unwrap();
        assert_eq!(t2_edges.len(), 1);

        store.purge_tenant(t1).unwrap();
        assert!(
            store
                .get_edge(t1, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.get_edge(t2, COLL, "alice", "KNOWS", "bob").unwrap(),
            Some(b"t2".to_vec())
        );
    }

    #[test]
    fn purge_collection_removes_only_matching_edges() {
        let (store, _dir) = make_store();
        let coll_a = "collA";
        let coll_b = "collB";

        store
            .put_edge(T, coll_a, "alice", "KNOWS", "bob", b"a1")
            .unwrap();
        store
            .put_edge(T, coll_a, "carol", "KNOWS", "dave", b"a2")
            .unwrap();
        store
            .put_edge(T, coll_b, "alice", "KNOWS", "bob", b"b1")
            .unwrap();

        let removed = store.purge_collection(T, coll_a).unwrap();
        assert_eq!(removed, 2, "should have removed 2 forward edges");

        // coll_a edges gone.
        assert!(
            store
                .get_edge(T, coll_a, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_edge(T, coll_a, "carol", "KNOWS", "dave")
                .unwrap()
                .is_none()
        );

        // coll_b edge untouched.
        assert_eq!(
            store.get_edge(T, coll_b, "alice", "KNOWS", "bob").unwrap(),
            Some(b"b1".to_vec())
        );
    }

    #[test]
    fn collections_are_isolated_within_same_tenant() {
        let (store, _dir) = make_store();
        let coll_a = "cA";
        let coll_b = "cB";

        store.put_edge(T, coll_a, "x", "L", "y", b"ca").unwrap();
        store.put_edge(T, coll_b, "x", "L", "y", b"cb").unwrap();

        let a = store.get_edge(T, coll_a, "x", "L", "y").unwrap().unwrap();
        let b = store.get_edge(T, coll_b, "x", "L", "y").unwrap().unwrap();
        assert_eq!(a, b"ca");
        assert_eq!(b, b"cb");
    }
}
