use std::path::Path;
use std::sync::Arc;

use redb::{Database, TableDefinition};
use tracing::{debug, info};

/// Table definition for the primary document store.
/// Key: "{collection}:{document_id}" → Value: document bytes.
const DOCUMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

/// Table definition for secondary indexes.
/// Key: "{collection}:{field}:{value}:{document_id}" → Value: empty (existence index).
const INDEXES: TableDefinition<&str, &[u8]> = TableDefinition::new("indexes");

/// Map a redb error into our crate error with context.
fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("{ctx}: {e}"),
    }
}

// Thread-local reusable buffer for building composite keys without heap allocation.
// Most composite keys are short (collection + doc_id < 256 bytes), so this avoids
// a `format!()` heap allocation on every get/put/delete in the hot path.
std::thread_local! {
    static KEY_BUF: std::cell::RefCell<String> = std::cell::RefCell::new(String::with_capacity(256));
}

/// Build a composite key `"{a}:{b}"` using thread-local buffer, call `f` with the result.
fn with_key2<R>(a: &str, b: &str, f: impl FnOnce(&str) -> R) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        buf.push_str(a);
        buf.push(':');
        buf.push_str(b);
        f(&buf)
    })
}

/// Build a composite key `"{a}:{b}:{c}:{d}"` using thread-local buffer, call `f` with the result.
fn with_key4<R>(a: &str, b: &str, c: &str, d: &str, f: impl FnOnce(&str) -> R) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        buf.push_str(a);
        buf.push(':');
        buf.push_str(b);
        buf.push(':');
        buf.push_str(c);
        buf.push(':');
        buf.push_str(d);
        f(&buf)
    })
}

/// redb-backed B-Tree storage engine for sparse/metadata queries.
///
/// Provides ACID point lookups, range scans, and secondary index support
/// via redb's embedded B-Tree. This is the Sparse & Metadata engine.
///
/// Thread-safety: redb `Database` is `Send + Sync` — safe for Control Plane.
/// For Data Plane (single-core), each core gets its own `SparseEngine` instance
/// or accesses the shared `Database` via read transactions (lock-free readers).
pub struct SparseEngine {
    db: Arc<Database>,
}

impl SparseEngine {
    /// Open or create the sparse engine database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        // Ensure tables exist.
        let write_txn = db.begin_write().map_err(|e| redb_err("write txn", e))?;
        {
            let _ = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open documents table", e))?;
            let _ = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes table", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        info!(path = %path.display(), "sparse engine opened");

        Ok(Self { db: Arc::new(db) })
    }

    /// Insert or update a document.
    pub fn put(&self, collection: &str, document_id: &str, value: &[u8]) -> crate::Result<()> {
        with_key2(collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .insert(key, value)
                    .map_err(|e| redb_err("insert", e))?;
            }
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(collection, document_id, len = value.len(), "document put");
            Ok(())
        })
    }

    /// Point lookup: retrieve a document by collection + document_id.
    pub fn get(&self, collection: &str, document_id: &str) -> crate::Result<Option<Vec<u8>>> {
        with_key2(collection, document_id, |key| {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;

            match table.get(key) {
                Ok(Some(value)) => Ok(Some(value.value().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(redb_err("get", e)),
            }
        })
    }

    /// Delete a document.
    pub fn delete(&self, collection: &str, document_id: &str) -> crate::Result<bool> {
        with_key2(collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            let removed = {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .remove(key)
                    .map_err(|e| redb_err("remove", e))?
                    .is_some()
            };
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(collection, document_id, removed, "document delete");
            Ok(removed)
        })
    }

    /// Range scan: retrieve documents in a collection with keys in [lower, upper).
    ///
    /// The `field` parameter scopes the scan prefix. Returns up to `limit` results.
    pub fn range_scan(
        &self,
        collection: &str,
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("{collection}:{field}:");

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open table", e))?;

        let start = match lower {
            Some(l) => format!("{prefix}{}", String::from_utf8_lossy(l)),
            None => prefix.clone(),
        };
        let end = match upper {
            Some(u) => format!("{prefix}{}", String::from_utf8_lossy(u)),
            None => {
                // Increment last byte for exclusive upper bound.
                let mut end = prefix.clone();
                end.push('\u{ffff}');
                end
            }
        };

        let mut results = Vec::with_capacity(limit.min(256));
        let range = table
            .range(start.as_str()..end.as_str())
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            if results.len() >= limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("range entry", e))?;
            let key = entry.0.value().to_string();
            let value = entry.1.value().to_vec();
            results.push((key, value));
        }

        debug!(collection, field, count = results.len(), "range scan");
        Ok(results)
    }

    /// Insert a secondary index entry for range scan support.
    pub fn index_put(
        &self,
        collection: &str,
        field: &str,
        value: &str,
        document_id: &str,
    ) -> crate::Result<()> {
        with_key4(collection, field, value, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            {
                let mut table = write_txn
                    .open_table(INDEXES)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .insert(key, [].as_slice())
                    .map_err(|e| redb_err("index insert", e))?;
            }
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            Ok(())
        })
    }

    /// Get the underlying database handle (for advanced use / shared access).
    pub fn db(&self) -> &Arc<Database> {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    #[test]
    fn put_and_get() {
        let (engine, _dir) = open_temp();

        engine.put("users", "u1", b"alice").unwrap();
        engine.put("users", "u2", b"bob").unwrap();

        assert_eq!(engine.get("users", "u1").unwrap(), Some(b"alice".to_vec()));
        assert_eq!(engine.get("users", "u2").unwrap(), Some(b"bob".to_vec()));
        assert_eq!(engine.get("users", "u3").unwrap(), None);
    }

    #[test]
    fn put_overwrites() {
        let (engine, _dir) = open_temp();

        engine.put("users", "u1", b"alice").unwrap();
        engine.put("users", "u1", b"ALICE").unwrap();

        assert_eq!(engine.get("users", "u1").unwrap(), Some(b"ALICE".to_vec()));
    }

    #[test]
    fn delete_removes() {
        let (engine, _dir) = open_temp();

        engine.put("users", "u1", b"alice").unwrap();
        assert!(engine.delete("users", "u1").unwrap());
        assert_eq!(engine.get("users", "u1").unwrap(), None);
        // Double delete returns false.
        assert!(!engine.delete("users", "u1").unwrap());
    }

    #[test]
    fn range_scan_with_index() {
        let (engine, _dir) = open_temp();

        // Insert index entries: users by age.
        engine.index_put("users", "age", "025", "u1").unwrap();
        engine.index_put("users", "age", "030", "u2").unwrap();
        engine.index_put("users", "age", "035", "u3").unwrap();
        engine.index_put("users", "age", "040", "u4").unwrap();

        // Scan age [025, 036).
        let results = engine
            .range_scan("users", "age", Some(b"025"), Some(b"036"), 10)
            .unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].0.contains("025"));
        assert!(results[2].0.contains("035"));
    }

    #[test]
    fn range_scan_respects_limit() {
        let (engine, _dir) = open_temp();

        for i in 0..20 {
            engine
                .index_put("logs", "ts", &format!("{i:04}"), &format!("doc{i}"))
                .unwrap();
        }

        let results = engine.range_scan("logs", "ts", None, None, 5).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn collections_are_isolated() {
        let (engine, _dir) = open_temp();

        engine.put("users", "u1", b"alice").unwrap();
        engine.put("orders", "u1", b"order-1").unwrap();

        assert_eq!(engine.get("users", "u1").unwrap(), Some(b"alice".to_vec()));
        assert_eq!(
            engine.get("orders", "u1").unwrap(),
            Some(b"order-1".to_vec())
        );
    }
}
