//! Collection metadata operations for the system catalog.

use super::types::{COLLECTIONS, StoredCollection, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Store a collection record.
    pub fn put_collection(&self, coll: &StoredCollection) -> crate::Result<()> {
        let key = format!("{}:{}", coll.tenant_id, coll.name);
        let bytes =
            zerompk::to_msgpack_vec(coll).map_err(|e| catalog_err("serialize collection", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("open collections", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert collection", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load all collections for a tenant.
    pub fn load_collections_for_tenant(
        &self,
        tenant_id: u32,
    ) -> crate::Result<Vec<StoredCollection>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        let mut colls = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range collections", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read collection", e))?;
            if key.value().starts_with(&prefix) {
                let coll: StoredCollection = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser collection", e))?;
                if coll.is_active {
                    colls.push(coll);
                }
            }
        }
        Ok(colls)
    }

    /// Load every soft-deleted collection across all tenants.
    ///
    /// Used by the Event-Plane collection-GC sweeper to enumerate
    /// retention candidates, and by `_system.dropped_collections` to
    /// surface them to operators. Returns a fresh `Vec` each call —
    /// the row count is bounded by the retention window, not by
    /// workload.
    pub fn load_dropped_collections(&self) -> crate::Result<Vec<StoredCollection>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        let mut colls = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range collections", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read collection", e))?;
            let coll: StoredCollection = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser collection", e))?;
            if !coll.is_active {
                colls.push(coll);
            }
        }
        Ok(colls)
    }

    /// Load all collections across all tenants.
    pub fn load_all_collections(&self) -> crate::Result<Vec<StoredCollection>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        let mut colls = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range collections", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read collection", e))?;
            let coll: StoredCollection = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser collection", e))?;
            colls.push(coll);
        }
        Ok(colls)
    }

    /// Load all soft-deleted (`is_active = false`) collections across
    /// all tenants. Used by the retention GC sweeper on the Event
    /// Plane and by the `_system.dropped_collections` view.
    pub fn load_deactivated_collections(&self) -> crate::Result<Vec<StoredCollection>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        let mut colls = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range collections", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read collection", e))?;
            let coll: StoredCollection = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser collection", e))?;
            if !coll.is_active {
                colls.push(coll);
            }
        }
        Ok(colls)
    }

    /// Hard-delete a collection row. Returns `true` if a row was
    /// removed, `false` if the row was already absent (idempotent).
    /// Called by the `PurgeCollection` apply path; the corresponding
    /// owner row is deleted separately via `delete_parent_owner` so
    /// the caller can cascade peer catalog entries in the same
    /// transaction at the applier level.
    pub fn delete_collection(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = format!("{tenant_id}:{name}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed;
        {
            let mut table = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("open collections", e))?;
            removed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove collection", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }

    /// Get a single collection by tenant_id + name.
    pub fn get_collection(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> crate::Result<Option<StoredCollection>> {
        let key = format!("{tenant_id}:{name}");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let coll: StoredCollection = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser collection", e))?;
                Ok(Some(coll))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get collection", e)),
        }
    }
}
