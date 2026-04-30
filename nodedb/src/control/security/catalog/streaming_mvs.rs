//! Streaming MV metadata operations for the system catalog.

use super::types::{STREAMING_MVS, SystemCatalog, catalog_err};
use crate::event::streaming_mv::StreamingMvDef;

impl SystemCatalog {
    pub fn put_streaming_mv(&self, def: &StreamingMvDef) -> crate::Result<()> {
        let key = mv_key(def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize streaming_mv", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(STREAMING_MVS)
                .map_err(|e| catalog_err("open streaming_mvs", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert streaming_mv", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_streaming_mv(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        let key = mv_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(STREAMING_MVS)
                .map_err(|e| catalog_err("open streaming_mvs", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete streaming_mv", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    pub fn load_all_streaming_mvs(&self) -> crate::Result<Vec<StreamingMvDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(STREAMING_MVS)
            .map_err(|e| catalog_err("open streaming_mvs", e))?;
        let mut mvs = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range streaming_mvs", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<StreamingMvDef>(value.value()) {
                mvs.push(def);
            }
        }
        Ok(mvs)
    }
}

fn mv_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
}
