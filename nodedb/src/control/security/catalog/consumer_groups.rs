//! Consumer group metadata operations for the system catalog.

use super::types::{CONSUMER_GROUPS, SystemCatalog, catalog_err};
use crate::event::cdc::consumer_group::ConsumerGroupDef;

impl SystemCatalog {
    /// Store a consumer group definition.
    ///
    /// Key format: `"{tenant_id}:{stream_name}:{group_name}"`.
    pub fn put_consumer_group(&self, def: &ConsumerGroupDef) -> crate::Result<()> {
        let key = group_key(def.tenant_id, &def.stream_name, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize consumer_group", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert consumer_group", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete a consumer group. Returns true if it existed.
    pub fn delete_consumer_group(
        &self,
        tenant_id: u32,
        stream: &str,
        group: &str,
    ) -> crate::Result<bool> {
        let key = group_key(tenant_id, stream, group);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete consumer_group", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all consumer groups (all tenants).
    pub fn load_all_consumer_groups(&self) -> crate::Result<Vec<ConsumerGroupDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CONSUMER_GROUPS)
            .map_err(|e| catalog_err("open consumer_groups", e))?;

        let mut groups = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range consumer_groups", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<ConsumerGroupDef>(value.value()) {
                groups.push(def);
            }
        }
        Ok(groups)
    }
}

fn group_key(tenant_id: u32, stream: &str, group: &str) -> String {
    format!("{tenant_id}:{stream}:{group}")
}

#[cfg(test)]
mod tests {
    use crate::control::security::catalog::types::SystemCatalog;
    use crate::event::cdc::consumer_group::ConsumerGroupDef;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    #[test]
    fn put_and_load_roundtrip() {
        let cat = make_catalog();
        let def = ConsumerGroupDef {
            tenant_id: 1,
            name: "analytics".into(),
            stream_name: "orders_stream".into(),
            owner: "admin".into(),
            created_at: 1000,
        };
        cat.put_consumer_group(&def).unwrap();

        let all = cat.load_all_consumer_groups().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "analytics");
        assert_eq!(all[0].stream_name, "orders_stream");
    }

    #[test]
    fn delete_group() {
        let cat = make_catalog();
        let def = ConsumerGroupDef {
            tenant_id: 1,
            name: "g1".into(),
            stream_name: "s1".into(),
            owner: "admin".into(),
            created_at: 0,
        };
        cat.put_consumer_group(&def).unwrap();
        assert!(cat.delete_consumer_group(1, "s1", "g1").unwrap());
        assert!(!cat.delete_consumer_group(1, "s1", "g1").unwrap());
        assert!(cat.load_all_consumer_groups().unwrap().is_empty());
    }
}
