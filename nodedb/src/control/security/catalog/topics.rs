//! Durable topic metadata operations for the system catalog.

use super::types::{SystemCatalog, TOPICS_EP, catalog_err};
use crate::event::topic::TopicDef;

impl SystemCatalog {
    pub fn put_ep_topic(&self, def: &TopicDef) -> crate::Result<()> {
        let key = topic_key(def.tenant_id, &def.name);
        let bytes = zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize topic", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert topic", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_ep_topic(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = topic_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete topic", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    pub fn load_all_ep_topics(&self) -> crate::Result<Vec<TopicDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TOPICS_EP)
            .map_err(|e| catalog_err("open topics_ep", e))?;
        let mut topics = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range topics_ep", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<TopicDef>(value.value()) {
                topics.push(def);
            }
        }
        Ok(topics)
    }
}

fn topic_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}
