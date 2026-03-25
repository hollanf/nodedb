//! Collection DDL: create, drop, list collections with metadata.

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// Collection metadata stored in redb.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionMeta {
    pub name: String,
    pub collection_type: String,
    pub created_at_ms: u64,
    pub fields: Vec<(String, String)>,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Create a collection with optional schema.
    ///
    /// If the collection already exists, returns Ok (idempotent).
    /// Schema is advisory — documents are schemaless by default.
    pub async fn create_collection(
        &self,
        name: &str,
        fields: &[(String, String)],
    ) -> NodeDbResult<()> {
        let meta = CollectionMeta {
            name: name.to_string(),
            collection_type: "document".to_string(),
            created_at_ms: now_ms(),
            fields: fields.to_vec(),
        };
        let key = format!("collection:{name}");
        let bytes = serde_json::to_vec(&meta).map_err(|e| NodeDbError::storage(e.to_string()))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await?;
        Ok(())
    }

    /// Drop a collection — deletes all documents and metadata.
    pub async fn drop_collection(&self, name: &str) -> NodeDbResult<()> {
        let ids = {
            let crdt = self.crdt.lock_or_recover();
            crdt.list_ids(name)
        };
        for id in &ids {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete(name, id).map_err(NodeDbError::storage)?;
        }

        let key = format!("collection:{name}");
        self.storage
            .delete(nodedb_types::Namespace::Meta, key.as_bytes())
            .await?;
        Ok(())
    }

    /// List all collections.
    pub async fn list_collections(&self) -> NodeDbResult<Vec<CollectionMeta>> {
        let pairs = self
            .storage
            .scan_prefix(nodedb_types::Namespace::Meta, b"collection:")
            .await?;
        let mut result = Vec::new();
        for (_, value) in &pairs {
            if let Ok(meta) = serde_json::from_slice::<CollectionMeta>(value) {
                result.push(meta);
            }
        }
        // Also include implicit collections (from CRDT state without explicit DDL).
        let crdt = self.crdt.lock_or_recover();
        let crdt_names = crdt.collection_names();
        let explicit: std::collections::HashSet<String> =
            result.iter().map(|m| m.name.clone()).collect();
        for name in crdt_names {
            if !name.starts_with("__") && !explicit.contains(&name) {
                result.push(CollectionMeta {
                    name,
                    collection_type: "document".to_string(),
                    created_at_ms: 0,
                    fields: Vec::new(),
                });
            }
        }
        Ok(result)
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
