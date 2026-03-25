//! Bulk update and delete by SQL WHERE predicate.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use super::super::convert::value_to_loro;
use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Bulk update documents matching a SQL WHERE predicate.
    ///
    /// Executes `SELECT id FROM {collection} WHERE {predicate}` via DataFusion,
    /// then applies `updates` to each matching document.
    ///
    /// Returns the number of documents updated.
    pub async fn bulk_update(
        &self,
        collection: &str,
        predicate: &str,
        updates: &HashMap<String, Value>,
    ) -> NodeDbResult<u64> {
        let sql = format!("SELECT id FROM {collection} WHERE {predicate}");
        let result = self
            .query_engine
            .execute_sql(&sql)
            .await
            .map_err(NodeDbError::storage)?;

        let mut count = 0u64;
        let mut crdt = self.crdt.lock_or_recover();
        for row in &result.rows {
            if let Some(Value::String(id)) = row.first() {
                let fields: Vec<(&str, loro::LoroValue)> = updates
                    .iter()
                    .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                    .collect();
                crdt.upsert(collection, id, &fields)
                    .map_err(NodeDbError::storage)?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Bulk delete documents matching a SQL WHERE predicate.
    ///
    /// Returns the number of documents deleted.
    pub async fn bulk_delete(&self, collection: &str, predicate: &str) -> NodeDbResult<u64> {
        let sql = format!("SELECT id FROM {collection} WHERE {predicate}");
        let result = self
            .query_engine
            .execute_sql(&sql)
            .await
            .map_err(NodeDbError::storage)?;

        let mut count = 0u64;
        let mut crdt = self.crdt.lock_or_recover();
        for row in &result.rows {
            if let Some(Value::String(id)) = row.first() {
                crdt.delete(collection, id).map_err(NodeDbError::storage)?;
                count += 1;
            }
        }
        Ok(count)
    }
}
