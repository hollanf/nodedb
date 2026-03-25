//! Atomic transactions and conflict resolution policies.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use super::super::convert::value_to_loro;
use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// A single operation in a transaction batch.
pub enum TransactionOp {
    Put {
        collection: String,
        doc_id: String,
        fields: HashMap<String, Value>,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Execute a batch of operations atomically.
    ///
    /// All operations share a single Loro transaction (one delta export),
    /// and a single redb write transaction. If any operation fails,
    /// none are committed.
    pub fn transaction(&self, ops: &[TransactionOp]) -> NodeDbResult<u64> {
        let mut crdt = self.crdt.lock_or_recover();
        let mut count = 0u64;

        for op in ops {
            match op {
                TransactionOp::Put {
                    collection,
                    doc_id,
                    fields,
                } => {
                    let loro_fields: Vec<(&str, loro::LoroValue)> = fields
                        .iter()
                        .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                        .collect();
                    crdt.upsert(collection, doc_id, &loro_fields)
                        .map_err(NodeDbError::storage)?;
                    count += 1;
                }
                TransactionOp::Delete { collection, doc_id } => {
                    crdt.delete(collection, doc_id)
                        .map_err(NodeDbError::storage)?;
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Set conflict resolution policy for a collection.
    ///
    /// Policies are evaluated on sync when Origin rejects a delta.
    /// Available policies from `nodedb-crdt::PolicyRegistry`:
    /// - LastWriterWins (default)
    /// - RenameSuffix
    /// - CascadeDefer
    /// - EscalateToDlq
    pub fn set_conflict_policy(&self, collection: &str, policy: nodedb_crdt::CollectionPolicy) {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.set_policy(collection, policy);
    }
}
