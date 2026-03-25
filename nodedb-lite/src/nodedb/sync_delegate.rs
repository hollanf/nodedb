//! `SyncDelegate` implementation — bridges the sync transport to NodeDbLite's engines.

use crate::storage::engine::StorageEngine;

use super::core::NodeDbLite;

#[cfg(not(target_arch = "wasm32"))]
impl<S: StorageEngine> crate::sync::SyncDelegate for NodeDbLite<S> {
    fn pending_deltas(&self) -> Vec<crate::engine::crdt::engine::PendingDelta> {
        self.pending_crdt_deltas().unwrap_or_default()
    }

    fn acknowledge(&self, mutation_id: u64) {
        if let Err(e) = self.acknowledge_deltas(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: acknowledge failed");
        }
    }

    fn reject(&self, mutation_id: u64) {
        if let Err(e) = self.reject_delta(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: reject failed");
        }
    }

    fn import_remote(&self, data: &[u8]) {
        if let Err(e) = self.import_remote_deltas(data) {
            tracing::warn!(error = %e, "SyncDelegate: import_remote failed");
        }
    }
}
