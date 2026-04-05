//! Cross-shard write atomicity via Calvin-style deterministic transactions.
//!
//! When a transaction spans multiple vShards, the coordinator:
//! 1. Leader appends the full transaction to its local Raft log
//! 2. Each involved shard receives a `ForwardEntry` as a Raft-replicated side-effect
//! 3. All shards apply the writes in the same deterministic order
//!
//! No 2PC needed: Raft guarantees total order within each group, and the
//! deterministic scheduling ensures all groups see the same global order.
//!
//! This is the Calvin protocol (Thomson et al., SIGMOD 2012).

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// A cross-shard transaction: a set of writes targeting multiple vShards.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct CrossShardTransaction {
    /// Unique transaction ID (monotonic from coordinator).
    pub txn_id: u64,
    /// Tenant scope.
    pub tenant_id: u32,
    /// Per-shard write sets: `(vshard_id, serialized_writes)`.
    /// Each entry is the MessagePack-encoded writes for that shard.
    pub shard_writes: Vec<(u16, Vec<u8>)>,
    /// Coordinator node ID (the node that initiated the transaction).
    pub coordinator_node: u64,
    /// Coordinator's Raft log index where this transaction was proposed.
    pub coordinator_log_index: u64,
}

/// A forwarded write entry that a remote shard applies as a Raft side-effect.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ForwardEntry {
    /// Transaction ID — used to correlate with the coordinator's commit.
    pub txn_id: u64,
    /// The writes to apply on this shard (serialized PhysicalPlan operations).
    pub writes: Vec<u8>,
    /// Source shard that originated the transaction.
    pub source_vshard: u16,
    /// Coordinator's log index for ordering.
    pub coordinator_log_index: u64,
}

/// Cross-shard GSI update entry.
///
/// When a document write triggers a GSI update on a different shard,
/// this entry is forwarded to the target shard as a Raft-replicated
/// side-effect, ensuring atomic consistency with the primary write.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct GsiForwardEntry {
    /// GSI index name.
    pub index_name: String,
    /// The indexed field value.
    pub value: String,
    /// Document location in the primary shard.
    pub tenant_id: u32,
    pub collection: String,
    pub document_id: String,
    pub source_vshard: u16,
    /// Whether to add or remove the GSI entry.
    pub action: GsiAction,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum GsiAction {
    /// Add/update the GSI entry for this document.
    Upsert = 0,
    /// Remove the GSI entry for this document.
    Remove = 1,
}

/// Cross-shard edge creation validation request.
///
/// Before creating an edge where src and dst are on different shards,
/// the Control Plane sends a validation request to the destination
/// shard to confirm the dst node exists.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct EdgeValidationRequest {
    pub src_id: String,
    pub src_vshard: u16,
    pub dst_id: String,
    pub dst_vshard: u16,
    pub label: String,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum EdgeValidationResult {
    /// Destination exists — safe to create the edge.
    Exists = 0,
    /// Destination not found — reject the edge.
    NotFound = 1,
    /// Destination shard unreachable — retry later.
    Unavailable = 2,
}

/// Transaction coordinator state (per node).
///
/// Tracks pending cross-shard transactions and their completion status.
pub struct TransactionCoordinator {
    /// Next transaction ID (monotonic).
    next_txn_id: u64,
    /// Pending transactions: txn_id → transaction state.
    pending: std::collections::HashMap<u64, TxnState>,
    /// This node's ID.
    node_id: u64,
}

#[derive(Debug)]
struct TxnState {
    txn: CrossShardTransaction,
    /// Shards that have acknowledged applying the forwarded writes.
    acks_received: std::collections::HashSet<u16>,
    /// Shards that need to acknowledge.
    acks_needed: std::collections::HashSet<u16>,
    /// Whether the transaction is fully committed (all shards acked).
    committed: bool,
}

impl TransactionCoordinator {
    pub fn new(node_id: u64) -> Self {
        Self {
            next_txn_id: 1,
            pending: std::collections::HashMap::new(),
            node_id,
        }
    }

    /// Begin a cross-shard transaction.
    ///
    /// Returns the transaction ID and the `CrossShardTransaction` to be
    /// proposed to the coordinator's Raft group.
    pub fn begin(
        &mut self,
        tenant_id: u32,
        shard_writes: Vec<(u16, Vec<u8>)>,
        coordinator_log_index: u64,
    ) -> CrossShardTransaction {
        let txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        let acks_needed: std::collections::HashSet<u16> =
            shard_writes.iter().map(|(s, _)| *s).collect();

        let txn = CrossShardTransaction {
            txn_id,
            tenant_id,
            shard_writes,
            coordinator_node: self.node_id,
            coordinator_log_index,
        };

        self.pending.insert(
            txn_id,
            TxnState {
                txn: txn.clone(),
                acks_received: std::collections::HashSet::new(),
                acks_needed,
                committed: false,
            },
        );

        debug!(txn_id, "cross-shard transaction created");
        txn
    }

    /// Record an acknowledgment from a shard that applied the forwarded writes.
    ///
    /// Returns `true` if all shards have acked (transaction fully committed).
    pub fn ack(&mut self, txn_id: u64, vshard_id: u16) -> bool {
        if let Some(state) = self.pending.get_mut(&txn_id) {
            state.acks_received.insert(vshard_id);
            if state.acks_received == state.acks_needed {
                state.committed = true;
                info!(txn_id, "cross-shard transaction fully committed");
                true
            } else {
                debug!(
                    txn_id,
                    received = state.acks_received.len(),
                    needed = state.acks_needed.len(),
                    "cross-shard transaction partial ack"
                );
                false
            }
        } else {
            warn!(txn_id, "ack for unknown transaction");
            false
        }
    }

    /// Check if a transaction is fully committed.
    pub fn is_committed(&self, txn_id: u64) -> bool {
        self.pending.get(&txn_id).is_some_and(|s| s.committed)
    }

    /// Remove a committed transaction from the pending set.
    pub fn cleanup(&mut self, txn_id: u64) {
        self.pending.remove(&txn_id);
    }

    /// Get the transaction details for a pending transaction.
    pub fn get_transaction(&self, txn_id: u64) -> Option<&CrossShardTransaction> {
        self.pending.get(&txn_id).map(|s| &s.txn)
    }

    /// Number of pending (in-flight) transactions.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Generate `ForwardEntry` messages for each shard in the transaction.
    ///
    /// Called after the coordinator's Raft group commits the transaction.
    /// Each entry is sent to the target shard's Raft group for replication.
    pub fn generate_forwards(txn: &CrossShardTransaction) -> Vec<(u16, ForwardEntry)> {
        txn.shard_writes
            .iter()
            .map(|(vshard, writes)| {
                (
                    *vshard,
                    ForwardEntry {
                        txn_id: txn.txn_id,
                        writes: writes.clone(),
                        source_vshard: txn.shard_writes.first().map(|(s, _)| *s).unwrap_or(0),
                        coordinator_log_index: txn.coordinator_log_index,
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_lifecycle() {
        let mut coord = TransactionCoordinator::new(1);

        let txn = coord.begin(
            1,
            vec![
                (10, b"writes_for_shard_10".to_vec()),
                (20, b"writes_for_shard_20".to_vec()),
            ],
            100,
        );
        assert_eq!(txn.txn_id, 1);
        assert_eq!(coord.pending_count(), 1);
        assert!(!coord.is_committed(1));

        // First ack.
        assert!(!coord.ack(1, 10));
        assert!(!coord.is_committed(1));

        // Second ack — fully committed.
        assert!(coord.ack(1, 20));
        assert!(coord.is_committed(1));

        coord.cleanup(1);
        assert_eq!(coord.pending_count(), 0);
    }

    #[test]
    fn generate_forwards() {
        let txn = CrossShardTransaction {
            txn_id: 42,
            tenant_id: 1,
            shard_writes: vec![(10, b"w1".to_vec()), (20, b"w2".to_vec())],
            coordinator_node: 1,
            coordinator_log_index: 100,
        };

        let forwards = TransactionCoordinator::generate_forwards(&txn);
        assert_eq!(forwards.len(), 2);
        assert_eq!(forwards[0].0, 10);
        assert_eq!(forwards[0].1.txn_id, 42);
        assert_eq!(forwards[1].0, 20);
    }

    #[test]
    fn edge_validation_types() {
        let req = EdgeValidationRequest {
            src_id: "alice".into(),
            src_vshard: 10,
            dst_id: "bob".into(),
            dst_vshard: 20,
            label: "KNOWS".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: EdgeValidationRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.src_id, "alice");
        assert_eq!(decoded.dst_vshard, 20);
    }

    #[test]
    fn gsi_forward_roundtrip() {
        let entry = GsiForwardEntry {
            index_name: "email_idx".into(),
            value: "alice@example.com".into(),
            tenant_id: 1,
            collection: "users".into(),
            document_id: "u1".into(),
            source_vshard: 10,
            action: GsiAction::Upsert,
        };
        let bytes = zerompk::to_msgpack_vec(&entry).unwrap();
        let decoded: GsiForwardEntry = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.index_name, "email_idx");
    }
}
