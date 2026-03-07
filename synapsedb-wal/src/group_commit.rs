//! Group commit coordinator.
//!
//! In a concurrent setting, many tasks submit WAL records simultaneously.
//! Rather than each task issuing its own `fsync`, we batch them:
//!
//! 1. Tasks submit records to a queue.
//! 2. A single "commit leader" collects all pending records, writes them
//!    to the aligned buffer, and issues one `fsync`.
//! 3. All waiting tasks are notified with their assigned LSNs.
//!
//! This module defines the coordination types. The actual I/O is done by
//! `WalWriter` — this module manages the batching and notification.
//!
//! ## Future work
//!
//! This will integrate with the `synapsedb-bridge` SPSC channel when the
//! Data Plane submits WAL writes through the bridge.

/// A pending write request waiting to be committed.
#[derive(Debug)]
pub struct PendingWrite {
    /// Record type discriminant.
    pub record_type: u16,

    /// Tenant ID.
    pub tenant_id: u32,

    /// Virtual shard ID.
    pub vshard_id: u16,

    /// Payload bytes. Owned by the submitter until commit completes.
    pub payload: Vec<u8>,
}

/// The result of a group commit, delivered to each waiting task.
#[derive(Debug, Clone, Copy)]
pub struct CommitResult {
    /// The LSN assigned to this record.
    pub lsn: u64,

    /// Whether the fsync succeeded.
    pub durable: bool,
}
