//! In-flight transaction types for the Calvin scheduler driver.

use std::collections::BTreeSet;
use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::lock_manager::LockKey;
use crate::types::RequestId;

/// An in-flight transaction that has been dispatched and is awaiting a
/// Data Plane response.
#[allow(dead_code)]
pub(super) struct PendingTxn {
    /// Original sequenced transaction (for WAL record on completion).
    pub txn: SequencedTxn,
    /// Pre-computed key set (stored so we don't re-expand on response).
    pub keys: BTreeSet<LockKey>,
    /// Request ID used for SPSC bridge correlation.
    pub request_id: RequestId,
    /// Wall-clock time at dispatch (for lock-wait latency metrics).
    ///
    /// `Instant::now()` is used here for observability only; never
    /// influences WAL bytes.
    pub dispatch_time: Instant,
    /// Wall-clock time at lock acquisition (for wait-latency measurement).
    pub lock_acquired_time: Instant,
}

/// A transaction that is blocked on lock acquisition.
pub(super) struct BlockedTxn {
    pub txn: SequencedTxn,
    pub keys: BTreeSet<LockKey>,
    /// Wall-clock time at first block (for latency metrics).
    ///
    /// `Instant::now()` used for observability only.
    pub blocked_at: Instant,
}
