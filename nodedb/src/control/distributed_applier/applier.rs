//! `DistributedApplier` — `CommitApplier` impl that queues committed Raft
//! entries onto a bounded mpsc channel for the background apply loop.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::warn;

use nodedb_cluster::raft_loop::CommitApplier;
use nodedb_raft::message::LogEntry;

use super::propose_tracker::ProposeTracker;

/// Queued entry for the background apply loop.
pub struct ApplyBatch {
    pub(crate) group_id: u64,
    pub(crate) entries: Vec<LogEntry>,
}

/// CommitApplier that queues committed entries for async Data Plane execution.
///
/// Uses a bounded tokio mpsc channel: `apply_committed()` is called from the
/// sync Raft tick loop and pushes non-blockingly. A background async task
/// reads from the channel, dispatches each write to the Data Plane, and
/// notifies any waiting proposers.
pub struct DistributedApplier {
    apply_tx: mpsc::Sender<ApplyBatch>,
    tracker: Arc<ProposeTracker>,
}

impl DistributedApplier {
    pub fn new(apply_tx: mpsc::Sender<ApplyBatch>, tracker: Arc<ProposeTracker>) -> Self {
        Self { apply_tx, tracker }
    }

    /// Access the tracker (for registering propose waiters).
    pub fn tracker(&self) -> &Arc<ProposeTracker> {
        &self.tracker
    }
}

impl CommitApplier for DistributedApplier {
    fn apply_committed(&self, group_id: u64, entries: &[LogEntry]) -> u64 {
        let last_index = entries.last().map(|e| e.index).unwrap_or(0);

        // Empty entries are Raft leader-transition no-ops, not user
        // proposals. A waiter registered at (group_id, idx) was
        // proposed by a previous leader at index `idx`; when that
        // leader stepped down before the entry committed, the new
        // leader's election no-op commits at the same index and
        // overwrites it. The proposer's data is GONE — silently
        // firing `tracker.complete(Ok([]))` here would tell the
        // proposer their INSERT succeeded when in fact it was
        // truncated, producing the classic "simple_query returned
        // Ok but the row never appears" silent data-loss bug.
        //
        // Surface the truncation as an explicit error so the gateway
        // / caller can retry. Idempotent re-propose is safe because
        // the encoded payload carries enough identity (collection,
        // PK, surrogate) for the apply path to be replayable.
        for entry in entries {
            if entry.data.is_empty() {
                tracing::error!(
                    group_id,
                    log_index = entry.index,
                    "leader-change no-op committed at index where a proposer was waiting; \
                     surfacing RetryableLeaderChange so the gateway re-proposes"
                );
                // applied_key = 0 (no entry payload to derive a key
                // from). The slot fires the explicit
                // `RetryableLeaderChange` carried in `result`.
                self.tracker.complete(
                    group_id,
                    entry.index,
                    0,
                    Err(crate::Error::RetryableLeaderChange {
                        group_id,
                        log_index: entry.index,
                    }),
                );
            }
        }

        let real_entries: Vec<LogEntry> = entries
            .iter()
            .filter(|e| !e.data.is_empty())
            .cloned()
            .collect();

        if real_entries.is_empty() {
            return last_index;
        }

        // Push to background task. If the channel is full, log a warning
        // but don't block the tick loop.
        if let Err(e) = self.apply_tx.try_send(ApplyBatch {
            group_id,
            entries: real_entries,
        }) {
            warn!(group_id, error = %e, "apply queue full, entries will be retried on next tick");
            // Don't advance applied index — entries will be re-delivered.
            return 0;
        }

        last_index
    }
}

/// Create a DistributedApplier and the channel for the background apply loop.
///
/// Returns (applier, receiver). Spawn `run_apply_loop` with the receiver.
pub fn create_distributed_applier(
    tracker: Arc<ProposeTracker>,
) -> (DistributedApplier, mpsc::Receiver<ApplyBatch>) {
    let (tx, rx) = mpsc::channel(1024);
    let applier = DistributedApplier::new(tx, tracker);
    (applier, rx)
}
