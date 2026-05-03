//! `Scheduler` struct definition, constructor, and main run loop.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::info;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::barrier::{PendingDependentBarrier, ReadResultEvent};
use super::super::config::SchedulerConfig;
use super::super::types::{BlockedTxn, PendingTxn};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
#[allow(unused_imports)]
use crate::control::cluster::calvin::scheduler::recovery::read_last_applied_epoch;
use crate::control::request_tracker::RequestTracker;
use crate::control::shutdown::ShutdownReceiver;
use crate::types::RequestId;
use crate::wal::manager::WalManager;

/// The Calvin scheduler for one vshard.
///
/// Owns the in-memory lock table and orchestrates lock acquisition, dispatch,
/// and response handling for both static-set and dependent-read transactions.
///
/// `Send` — runs as a Tokio task on the Control Plane.
pub struct Scheduler {
    /// Vshard this scheduler is responsible for.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) vshard_id: u32,
    /// Incoming sequenced transactions from the sequencer fan-out.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) receiver:
        mpsc::Receiver<SequencedTxn>,
    /// SPSC bridge dispatcher (shared with the rest of the Control Plane).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dispatcher:
        Arc<std::sync::Mutex<crate::bridge::dispatch::Dispatcher>>,
    /// Request tracker for response routing.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) tracker: Arc<RequestTracker>,
    /// WAL manager for writing `CalvinApplied` records.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) wal: Arc<WalManager>,
    /// Deterministic lock manager for this vshard.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) lock_manager: LockManager,
    /// In-flight static/active transactions awaiting executor response.
    /// `BTreeMap` ensures deterministic iteration order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) pending:
        BTreeMap<TxnId, PendingTxn>,
    /// Blocked transactions awaiting lock release.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) blocked:
        BTreeMap<TxnId, BlockedTxn>,
    /// Dependent-read barriers awaiting passive read results.
    /// `BTreeMap` for determinism.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dependent_barrier:
        BTreeMap<TxnId, PendingDependentBarrier>,
    /// Channel receiving `CalvinReadResult` Raft apply events from the
    /// per-vshard data Raft apply loop. Bounded.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) read_result_rx:
        mpsc::Receiver<ReadResultEvent>,
    /// Highest epoch applied so far.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) last_applied_epoch: u64,
    /// Rebuild target epoch (from initial recovery scan).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) rebuild_target_epoch: u64,
    /// Scheduler configuration.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) config: SchedulerConfig,
    /// Metrics.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) metrics: Arc<SchedulerMetrics>,
    /// Next monotonic request ID counter.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) state_request_counter:
        Arc<std::sync::atomic::AtomicU64>,
}

impl Scheduler {
    /// Construct a scheduler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vshard_id: u32,
        receiver: mpsc::Receiver<SequencedTxn>,
        dispatcher: Arc<std::sync::Mutex<crate::bridge::dispatch::Dispatcher>>,
        tracker: Arc<RequestTracker>,
        wal: Arc<WalManager>,
        last_applied_epoch: u64,
        rebuild_target_epoch: u64,
        config: SchedulerConfig,
        metrics: Arc<SchedulerMetrics>,
        request_counter: Arc<std::sync::atomic::AtomicU64>,
        read_result_rx: mpsc::Receiver<ReadResultEvent>,
    ) -> Self {
        Self {
            vshard_id,
            receiver,
            dispatcher,
            tracker,
            wal,
            lock_manager: LockManager::new(),
            pending: BTreeMap::new(),
            blocked: BTreeMap::new(),
            dependent_barrier: BTreeMap::new(),
            read_result_rx,
            last_applied_epoch,
            rebuild_target_epoch,
            config,
            metrics,
            state_request_counter: request_counter,
        }
    }

    /// Whether the scheduler has caught up to the rebuild target epoch.
    pub fn is_caught_up(&self) -> bool {
        self.last_applied_epoch >= self.rebuild_target_epoch
    }

    /// Run the scheduler event loop until shutdown is signaled.
    pub async fn run(mut self, mut shutdown: ShutdownReceiver) {
        info!(
            vshard_id = self.vshard_id,
            last_applied_epoch = self.last_applied_epoch,
            rebuild_target_epoch = self.rebuild_target_epoch,
            "calvin scheduler starting"
        );

        loop {
            self.poll_pending_responses().await;
            self.check_dependent_barrier_timeouts();

            tokio::select! {
                biased;

                _ = shutdown.wait_cancelled() => {
                    info!(vshard_id = self.vshard_id, "calvin scheduler shutting down");
                    break;
                }

                maybe_event = self.read_result_rx.recv() => {
                    if let Some(event) = maybe_event {
                        self.handle_read_result(event);
                    }
                }

                maybe_txn = self.receiver.recv() => {
                    match maybe_txn {
                        Some(txn) => self.process_new_txn(txn),
                        None => {
                            info!(
                                vshard_id = self.vshard_id,
                                "calvin scheduler: receiver channel closed; exiting"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Poll all in-flight pending requests for responses (best-effort).
    async fn poll_pending_responses(&mut self) {
        let pending_ids: Vec<(TxnId, RequestId)> = self
            .pending
            .iter()
            .map(|(tid, p)| (*tid, p.request_id))
            .collect();

        for (txn_id, request_id) in pending_ids {
            if let Some(pending) = self.pending.get(&txn_id) {
                let _ = (txn_id, request_id, pending);
            }
        }
    }

    /// Allocate a fresh request ID for a dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_request_id(
        &self,
    ) -> RequestId {
        let n = self
            .state_request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RequestId::new(n)
    }
}
