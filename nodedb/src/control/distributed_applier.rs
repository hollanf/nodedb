//! Distributed apply machinery — tracks pending Raft proposals and applies
//! committed entries to the local Data Plane.
//!
//! See [`crate::control::wal_replication`] for the full write flow description.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use nodedb_cluster::GroupAppliedWatchers;
use nodedb_cluster::raft_loop::CommitApplier;
use nodedb_raft::message::LogEntry;

use crate::bridge::envelope::{Priority, Request, Status};
use crate::control::array_sync::raft_apply::{apply_array_op, apply_array_schema};
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, from_replicated_entry};
use crate::types::ReadConsistency;

// ── Propose tracker ─────────────────────────────────────────────────

/// Response payload sent back to the proposer after commit + execution.
pub type ProposeResult = std::result::Result<Vec<u8>, crate::Error>;

/// Slot in the propose tracker — either a pending waiter or a completed result
/// that arrived before the waiter was registered.
enum TrackerSlot {
    /// Waiter registered by the proposer; awaiting `complete()`.
    ///
    /// `expected_key` is the proposer's idempotency key. The apply path
    /// passes the applied entry's key to `complete`; if they differ the
    /// proposer's reservation was overwritten by a different proposer's
    /// entry under a leader change and we surface
    /// `RetryableLeaderChange` instead of the (success-shaped) result
    /// that would otherwise leak the wrong entry's payload back to the
    /// proposer. `expected_key == 0` is a wildcard accepting any key
    /// (used for legacy synthetic registrations).
    Waiting {
        tx: oneshot::Sender<ProposeResult>,
        expected_key: u64,
    },
    /// `complete()` was called before `register()`. Stored so `register()`
    /// can resolve the channel immediately.
    Completed(ProposeResult),
}

/// Tracks pending proposals awaiting Raft commit.
///
/// Keyed by `(group_id, log_index)`. The proposer calls `register()` after
/// the proposal returns the log index; `run_apply_loop` calls `complete()`
/// after the entry is applied. Either side may win the race — `complete()`
/// stores the result if no waiter exists yet, and `register()` picks it up
/// immediately if `complete()` already fired.
pub struct ProposeTracker {
    slots: Mutex<HashMap<(u64, u64), TrackerSlot>>,
    /// Per-Raft-group apply watermark registry. Bumped on every
    /// [`Self::complete`] so the watcher reflects "data applied on
    /// this node up to index N" — the only semantic that's useful
    /// for cross-node visibility waits. Tick-loop bumps cover the
    /// metadata group (sync redb apply); this tracker covers data
    /// groups (async SPSC dispatch through `run_apply_loop`).
    /// `None` only in tests that don't exercise the watcher.
    group_watchers: Option<Arc<GroupAppliedWatchers>>,
}

impl Default for ProposeTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ProposeTracker {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            group_watchers: None,
        }
    }

    /// Wire the per-group apply watermark registry. Called by
    /// `start_raft` after `SharedState` is constructed.
    pub fn with_group_watchers(mut self, watchers: Arc<GroupAppliedWatchers>) -> Self {
        self.group_watchers = Some(watchers);
        self
    }

    /// Register a waiter for a proposed entry. Returns a receiver that
    /// resolves when the entry is committed and executed.
    ///
    /// If `complete()` was called first (the entry was applied before this
    /// node could register), the receiver is pre-resolved and ready
    /// immediately.
    pub fn register(
        &self,
        group_id: u64,
        log_index: u64,
        expected_key: u64,
    ) -> oneshot::Receiver<ProposeResult> {
        let (tx, rx) = oneshot::channel();
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        match slots.entry((group_id, log_index)) {
            Entry::Vacant(e) => {
                e.insert(TrackerSlot::Waiting { tx, expected_key });
            }
            Entry::Occupied(e) => {
                match e.get() {
                    TrackerSlot::Completed(_) => {
                        // complete() already fired — extract the result, resolve
                        // the receiver immediately, and clean up the slot.
                        if let TrackerSlot::Completed(result) = e.remove() {
                            let _ = tx.send(result);
                        }
                    }
                    TrackerSlot::Waiting { .. } => {
                        // Duplicate register — shouldn't happen. Insert the new
                        // sender; the old receiver will see channel-closed.
                        *e.into_mut() = TrackerSlot::Waiting { tx, expected_key };
                    }
                }
            }
        }
        rx
    }

    /// Complete a waiter after the entry has been committed and executed.
    ///
    /// If the proposer has already called `register()`, the result is sent
    /// immediately. If not, the result is stored so the next `register()`
    /// call picks it up without waiting.
    ///
    /// Returns true if a live waiter was found and notified, false otherwise.
    pub fn complete(
        &self,
        group_id: u64,
        log_index: u64,
        applied_key: u64,
        result: ProposeResult,
    ) -> bool {
        // Bump the per-group apply watermark. Bumping unconditionally
        // (success and error) keeps the watcher monotonic with raft's
        // commit progression — a data-plane error means "the entry
        // could not be applied" but the entry IS committed and Raft
        // has advanced its applied index. Tests waiting on
        // visibility care about the success path; liveness on the
        // error path requires the bump too.
        if let Some(w) = &self.group_watchers {
            w.bump(group_id, log_index);
        }

        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        match slots.entry((group_id, log_index)) {
            Entry::Vacant(e) => {
                // No waiter yet — store result for the upcoming register().
                e.insert(TrackerSlot::Completed(result));
                false
            }
            Entry::Occupied(e) => {
                match e.get() {
                    TrackerSlot::Waiting { expected_key, .. } => {
                        // Idempotency-key gate: the entry that committed
                        // at this (group_id, log_index) must be the one
                        // the proposer reserved. If the keys disagree,
                        // a leader change overwrote the proposer's entry
                        // with a different one — surface the retryable
                        // signal instead of the (success-shaped) result
                        // that belongs to a different proposer. A zero
                        // applied_key means "no key carried" (empty
                        // entry / legacy); a zero expected_key means the
                        // registration is wildcard (legacy callers).
                        let mismatch =
                            applied_key != 0 && *expected_key != 0 && applied_key != *expected_key;
                        let final_result = if mismatch {
                            tracing::warn!(
                                group_id,
                                log_index,
                                applied_key,
                                expected_key = *expected_key,
                                "raft entry at proposer's index was overwritten by \
                                 a different proposal (idempotency_key mismatch); \
                                 surfacing RetryableLeaderChange"
                            );
                            Err(crate::Error::RetryableLeaderChange {
                                group_id,
                                log_index,
                            })
                        } else {
                            result
                        };
                        if let TrackerSlot::Waiting { tx, .. } = e.remove() {
                            let _ = tx.send(final_result);
                            return true;
                        }
                    }
                    TrackerSlot::Completed(_) => {
                        // Already completed — overwrite with newer result.
                        // Duplicate completes should not occur in practice;
                        // last write wins.
                        *e.into_mut() = TrackerSlot::Completed(result);
                    }
                }
                false
            }
        }
    }
}

// ── Distributed applier ─────────────────────────────────────────────

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

// ── Background apply loop ───────────────────────────────────────────

/// Run the background loop that applies committed Raft entries to the local Data Plane.
///
/// This task reads from the apply channel, deserializes each entry, dispatches
/// the write to the Data Plane via SPSC, and notifies proposers.
pub async fn run_apply_loop(
    mut apply_rx: mpsc::Receiver<ApplyBatch>,
    state: Arc<SharedState>,
    tracker: Arc<ProposeTracker>,
) {
    while let Some(batch) = apply_rx.recv().await {
        for entry in &batch.entries {
            // Extract idempotency key once at the top so every
            // tracker.complete on this entry can pass it. Returns 0
            // for unparseable / pre-key entries; the tracker treats
            // 0 as "no key" (no mismatch detection).
            let applied_key = ReplicatedEntry::from_bytes(&entry.data)
                .map(|e| e.idempotency_key)
                .unwrap_or(0);

            // ── Array CRDT variants — handled on the Control Plane, bypass Data Plane ──
            if let Some(replicated) = ReplicatedEntry::from_bytes(&entry.data) {
                match replicated.write {
                    ReplicatedWrite::ArrayOp {
                        ref array,
                        ref op_bytes,
                        ..
                    } => {
                        apply_array_op(
                            &state,
                            &tracker,
                            batch.group_id,
                            entry.index,
                            applied_key,
                            array,
                            op_bytes,
                        )
                        .await;
                        continue;
                    }
                    ReplicatedWrite::ArraySchema {
                        ref array,
                        ref snapshot_payload,
                        schema_hlc_bytes,
                    } => {
                        apply_array_schema(
                            &state,
                            &tracker,
                            batch.group_id,
                            entry.index,
                            applied_key,
                            crate::control::array_sync::raft_apply::ArraySchemaPayload {
                                array,
                                snapshot_payload,
                                schema_hlc_bytes,
                            },
                        );
                        continue;
                    }
                    _ => {}
                }
            }

            let decoded =
                from_replicated_entry(&entry.data, Some(state.surrogate_assigner.as_ref()));
            let (tenant_id, vshard_id, plan) = match decoded {
                Ok(Some(t)) => t,
                Ok(None) => {
                    // Couldn't deserialize — might be a different format or corrupted.
                    debug!(
                        group_id = batch.group_id,
                        index = entry.index,
                        "skipping non-ReplicatedEntry commit"
                    );
                    tracker.complete(batch.group_id, entry.index, applied_key, Ok(vec![]));
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        group_id = batch.group_id,
                        index = entry.index,
                        error = %e,
                        "failed to decode replicated entry (surrogate bind error)"
                    );
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        applied_key,
                        Err(crate::Error::Internal {
                            detail: format!("decode replicated entry: {e}"),
                        }),
                    );
                    continue;
                }
            };

            let request_id = state.next_request_id();

            let request = Request {
                request_id,
                tenant_id,
                vshard_id,
                plan,
                deadline: Instant::now() + Duration::from_secs(30),
                priority: Priority::Normal,
                trace_id: 0,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
            };

            let rx = state.tracker.register_oneshot(request_id);

            let dispatch_result = match state.dispatcher.lock() {
                Ok(mut d) => d.dispatch(request),
                Err(poisoned) => poisoned.into_inner().dispatch(request),
            };

            if let Err(e) = dispatch_result {
                warn!(
                    group_id = batch.group_id,
                    index = entry.index,
                    error = %e,
                    "failed to dispatch committed write"
                );
                tracker.complete(
                    batch.group_id,
                    entry.index,
                    applied_key,
                    Err(crate::Error::Internal {
                        detail: format!("dispatch failed: {e}"),
                    }),
                );
                continue;
            }

            // Await Data Plane response.
            match tokio::time::timeout(Duration::from_secs(30), rx).await {
                Ok(Ok(resp)) => {
                    let payload = resp.payload.to_vec();
                    if resp.status == Status::Error {
                        let err_msg = resp
                            .error_code
                            .as_ref()
                            .map(|c| format!("{c:?}"))
                            .unwrap_or_else(|| "execution error".into());
                        tracker.complete(
                            batch.group_id,
                            entry.index,
                            applied_key,
                            Err(crate::Error::Internal { detail: err_msg }),
                        );
                    } else {
                        tracker.complete(batch.group_id, entry.index, applied_key, Ok(payload));
                    }
                }
                Ok(Err(_)) => {
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        applied_key,
                        Err(crate::Error::Internal {
                            detail: "response channel closed".to_string(),
                        }),
                    );
                }
                Err(_) => {
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        applied_key,
                        Err(crate::Error::Internal {
                            detail: "deadline exceeded applying committed write".to_string(),
                        }),
                    );
                }
            }
        }
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
