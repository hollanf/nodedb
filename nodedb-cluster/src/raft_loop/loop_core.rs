//! Core `RaftLoop` struct, construction, and the periodic tick pipeline.
//!
//! The RPC handler (`impl RaftRpcHandler for RaftLoop`) lives in
//! [`super::handle_rpc`] — it's a separate file so the tick/dispatch pipeline
//! and the inbound-RPC fan-out don't share a single oversized file.

use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tracing::{debug, warn};

use nodedb_raft::message::LogEntry;
use nodedb_raft::transport::RaftTransport;

use crate::conf_change::ConfChange;
use crate::error::Result;
use crate::forward::RequestForwarder;
use crate::multi_raft::MultiRaft;
use crate::topology::ClusterTopology;
use crate::transport::NexarTransport;

/// Default tick interval (10ms — fast enough for sub-second elections).
///
/// Matches `ClusterTransportTuning::raft_tick_interval_ms` default. Override
/// at startup by calling `.with_tick_interval()` on the `RaftLoop`, passing
/// `Duration::from_millis(config.tuning.cluster_transport.raft_tick_interval_ms)`.
const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(10);

/// Callback for applying committed Raft log entries to the state machine.
///
/// Called synchronously during the tick loop. Implementations should be fast
/// (enqueue to SPSC, not perform I/O directly).
pub trait CommitApplier: Send + Sync + 'static {
    /// Apply committed entries for a Raft group.
    ///
    /// Returns the index of the last successfully applied entry.
    fn apply_committed(&self, group_id: u64, entries: &[LogEntry]) -> u64;
}

/// Type-erased async handler for incoming `VShardEnvelope` messages.
///
/// Receives raw envelope bytes, returns response bytes. Set by the main binary
/// to dispatch to the appropriate engine handler (Event Plane, timeseries, etc.).
pub type VShardEnvelopeHandler = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>
        + Send
        + Sync,
>;

/// Raft event loop coordinator.
///
/// Owns the MultiRaft state (behind `Arc<Mutex>`) and drives it via periodic
/// ticks. Implements [`crate::transport::RaftRpcHandler`] (in
/// [`super::handle_rpc`]) so it can be passed directly to
/// [`NexarTransport::serve`] for incoming RPC dispatch.
pub struct RaftLoop<A: CommitApplier, F: RequestForwarder = crate::forward::NoopForwarder> {
    pub(super) node_id: u64,
    pub(super) multi_raft: Arc<Mutex<MultiRaft>>,
    pub(super) transport: Arc<NexarTransport>,
    pub(super) topology: Arc<RwLock<ClusterTopology>>,
    pub(super) applier: A,
    pub(super) forwarder: Arc<F>,
    pub(super) tick_interval: Duration,
    /// Optional handler for incoming VShardEnvelope messages.
    /// Set when the Event Plane or other subsystems need cross-node messaging.
    pub(super) vshard_handler: Option<VShardEnvelopeHandler>,
}

impl<A: CommitApplier> RaftLoop<A> {
    pub fn new(
        multi_raft: MultiRaft,
        transport: Arc<NexarTransport>,
        topology: Arc<RwLock<ClusterTopology>>,
        applier: A,
    ) -> Self {
        let node_id = multi_raft.node_id();
        Self {
            node_id,
            multi_raft: Arc::new(Mutex::new(multi_raft)),
            transport,
            topology,
            applier,
            forwarder: Arc::new(crate::forward::NoopForwarder),
            tick_interval: DEFAULT_TICK_INTERVAL,
            vshard_handler: None,
        }
    }
}

impl<A: CommitApplier, F: RequestForwarder> RaftLoop<A, F> {
    /// Create a RaftLoop with a custom request forwarder (for cluster mode).
    pub fn with_forwarder(
        multi_raft: MultiRaft,
        transport: Arc<NexarTransport>,
        topology: Arc<RwLock<ClusterTopology>>,
        applier: A,
        forwarder: Arc<F>,
    ) -> Self {
        let node_id = multi_raft.node_id();
        Self {
            node_id,
            multi_raft: Arc::new(Mutex::new(multi_raft)),
            transport,
            topology,
            applier,
            forwarder,
            tick_interval: DEFAULT_TICK_INTERVAL,
            vshard_handler: None,
        }
    }

    /// Set a handler for incoming VShardEnvelope messages.
    pub fn with_vshard_handler(mut self, handler: VShardEnvelopeHandler) -> Self {
        self.vshard_handler = Some(handler);
        self
    }

    pub fn with_tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    /// This node's id (exposed for handlers and tests).
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Run the event loop until shutdown.
    ///
    /// This drives Raft elections, heartbeats, and message dispatch.
    /// Call [`NexarTransport::serve`] separately with `Arc<Self>` as the handler.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.do_tick();
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!("raft loop shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Execute a single tick: drive Raft, dispatch outbound messages, apply commits.
    pub(super) fn do_tick(&self) {
        // Phase 1: tick under lock, extract Ready.
        let ready = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.tick()
        };

        if ready.is_empty() {
            return;
        }

        // Phase 2: Batch messages by peer for efficient dispatch.
        // Instead of spawning one task per (group, peer) message, we batch all
        // messages to the same peer into one task, reducing QUIC stream overhead.
        use std::collections::HashMap as BatchMap;

        let mut ae_batches: BatchMap<u64, Vec<(u64, nodedb_raft::AppendEntriesRequest)>> =
            BatchMap::new();
        let mut vote_batches: BatchMap<u64, Vec<(u64, nodedb_raft::RequestVoteRequest)>> =
            BatchMap::new();

        for (group_id, group_ready) in &ready.groups {
            for (peer, req) in &group_ready.messages {
                ae_batches
                    .entry(*peer)
                    .or_default()
                    .push((*group_id, req.clone()));
            }
            for (peer, req) in &group_ready.vote_requests {
                vote_batches
                    .entry(*peer)
                    .or_default()
                    .push((*group_id, req.clone()));
            }
        }

        // Dispatch batched AppendEntries — one task per peer.
        for (peer, messages) in ae_batches {
            let transport = self.transport.clone();
            let mr = self.multi_raft.clone();
            tokio::spawn(async move {
                for (group_id, req) in messages {
                    match transport.append_entries(peer, req).await {
                        Ok(resp) => {
                            let mut mr = mr.lock().unwrap_or_else(|p| p.into_inner());
                            if let Err(e) = mr.handle_append_entries_response(group_id, peer, &resp)
                            {
                                debug!(group_id, peer, error = %e, "handle ae response");
                            }
                        }
                        Err(e) => {
                            warn!(group_id, peer, error = %e, "append_entries RPC failed");
                            break; // Peer is down — skip remaining groups.
                        }
                    }
                }
            });
        }

        // Dispatch batched RequestVote — one task per peer.
        for (peer, votes) in vote_batches {
            let transport = self.transport.clone();
            let mr = self.multi_raft.clone();
            tokio::spawn(async move {
                for (group_id, req) in votes {
                    match transport.request_vote(peer, req).await {
                        Ok(resp) => {
                            let mut mr = mr.lock().unwrap_or_else(|p| p.into_inner());
                            if let Err(e) = mr.handle_request_vote_response(group_id, peer, &resp) {
                                debug!(group_id, peer, error = %e, "handle vote response");
                            }
                        }
                        Err(e) => {
                            warn!(group_id, peer, error = %e, "request_vote RPC failed");
                            break;
                        }
                    }
                }
            });
        }

        for (group_id, group_ready) in ready.groups {
            // Apply committed entries synchronously.
            if !group_ready.committed_entries.is_empty() {
                // First, detect and apply any ConfChange entries to MultiRaft.
                for entry in &group_ready.committed_entries {
                    if let Some(cc) = ConfChange::from_entry_data(&entry.data) {
                        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                        if let Err(e) = mr.apply_conf_change(group_id, &cc) {
                            warn!(group_id, error = %e, "failed to apply conf change");
                        }
                    }
                }

                // Then pass all entries (including ConfChanges) to the state machine.
                let last_applied = self
                    .applier
                    .apply_committed(group_id, &group_ready.committed_entries);
                if last_applied > 0 {
                    let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                    if let Err(e) = mr.advance_applied(group_id, last_applied) {
                        warn!(group_id, error = %e, "failed to advance applied index");
                    }
                }
            }

            // Send InstallSnapshot RPCs to peers that are too far behind.
            if !group_ready.snapshots_needed.is_empty() {
                // Get snapshot metadata under lock.
                let snapshot_meta = {
                    let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                    mr.snapshot_metadata(group_id).ok()
                };

                if let Some((term, snap_index, snap_term)) = snapshot_meta {
                    for peer in group_ready.snapshots_needed {
                        let transport = self.transport.clone();
                        let mr = self.multi_raft.clone();
                        let req = nodedb_raft::InstallSnapshotRequest {
                            term,
                            leader_id: self.node_id,
                            last_included_index: snap_index,
                            last_included_term: snap_term,
                            offset: 0,
                            data: vec![], // Metadata-only snapshot.
                            done: true,
                            group_id,
                        };
                        tokio::spawn(async move {
                            match transport.install_snapshot(peer, req).await {
                                Ok(resp) => {
                                    if resp.term > term {
                                        let mut mr = mr.lock().unwrap_or_else(|p| p.into_inner());
                                        // Higher term — let the tick loop handle step-down.
                                        let _ = mr.handle_append_entries_response(
                                            group_id,
                                            peer,
                                            &nodedb_raft::AppendEntriesResponse {
                                                term: resp.term,
                                                success: false,
                                                last_log_index: 0,
                                            },
                                        );
                                    }
                                    debug!(group_id, peer, "install_snapshot sent");
                                }
                                Err(e) => {
                                    warn!(
                                        group_id, peer, error = %e,
                                        "install_snapshot RPC failed"
                                    );
                                }
                            }
                        });
                    }
                }
            }
        }
    }

    /// Propose a command to the Raft group owning the given vShard.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose(&self, vshard_id: u16, data: Vec<u8>) -> Result<(u64, u64)> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose(vshard_id, data)
    }

    /// Snapshot all Raft group states for observability (SHOW RAFT GROUPS).
    pub fn group_statuses(&self) -> Vec<crate::multi_raft::GroupStatus> {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.group_statuses()
    }

    /// Propose a configuration change to a Raft group.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose_conf_change(&self, group_id: u64, change: &ConfChange) -> Result<(u64, u64)> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_conf_change(group_id, change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutingTable;
    use nodedb_types::config::tuning::ClusterTransportTuning;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    /// Test applier that counts applied entries.
    pub(super) struct CountingApplier {
        applied: AtomicU64,
    }

    impl CountingApplier {
        pub(super) fn new() -> Self {
            Self {
                applied: AtomicU64::new(0),
            }
        }

        pub(super) fn count(&self) -> u64 {
            self.applied.load(Ordering::Relaxed)
        }
    }

    impl CommitApplier for CountingApplier {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            self.applied
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
            entries.last().map(|e| e.index).unwrap_or(0)
        }
    }

    /// Helper: create a transport on an ephemeral port.
    fn make_transport(node_id: u64) -> Arc<NexarTransport> {
        Arc::new(NexarTransport::new(node_id, "127.0.0.1:0".parse().unwrap()).unwrap())
    }

    #[tokio::test]
    async fn single_node_raft_loop_commits() {
        // Single-node cluster: elections and commits happen locally.
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        // Force election.
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let applier = CountingApplier::new();
        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = Arc::new(RaftLoop::new(mr, transport, topo, applier));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let rl = raft_loop.clone();
        let run_handle = tokio::spawn(async move {
            rl.run(shutdown_rx).await;
        });

        // Wait for election + no-op commit.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The no-op entry should have been committed and applied.
        assert!(
            raft_loop.applier.count() >= 1,
            "expected at least 1 applied entry (no-op), got {}",
            raft_loop.applier.count()
        );

        // Propose a command.
        let (_gid, idx) = raft_loop.propose(0, b"hello".to_vec()).unwrap();
        assert!(idx >= 2); // 1 = no-op, 2+ = our command

        // Wait for commit.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            raft_loop.applier.count() >= 2,
            "expected at least 2 applied entries, got {}",
            raft_loop.applier.count()
        );

        shutdown_tx.send(true).unwrap();
        run_handle.abort();
    }

    #[tokio::test]
    async fn three_node_election_over_quic() {
        // Three-node cluster: node 1 should win election via QUIC RPCs.
        let t1 = make_transport(1);
        let t2 = make_transport(2);
        let t3 = make_transport(3);

        // Register peers.
        t1.register_peer(2, t2.local_addr());
        t1.register_peer(3, t3.local_addr());
        t2.register_peer(1, t1.local_addr());
        t2.register_peer(3, t3.local_addr());
        t3.register_peer(1, t1.local_addr());
        t3.register_peer(2, t2.local_addr());

        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);

        // Node 1: force immediate election.
        let dir1 = tempfile::tempdir().unwrap();
        let mut mr1 = MultiRaft::new(1, rt.clone(), dir1.path().to_path_buf());
        mr1.add_group(0, vec![2, 3]).unwrap();
        for node in mr1.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        // Nodes 2 and 3: normal timeouts (won't start election).
        let transport_tuning = ClusterTransportTuning::default();
        let election_timeout_min = Duration::from_secs(transport_tuning.election_timeout_min_secs);
        let election_timeout_max = Duration::from_secs(transport_tuning.election_timeout_max_secs);

        let dir2 = tempfile::tempdir().unwrap();
        let mut mr2 = MultiRaft::new(2, rt.clone(), dir2.path().to_path_buf())
            .with_election_timeout(election_timeout_min, election_timeout_max);
        mr2.add_group(0, vec![1, 3]).unwrap();

        let dir3 = tempfile::tempdir().unwrap();
        let mut mr3 = MultiRaft::new(3, rt.clone(), dir3.path().to_path_buf())
            .with_election_timeout(election_timeout_min, election_timeout_max);
        mr3.add_group(0, vec![1, 2]).unwrap();

        let a1 = CountingApplier::new();
        let a2 = CountingApplier::new();
        let a3 = CountingApplier::new();

        let topo1 = Arc::new(RwLock::new(ClusterTopology::new()));
        let topo2 = Arc::new(RwLock::new(ClusterTopology::new()));
        let topo3 = Arc::new(RwLock::new(ClusterTopology::new()));

        let rl1 = Arc::new(RaftLoop::new(mr1, t1.clone(), topo1, a1));
        let rl2 = Arc::new(RaftLoop::new(mr2, t2.clone(), topo2, a2));
        let rl3 = Arc::new(RaftLoop::new(mr3, t3.clone(), topo3, a3));

        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        // Start serve loops (incoming RPCs).
        let rl2_h = rl2.clone();
        let sr2 = shutdown_tx.subscribe();
        tokio::spawn(async move { t2.serve(rl2_h, sr2).await });

        let rl3_h = rl3.clone();
        let sr3 = shutdown_tx.subscribe();
        tokio::spawn(async move { t3.serve(rl3_h, sr3).await });

        // Start tick loops.
        let rl1_r = rl1.clone();
        let sr1 = shutdown_tx.subscribe();
        tokio::spawn(async move { rl1_r.run(sr1).await });

        let rl2_r = rl2.clone();
        let sr2r = shutdown_tx.subscribe();
        tokio::spawn(async move { rl2_r.run(sr2r).await });

        let rl3_r = rl3.clone();
        let sr3r = shutdown_tx.subscribe();
        tokio::spawn(async move { rl3_r.run(sr3r).await });

        // Also start serve for node 1 (receives responses).
        let rl1_h = rl1.clone();
        let sr1h = shutdown_tx.subscribe();
        tokio::spawn(async move { t1.serve(rl1_h, sr1h).await });

        // Wait for election + replication.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Node 1 should be leader and have committed the no-op.
        assert!(
            rl1.applier.count() >= 1,
            "node 1 should have committed at least the no-op, got {}",
            rl1.applier.count()
        );

        // Propose a command on node 1.
        let (_gid, idx) = rl1.propose(0, b"distributed-cmd".to_vec()).unwrap();
        assert!(idx >= 2);

        // Wait for replication.
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(
            rl1.applier.count() >= 2,
            "node 1: expected >= 2 applied, got {}",
            rl1.applier.count()
        );

        // Followers apply committed entries when they receive AppendEntries
        // with updated leader_commit.
        assert!(
            rl2.applier.count() >= 1,
            "node 2: expected >= 1 applied, got {}",
            rl2.applier.count()
        );
        assert!(
            rl3.applier.count() >= 1,
            "node 3: expected >= 1 applied, got {}",
            rl3.applier.count()
        );

        shutdown_tx.send(true).unwrap();
    }
}
