//! vShard migration executor — drives the 3-phase migration state machine.
//!
//! **Phase 1 (Base Copy):** Add target node to source Raft group as learner.
//! Raft replication handles data transfer (AppendEntries with committed log entries).
//!
//! **Phase 2 (WAL Catch-Up):** Monitor target's replication lag. When the target's
//! commit_index is within threshold of the leader's, catch-up is ready.
//!
//! **Phase 3 (Atomic Cut-Over):** Propose a routing table update through Raft.
//! Once committed on all replicas, the vShard is atomically owned by the target group.
//! Create ghost stubs on the source for transparent scatter-gather.

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tracing::{debug, info};

use crate::conf_change::{ConfChange, ConfChangeType};
use crate::decommission::MetadataProposer;
use crate::error::{ClusterError, Result};
use crate::ghost::{GhostStub, GhostTable};
use crate::metadata_group::{MetadataEntry, RoutingChange};
use crate::migration::{MigrationPhase, MigrationState};
use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;
use crate::transport::NexarTransport;

/// Configuration for a vShard migration.
#[derive(Debug, Clone)]
pub struct MigrationRequest {
    pub vshard_id: u16,
    pub source_node: u64,
    pub target_node: u64,
    /// Maximum allowed write pause during Phase 3 (microseconds).
    pub write_pause_budget_us: u64,
}

impl Default for MigrationRequest {
    fn default() -> Self {
        Self {
            vshard_id: 0,
            source_node: 0,
            target_node: 0,
            write_pause_budget_us: 500_000, // 500ms default budget.
        }
    }
}

/// Result of a completed migration.
#[derive(Debug)]
pub struct MigrationResult {
    pub vshard_id: u16,
    pub source_node: u64,
    pub target_node: u64,
    pub phase: MigrationPhase,
    pub elapsed: Option<Duration>,
}

/// Executes a vShard migration through the 3-phase protocol.
///
/// Coordinates between MultiRaft (for Raft membership + proposal), RoutingTable
/// (for vShard ownership), and the transport layer (for data transfer).
pub struct MigrationExecutor {
    multi_raft: Arc<Mutex<MultiRaft>>,
    routing: Arc<RwLock<RoutingTable>>,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<NexarTransport>,
    ghost_table: Arc<Mutex<GhostTable>>,
    /// Optional metadata proposer for replicated routing updates.
    /// When set, Phase 3 cut-over proposes a `RoutingChange` through
    /// the metadata Raft group so every node applies the routing
    /// update atomically on commit. When `None`, falls back to
    /// local-only routing mutation (used by tests that don't stand
    /// up a metadata group).
    metadata_proposer: Option<Arc<dyn MetadataProposer>>,
}

impl MigrationExecutor {
    pub fn new(
        multi_raft: Arc<Mutex<MultiRaft>>,
        routing: Arc<RwLock<RoutingTable>>,
        topology: Arc<RwLock<ClusterTopology>>,
        transport: Arc<NexarTransport>,
    ) -> Self {
        Self {
            multi_raft,
            routing,
            topology,
            transport,
            ghost_table: Arc::new(Mutex::new(GhostTable::new())),
            metadata_proposer: None,
        }
    }

    /// Attach a metadata proposer for replicated Phase 3 cut-over.
    /// Production wiring calls this; tests may omit it for simplicity.
    pub fn with_metadata_proposer(mut self, proposer: Arc<dyn MetadataProposer>) -> Self {
        self.metadata_proposer = Some(proposer);
        self
    }

    /// Access the ghost table (for scatter-gather resolution).
    pub fn ghost_table(&self) -> &Arc<Mutex<GhostTable>> {
        &self.ghost_table
    }

    /// Execute a full 3-phase migration.
    ///
    /// This must be called on the source node (the current leader for the vShard's group).
    pub async fn execute(&self, req: MigrationRequest) -> Result<MigrationResult> {
        // Resolve the source group from routing.
        let source_group = {
            let routing = self.routing.read().unwrap_or_else(|p| p.into_inner());
            routing.group_for_vshard(req.vshard_id)?
        };

        let mut state = MigrationState::new(
            req.vshard_id,
            source_group,
            source_group, // Target group is same group with new member.
            req.source_node,
            req.target_node,
            req.write_pause_budget_us,
        );

        info!(
            vshard = req.vshard_id,
            source = req.source_node,
            target = req.target_node,
            group = source_group,
            "starting vShard migration"
        );

        // ── Phase 1: Add target to Raft group (base copy via replication) ──

        self.phase1_base_copy(&mut state, source_group, &req)
            .await?;

        // ── Phase 2: WAL catch-up (monitor replication lag) ──

        self.phase2_wal_catchup(&mut state, source_group, &req)
            .await?;

        // ── Phase 3: Atomic cut-over (routing update via Raft) ──

        self.phase3_cutover(&mut state, source_group, &req).await?;

        let elapsed = state.elapsed();
        let phase = state.phase().clone();

        info!(
            vshard = req.vshard_id,
            source = req.source_node,
            target = req.target_node,
            elapsed_ms = elapsed.map(|d| d.as_millis() as u64).unwrap_or(0),
            "vShard migration completed"
        );

        Ok(MigrationResult {
            vshard_id: req.vshard_id,
            source_node: req.source_node,
            target_node: req.target_node,
            phase,
            elapsed,
        })
    }

    /// Phase 1: Add target node to the Raft group.
    ///
    /// Raft replication automatically transfers committed log entries to the new
    /// member. This is the "base copy" — the new node receives all historical
    /// state through Raft's AppendEntries mechanism.
    async fn phase1_base_copy(
        &self,
        state: &mut MigrationState,
        group_id: u64,
        req: &MigrationRequest,
    ) -> Result<()> {
        // Estimate base copy size (approximation: number of committed entries).
        let committed = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let statuses = mr.group_statuses();
            statuses
                .iter()
                .find(|s| s.group_id == group_id)
                .map(|s| s.commit_index)
                .unwrap_or(0)
        };
        state.start_base_copy(committed);

        info!(
            vshard = req.vshard_id,
            group = group_id,
            target = req.target_node,
            entries = committed,
            "phase 1: adding target to raft group"
        );

        // Add target node as a LEARNER so it can catch up via Raft
        // replication without participating in elections or voting.
        // Promotion to voter happens after Phase 2 confirms catch-up.
        let change = ConfChange {
            change_type: ConfChangeType::AddLearner,
            node_id: req.target_node,
        };

        {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.propose_conf_change(group_id, &change)?;
        }

        // Register the target peer in the transport so AppendEntries can reach it.
        if let Some(node_info) = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.get_node(req.target_node).map(|n| n.addr.clone())
        } && let Ok(addr) = node_info.parse()
        {
            self.transport.register_peer(req.target_node, addr);
        }

        // The ConfChange will be replicated and applied. The target node
        // receives the full log through Raft's normal replication.
        // Mark base copy as complete — Raft replication is now in
        // progress; the real progress signal is match_index in Phase 2.
        state.update_base_copy(committed);

        debug!(
            vshard = req.vshard_id,
            "phase 1 complete: target added as learner to raft group"
        );

        Ok(())
    }

    /// Phase 2: Monitor target's replication lag until catch-up is ready.
    async fn phase2_wal_catchup(
        &self,
        state: &mut MigrationState,
        group_id: u64,
        req: &MigrationRequest,
    ) -> Result<()> {
        let leader_commit = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let statuses = mr.group_statuses();
            statuses
                .iter()
                .find(|s| s.group_id == group_id)
                .map(|s| s.commit_index)
                .unwrap_or(0)
        };

        state.start_wal_catchup(leader_commit, leader_commit);

        info!(
            vshard = req.vshard_id,
            leader_commit, "phase 2: monitoring replication lag"
        );

        // Capture the initial connection stable_id to the target.
        // If this changes during catch-up, it means the connection was replaced
        // (possible node crash + restart at same address), and we must abort.
        let initial_stable_id = self.transport.peer_connection_stable_id(req.target_node);

        // Also capture the target's address from topology for change detection.
        let initial_target_addr = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.get_node(req.target_node).map(|n| n.addr.clone())
        };

        // Poll until the target has caught up by checking the leader's
        // match_index for the target node. This confirms the target has
        // actually replicated the data, not just the leader's commit index.
        let poll_interval = Duration::from_millis(100);
        let timeout = Duration::from_secs(60);
        let deadline = std::time::Instant::now() + timeout;

        loop {
            tokio::time::sleep(poll_interval).await;

            // Verify peer identity hasn't changed mid-transfer.
            // Check 1: connection stable_id — detects QUIC connection replacement.
            if let Some(initial_id) = initial_stable_id {
                match self.transport.peer_connection_stable_id(req.target_node) {
                    Some(current_id) if current_id != initial_id => {
                        let reason = format!(
                            "peer identity changed mid-migration: connection stable_id {} -> {} for node {}",
                            initial_id, current_id, req.target_node
                        );
                        state.fail(reason.clone());
                        return Err(ClusterError::Transport { detail: reason });
                    }
                    None => {
                        // Connection dropped — peer may have crashed.
                        let reason = format!(
                            "connection to target node {} lost during migration",
                            req.target_node
                        );
                        state.fail(reason.clone());
                        return Err(ClusterError::Transport { detail: reason });
                    }
                    _ => {}
                }
            }

            // Check 2: topology address — detects node replacement at different address.
            {
                let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
                let current_addr = topo.get_node(req.target_node).map(|n| n.addr.clone());
                if current_addr != initial_target_addr {
                    let reason = format!(
                        "target node {} address changed during migration: {:?} -> {:?}",
                        req.target_node, initial_target_addr, current_addr
                    );
                    state.fail(reason.clone());
                    return Err(ClusterError::Transport { detail: reason });
                }
            }

            let (leader_commit, target_match) = {
                let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let statuses = mr.group_statuses();
                let commit = statuses
                    .iter()
                    .find(|s| s.group_id == group_id)
                    .map(|s| s.commit_index)
                    .unwrap_or(0);
                // Query the target's match_index from the leader's replication state.
                let target_match = mr.match_index_for(group_id, req.target_node).unwrap_or(0);
                (commit, target_match)
            };

            state.update_wal_catchup(leader_commit, target_match);

            if state.is_catchup_ready() {
                // Learner has caught up — promote to voter so the
                // group has enough replicas for a safe cut-over.
                let promote = ConfChange {
                    change_type: ConfChangeType::PromoteLearner,
                    node_id: req.target_node,
                };
                {
                    let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                    mr.propose_conf_change(group_id, &promote)?;
                }
                debug!(
                    vshard = req.vshard_id,
                    leader_commit,
                    target_match,
                    "phase 2 complete: target caught up and promoted to voter"
                );
                return Ok(());
            }

            if std::time::Instant::now() >= deadline {
                let reason = format!(
                    "WAL catch-up timed out after {}s (leader={leader_commit}, target={target_match})",
                    timeout.as_secs()
                );
                state.fail(reason.clone());
                return Err(ClusterError::Transport { detail: reason });
            }
        }
    }

    /// Phase 3: Atomic routing table update.
    ///
    /// When a [`MetadataProposer`] is attached, the cut-over proposes
    /// a `LeadershipTransfer` through the metadata Raft group so
    /// every node applies the routing update atomically on commit.
    /// Without a proposer (tests), falls back to a local-only
    /// mutation.
    async fn phase3_cutover(
        &self,
        state: &mut MigrationState,
        group_id: u64,
        req: &MigrationRequest,
    ) -> Result<()> {
        let estimated_pause_us = 10_000;

        state.start_cutover(estimated_pause_us).map_err(|e| {
            state.fail(format!("cutover rejected: {e}"));
            e
        })?;

        let cutover_start = std::time::Instant::now();

        info!(
            vshard = req.vshard_id,
            estimated_pause_us, "phase 3: atomic cut-over"
        );

        // Propose the routing change. With a metadata proposer the
        // `CacheApplier::with_live_state` on every node handles the
        // actual routing mutation when the entry commits; without a
        // proposer we mutate locally for backward-compat.
        if let Some(proposer) = &self.metadata_proposer {
            let entry = MetadataEntry::RoutingChange(RoutingChange::LeadershipTransfer {
                group_id,
                new_leader_node_id: req.target_node,
            });
            proposer.propose_and_wait(entry).await?;
        } else {
            let mut routing = self.routing.write().unwrap_or_else(|p| p.into_inner());
            routing.set_leader(group_id, req.target_node);
        }

        // Ghost stub so in-flight scatter-gather queries that still
        // target the old leader are transparently forwarded.
        {
            let mut ghosts = self.ghost_table.lock().unwrap_or_else(|p| p.into_inner());
            ghosts.insert(GhostStub {
                node_id: format!("vshard-{}", req.vshard_id),
                target_shard: req.vshard_id,
                refcount: 1,
                created_at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            });
        }

        let actual_pause_us = cutover_start.elapsed().as_micros() as u64;
        state.complete(actual_pause_us);

        debug!(
            vshard = req.vshard_id,
            actual_pause_us, "phase 3 complete: routing updated"
        );

        Ok(())
    }
}

/// Track active migrations across the cluster.
pub struct MigrationTracker {
    active: Mutex<Vec<MigrationState>>,
}

impl MigrationTracker {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(Vec::new()),
        }
    }

    pub fn add(&self, state: MigrationState) {
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        active.push(state);
    }

    pub fn active_count(&self) -> usize {
        let active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        active.iter().filter(|s| s.is_active()).count()
    }

    /// Snapshot of all migration states for observability.
    pub fn snapshot(&self) -> Vec<MigrationSnapshot> {
        let active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        active
            .iter()
            .map(|s| MigrationSnapshot {
                vshard_id: s.vshard_id(),
                phase: format!("{:?}", s.phase()),
                elapsed_ms: s.elapsed().map(|d| d.as_millis() as u64).unwrap_or(0),
                is_active: s.is_active(),
            })
            .collect()
    }

    /// Remove completed/failed migrations older than the given age.
    pub fn gc(&self, max_age: Duration) {
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        active.retain(|s| s.is_active() || s.elapsed().map(|d| d < max_age).unwrap_or(true));
    }
}

impl Default for MigrationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Observability snapshot of a migration.
#[derive(Debug, Clone)]
pub struct MigrationSnapshot {
    pub vshard_id: u16,
    pub phase: String,
    pub elapsed_ms: u64,
    pub is_active: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutingTable;
    use crate::topology::ClusterTopology;

    #[test]
    fn migration_tracker_lifecycle() {
        let tracker = MigrationTracker::new();
        assert_eq!(tracker.active_count(), 0);

        let mut state = MigrationState::new(0, 0, 1, 1, 2, 500_000);
        state.start_base_copy(100);
        tracker.add(state);

        assert_eq!(tracker.active_count(), 1);
        assert_eq!(tracker.snapshot().len(), 1);
        assert!(tracker.snapshot()[0].is_active);
    }

    #[tokio::test]
    async fn migration_executor_phase1() {
        // Test that phase 1 adds the target node to the Raft group.
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = crate::multi_raft::MultiRaft::new(1, rt.clone(), dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        // Make node 1 the leader (single-node → auto-elected).
        use std::time::Instant;
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        // Tick to trigger election.
        let _ = mr.tick();
        // Drain ready to consume the no-op.
        for (gid, ready) in mr.tick().groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).unwrap();
            }
        }

        let multi_raft = Arc::new(Mutex::new(mr));
        let routing = Arc::new(RwLock::new(rt));
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        let transport = Arc::new(NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap());

        let executor = MigrationExecutor::new(multi_raft.clone(), routing, topology, transport);

        let mut state = MigrationState::new(0, 0, 0, 1, 2, 500_000);

        let req = MigrationRequest {
            vshard_id: 0,
            source_node: 1,
            target_node: 2,
            write_pause_budget_us: 500_000,
        };

        // Phase 1 should succeed (adds node 2 as learner to group 0).
        executor
            .phase1_base_copy(&mut state, 0, &req)
            .await
            .unwrap();

        // Verify: the ConfChange (AddLearner) was proposed in the Raft log.
        // Application happens on next tick/commit cycle.
    }

    #[test]
    fn migration_request_default() {
        let req = MigrationRequest::default();
        assert_eq!(req.write_pause_budget_us, 500_000);
    }
}
