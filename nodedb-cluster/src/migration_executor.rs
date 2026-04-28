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
//!
//! ## Crash-safety
//!
//! A `MigrationCheckpoint` entry is proposed to the metadata Raft group at every
//! phase boundary. On coordinator restart, `recover_in_flight_migrations` scans the
//! `MigrationStateTable` and resumes or aborts each in-flight migration.

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, info};
use uuid::Uuid;

use crate::catalog::ClusterCatalog;
use crate::conf_change::{ConfChange, ConfChangeType};
use crate::decommission::MetadataProposer;
use crate::error::{ClusterError, Result};
use crate::ghost::{GhostStub, GhostTable};
use crate::metadata_group::migration_recovery::{
    RecoveryDecision, recover_in_flight_migrations as recovery_scan,
};
use crate::metadata_group::migration_state::{
    MigrationCheckpointPayload, MigrationId, MigrationPhaseTag, SharedMigrationStateTable,
};
use crate::metadata_group::{MetadataEntry as Entry, RoutingChange};
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
    pub migration_id: MigrationId,
}

/// Executes a vShard migration through the 3-phase protocol.
pub struct MigrationExecutor {
    multi_raft: Arc<Mutex<MultiRaft>>,
    routing: Arc<RwLock<RoutingTable>>,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<NexarTransport>,
    ghost_table: Arc<Mutex<GhostTable>>,
    /// Optional catalog for ghost stub persistence.
    catalog: Option<Arc<ClusterCatalog>>,
    /// Optional metadata proposer for replicated routing updates + checkpoints.
    metadata_proposer: Option<Arc<dyn MetadataProposer>>,
    /// Optional migration state table for crash-safe checkpoints.
    migration_state: Option<SharedMigrationStateTable>,
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
            catalog: None,
            metadata_proposer: None,
            migration_state: None,
        }
    }

    /// Attach a metadata proposer for replicated Phase 3 cut-over and checkpoints.
    pub fn with_metadata_proposer(mut self, proposer: Arc<dyn MetadataProposer>) -> Self {
        self.metadata_proposer = Some(proposer);
        self
    }

    /// Attach a catalog for durable ghost stub persistence.
    pub fn with_catalog(mut self, catalog: Arc<ClusterCatalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Attach a shared migration state table for crash-safe checkpoints.
    pub fn with_migration_state(mut self, state: SharedMigrationStateTable) -> Self {
        self.migration_state = Some(state);
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

        // Guard: check if a migration for this vshard is already in progress
        // (deduplication across rebalancer ticks — audit gap #4).
        if let Some(state_table) = &self.migration_state {
            let guard = state_table.lock().unwrap_or_else(|p| p.into_inner());
            for row in guard.all_checkpoints() {
                if let Some(_id) = row.migration_uuid() {
                    let vshard_matches = match &row.payload {
                        MigrationCheckpointPayload::AddLearner { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::CatchUp { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::PromoteLearner { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::LeadershipTransfer { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::Cutover { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                        MigrationCheckpointPayload::Complete { vshard_id, .. } => {
                            *vshard_id == req.vshard_id
                        }
                    };
                    if vshard_matches && row.payload.phase_tag() != MigrationPhaseTag::Complete {
                        return Err(ClusterError::MigrationInProgress {
                            vshard_id: req.vshard_id,
                        });
                    }
                }
            }
        }

        let migration_id = Uuid::new_v4();

        let mut state = MigrationState::new(
            req.vshard_id,
            source_group,
            source_group,
            req.source_node,
            req.target_node,
            req.write_pause_budget_us,
        );

        info!(
            vshard = req.vshard_id,
            source = req.source_node,
            target = req.target_node,
            group = source_group,
            migration_id = %migration_id,
            "starting vShard migration"
        );

        self.phase1_base_copy(&mut state, source_group, &req, migration_id)
            .await?;
        self.phase2_wal_catchup(&mut state, source_group, &req, migration_id)
            .await?;
        self.phase3_cutover(&mut state, source_group, &req, migration_id)
            .await?;

        let elapsed = state.elapsed();
        let phase = state.phase().clone();

        info!(
            vshard = req.vshard_id,
            migration_id = %migration_id,
            elapsed_ms = elapsed.map(|d| d.as_millis() as u64).unwrap_or(0),
            "vShard migration completed"
        );

        Ok(MigrationResult {
            vshard_id: req.vshard_id,
            source_node: req.source_node,
            target_node: req.target_node,
            phase,
            elapsed,
            migration_id,
        })
    }

    // ── Phase 1 ──────────────────────────────────────────────────────────────

    async fn phase1_base_copy(
        &self,
        state: &mut MigrationState,
        group_id: u64,
        req: &MigrationRequest,
        migration_id: MigrationId,
    ) -> Result<()> {
        let committed = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.group_statuses()
                .iter()
                .find(|s| s.group_id == group_id)
                .map(|s| s.commit_index)
                .unwrap_or(0)
        };
        state.start_base_copy(committed);

        // Checkpoint: Phase 1 start — before AddLearner.
        self.propose_checkpoint(
            migration_id,
            0,
            MigrationCheckpointPayload::AddLearner {
                vshard_id: req.vshard_id,
                source_node: req.source_node,
                target_node: req.target_node,
                source_group: group_id,
                write_pause_budget_us: req.write_pause_budget_us,
                started_at_hlc: nodedb_types::Hlc::default(),
            },
        )
        .await?;

        info!(
            vshard = req.vshard_id,
            group = group_id,
            target = req.target_node,
            entries = committed,
            "phase 1: adding target to raft group"
        );

        let change = ConfChange {
            change_type: ConfChangeType::AddLearner,
            node_id: req.target_node,
        };
        let learner_log_index = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.propose_conf_change(group_id, &change)?;
            // The ConfChange is in the log; query committed index as proxy.
            mr.group_statuses()
                .iter()
                .find(|s| s.group_id == group_id)
                .map(|s| s.commit_index)
                .unwrap_or(committed)
        };

        if let Some(node_info) = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.get_node(req.target_node).map(|n| n.addr.clone())
        } && let Ok(addr) = node_info.parse()
        {
            self.transport.register_peer(req.target_node, addr);
        }

        state.update_base_copy(committed);

        // Checkpoint: Phase 2 — AddLearner committed.
        self.propose_checkpoint(
            migration_id,
            1,
            MigrationCheckpointPayload::CatchUp {
                vshard_id: req.vshard_id,
                learner_log_index_at_add: learner_log_index,
            },
        )
        .await?;

        debug!(
            vshard = req.vshard_id,
            "phase 1 complete: target added as learner"
        );
        Ok(())
    }

    // ── Phase 2 ──────────────────────────────────────────────────────────────

    async fn phase2_wal_catchup(
        &self,
        state: &mut MigrationState,
        group_id: u64,
        req: &MigrationRequest,
        migration_id: MigrationId,
    ) -> Result<()> {
        let leader_commit = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.group_statuses()
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

        let initial_stable_id = self.transport.peer_connection_stable_id(req.target_node);
        let initial_target_addr = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.get_node(req.target_node).map(|n| n.addr.clone())
        };

        let poll_interval = Duration::from_millis(100);
        let timeout = Duration::from_secs(60);
        let deadline = std::time::Instant::now() + timeout;

        loop {
            tokio::time::sleep(poll_interval).await;

            if let Some(initial_id) = initial_stable_id {
                match self.transport.peer_connection_stable_id(req.target_node) {
                    Some(current_id) if current_id != initial_id => {
                        let reason = format!(
                            "peer identity changed mid-migration: stable_id {} -> {} for node {}",
                            initial_id, current_id, req.target_node
                        );
                        state.fail(reason.clone());
                        return Err(ClusterError::Transport { detail: reason });
                    }
                    None => {
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

            {
                let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
                let current_addr = topo.get_node(req.target_node).map(|n| n.addr.clone());
                if current_addr != initial_target_addr {
                    let reason = format!(
                        "target node {} address changed: {:?} -> {:?}",
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
                let target_match = mr.match_index_for(group_id, req.target_node).unwrap_or(0);
                (commit, target_match)
            };
            state.update_wal_catchup(leader_commit, target_match);

            if state.is_catchup_ready() {
                let promote = ConfChange {
                    change_type: ConfChangeType::PromoteLearner,
                    node_id: req.target_node,
                };
                {
                    let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                    mr.propose_conf_change(group_id, &promote)?;
                }

                // Checkpoint: PromoteLearner committed.
                self.propose_checkpoint(
                    migration_id,
                    2,
                    MigrationCheckpointPayload::PromoteLearner {
                        vshard_id: req.vshard_id,
                        target_node: req.target_node,
                        source_group: group_id,
                    },
                )
                .await?;

                debug!(
                    vshard = req.vshard_id,
                    leader_commit, target_match, "phase 2 complete: target caught up and promoted"
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

    // ── Phase 3 ──────────────────────────────────────────────────────────────

    async fn phase3_cutover(
        &self,
        state: &mut MigrationState,
        group_id: u64,
        req: &MigrationRequest,
        migration_id: MigrationId,
    ) -> Result<()> {
        let estimated_pause_us = 10_000;

        state.start_cutover(estimated_pause_us).map_err(|e| {
            state.fail(format!("cutover rejected: {e}"));
            e
        })?;

        let cutover_start = std::time::Instant::now();

        // Checkpoint: about to propose LeadershipTransfer.
        self.propose_checkpoint(
            migration_id,
            3,
            MigrationCheckpointPayload::LeadershipTransfer {
                vshard_id: req.vshard_id,
                target_is_voter: true,
                new_leader_node_id: req.target_node,
                source_group: group_id,
            },
        )
        .await?;

        info!(
            vshard = req.vshard_id,
            estimated_pause_us, "phase 3: atomic cut-over"
        );

        if let Some(proposer) = &self.metadata_proposer {
            let entry = Entry::RoutingChange(RoutingChange::LeadershipTransfer {
                group_id,
                new_leader_node_id: req.target_node,
            });
            proposer.propose_and_wait(entry).await?;
        } else {
            let mut routing = self.routing.write().unwrap_or_else(|p| p.into_inner());
            routing.set_leader(group_id, req.target_node);
        }

        // Checkpoint: cut-over committed.
        self.propose_checkpoint(
            migration_id,
            4,
            MigrationCheckpointPayload::Cutover {
                vshard_id: req.vshard_id,
                new_leader_node_id: req.target_node,
                source_group: group_id,
            },
        )
        .await?;

        // Ghost stub durability (audit gap #4):
        // 1. Insert the ghost stub in memory.
        // 2. Persist via save_ghosts BEFORE writing the Complete checkpoint.
        // 3. If save_ghosts fails, we cannot proceed — the cut-over is committed
        //    but the ghost is not durable. Error loudly.
        // 4. On success, write the Complete checkpoint.
        let ghost_stub = GhostStub {
            node_id: format!("vshard-{}", req.vshard_id),
            target_shard: req.vshard_id,
            refcount: 1,
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        {
            let mut ghosts = self.ghost_table.lock().unwrap_or_else(|p| p.into_inner());
            ghosts.insert(ghost_stub);

            // Persist to catalog before marking Complete.
            if let Some(catalog) = &self.catalog {
                catalog.save_ghosts(req.vshard_id, &ghosts)?;
            }
        }

        let actual_pause_us = cutover_start.elapsed().as_micros() as u64;
        state.complete(actual_pause_us);

        // Checkpoint: Complete.
        self.propose_checkpoint(
            migration_id,
            5,
            MigrationCheckpointPayload::Complete {
                vshard_id: req.vshard_id,
                actual_pause_us,
                ghost_stub_installed: true,
            },
        )
        .await?;

        // Clean up the migration state table row — migration is done.
        if let Some(state_table) = &self.migration_state {
            let mut guard = state_table.lock().unwrap_or_else(|p| p.into_inner());
            let _ = guard.remove(&migration_id); // best-effort cleanup
        }

        debug!(
            vshard = req.vshard_id,
            actual_pause_us, "phase 3 complete: routing updated"
        );
        Ok(())
    }

    // ── Checkpoint helper ─────────────────────────────────────────────────────

    async fn propose_checkpoint(
        &self,
        migration_id: MigrationId,
        attempt: u32,
        payload: MigrationCheckpointPayload,
    ) -> Result<()> {
        let Some(proposer) = &self.metadata_proposer else {
            // No proposer wired — checkpoint is optional (tests / local mode).
            return Ok(());
        };

        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let crc32c = payload.crc32c()?;
        let phase = payload.phase_tag();

        let entry = Entry::MigrationCheckpoint {
            migration_id: migration_id.hyphenated().to_string(),
            phase,
            attempt,
            payload,
            crc32c,
            ts_ms,
        };

        proposer.propose_and_wait(entry).await?;
        Ok(())
    }
}

/// Scan in-flight migrations from the state table and either resume or
/// abort them.  Called from coordinator startup after the metadata Raft
/// group is up but before the rebalancer spawns.
///
/// # PHASE-A-WIRE
/// The call site for this function is `RebalancerSubsystem::start` in
/// the `nodedb` crate (after metadata-Raft-ready, before rebalancer-spawn).
/// If direct wiring from this scope is not possible, insert:
/// ```text
/// // PHASE-A-WIRE: call recover_in_flight_migrations here
/// ```
/// at the correct location in `nodedb/src/control/...`.
pub async fn recover_in_flight_migrations(
    migration_state: SharedMigrationStateTable,
    topology: Arc<RwLock<ClusterTopology>>,
    proposer: Arc<dyn MetadataProposer>,
    abort_timeout: std::time::Duration,
) -> Result<Vec<RecoveryDecision>> {
    // Load all rows from redb into memory before scanning.
    {
        let mut guard = migration_state.lock().unwrap_or_else(|p| p.into_inner());
        guard.load_all()?;
    }
    recovery_scan(migration_state, topology, proposer, abort_timeout).await
}

// ── MigrationTracker ─────────────────────────────────────────────────────────

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
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = crate::multi_raft::MultiRaft::new(1, rt.clone(), dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        use std::time::Instant;
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        let _ = mr.tick();
        for (gid, ready) in mr.tick().groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).unwrap();
            }
        }

        let multi_raft = Arc::new(Mutex::new(mr));
        let routing = Arc::new(RwLock::new(rt));
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        let transport = Arc::new(
            NexarTransport::new(
                1,
                "127.0.0.1:0".parse().unwrap(),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .unwrap(),
        );

        let executor = MigrationExecutor::new(multi_raft.clone(), routing, topology, transport);

        let mut state = MigrationState::new(0, 0, 0, 1, 2, 500_000);
        let req = MigrationRequest {
            vshard_id: 0,
            source_node: 1,
            target_node: 2,
            write_pause_budget_us: 500_000,
        };

        // Phase 1 should succeed (no proposer — checkpoint is no-op).
        executor
            .phase1_base_copy(&mut state, 0, &req, Uuid::new_v4())
            .await
            .unwrap();
    }

    #[test]
    fn migration_request_default() {
        let req = MigrationRequest::default();
        assert_eq!(req.write_pause_budget_us, 500_000);
    }
}
