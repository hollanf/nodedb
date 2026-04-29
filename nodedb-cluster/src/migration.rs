use std::time::Instant;

use tracing::{info, warn};

/// 3-phase shard migration state machine.
///
/// 1. **Base Copy**: Target node pulls the vShard's L1 segments via RDMA/QUIC.
/// 2. **WAL Catch-up**: Target subscribes to source WAL, replays live mutations.
/// 3. **Atomic Cut-over**: Raft leader updates routing table atomically.
///
/// Write Pause Disclosure : During Phase 3, writes to the
/// migrating vShard are paused. `migration_write_pause_budget_us` controls the
/// maximum acceptable pause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Not migrating.
    Idle,
    /// Phase 1: Bulk data transfer from source to target.
    BaseCopy {
        bytes_transferred: u64,
        bytes_total: u64,
    },
    /// Phase 2: Target replaying WAL entries from source.
    WalCatchUp {
        /// Source LSN when catch-up started.
        start_lsn: u64,
        /// Current LSN on target.
        current_lsn: u64,
        /// Latest LSN on source.
        source_lsn: u64,
    },
    /// Phase 3: Writes paused, routing table update in progress.
    AtomicCutOver {
        /// When the pause started.
        pause_start_us: u64,
    },
    /// Migration completed successfully.
    Completed {
        /// Actual pause duration in microseconds.
        pause_duration_us: u64,
    },
    /// Migration failed.
    Failed { reason: String },
}

/// Full state of a vShard migration.
#[derive(Debug, Clone)]
pub struct MigrationState {
    /// vShard being migrated.
    pub vshard_id: u32,
    /// Source Raft group.
    pub source_group: u64,
    /// Target Raft group.
    pub target_group: u64,
    /// Source node (current owner).
    pub source_node: u64,
    /// Target node.
    pub target_node: u64,
    /// Current phase.
    pub phase: MigrationPhase,
    /// Maximum write-pause budget in microseconds.
    pub write_pause_budget_us: u64,
    /// When migration was initiated.
    pub started_at: Option<Instant>,
}

impl MigrationState {
    pub fn new(
        vshard_id: u32,
        source_group: u64,
        target_group: u64,
        source_node: u64,
        target_node: u64,
        write_pause_budget_us: u64,
    ) -> Self {
        Self {
            vshard_id,
            source_group,
            target_group,
            source_node,
            target_node,
            phase: MigrationPhase::Idle,
            write_pause_budget_us,
            started_at: None,
        }
    }

    /// Start Phase 1: Base Copy.
    pub fn start_base_copy(&mut self, bytes_total: u64) {
        self.started_at = Some(Instant::now());
        self.phase = MigrationPhase::BaseCopy {
            bytes_transferred: 0,
            bytes_total,
        };
        info!(
            vshard = self.vshard_id,
            source = self.source_node,
            target = self.target_node,
            bytes_total,
            "starting base copy"
        );
    }

    /// Update base copy progress.
    pub fn update_base_copy(&mut self, bytes_transferred: u64) {
        if let MigrationPhase::BaseCopy { bytes_total, .. } = self.phase {
            self.phase = MigrationPhase::BaseCopy {
                bytes_transferred,
                bytes_total,
            };
        }
    }

    /// Transition to Phase 2: WAL Catch-up.
    pub fn start_wal_catchup(&mut self, start_lsn: u64, source_lsn: u64) {
        self.phase = MigrationPhase::WalCatchUp {
            start_lsn,
            current_lsn: start_lsn,
            source_lsn,
        };
        info!(
            vshard = self.vshard_id,
            start_lsn,
            source_lsn,
            lag = source_lsn - start_lsn,
            "starting wal catch-up"
        );
    }

    /// Update WAL catch-up progress.
    pub fn update_wal_catchup(&mut self, current_lsn: u64, source_lsn: u64) {
        if let MigrationPhase::WalCatchUp { start_lsn, .. } = self.phase {
            self.phase = MigrationPhase::WalCatchUp {
                start_lsn,
                current_lsn,
                source_lsn,
            };
        }
    }

    /// Check if WAL lag is low enough to proceed to cut-over.
    pub fn is_catchup_ready(&self) -> bool {
        if let MigrationPhase::WalCatchUp {
            current_lsn,
            source_lsn,
            ..
        } = self.phase
        {
            // Sub-millisecond lag = within 10 LSN entries.
            source_lsn.saturating_sub(current_lsn) <= 10
        } else {
            false
        }
    }

    /// Attempt to start Phase 3: Atomic Cut-over.
    ///
    /// Returns `Err` if the estimated pause would exceed the budget.
    pub fn start_cutover(&mut self, estimated_pause_us: u64) -> crate::Result<()> {
        if estimated_pause_us > self.write_pause_budget_us {
            warn!(
                vshard = self.vshard_id,
                estimated_us = estimated_pause_us,
                budget_us = self.write_pause_budget_us,
                "refusing cut-over: pause exceeds budget"
            );
            return Err(crate::ClusterError::MigrationPauseBudgetExceeded {
                estimated_us: estimated_pause_us,
                budget_us: self.write_pause_budget_us,
            });
        }

        self.phase = MigrationPhase::AtomicCutOver {
            pause_start_us: estimated_pause_us,
        };
        info!(
            vshard = self.vshard_id,
            estimated_pause_us, "starting atomic cut-over"
        );
        Ok(())
    }

    /// Complete the migration.
    pub fn complete(&mut self, actual_pause_us: u64) {
        self.phase = MigrationPhase::Completed {
            pause_duration_us: actual_pause_us,
        };
        info!(
            vshard = self.vshard_id,
            pause_us = actual_pause_us,
            "migration completed"
        );
    }

    /// Mark migration as failed. Source remains authoritative.
    pub fn fail(&mut self, reason: String) {
        warn!(
            vshard = self.vshard_id,
            reason = %reason,
            "migration failed, source remains authoritative"
        );
        self.phase = MigrationPhase::Failed { reason };
    }

    pub fn is_active(&self) -> bool {
        !matches!(
            self.phase,
            MigrationPhase::Idle | MigrationPhase::Completed { .. } | MigrationPhase::Failed { .. }
        )
    }

    /// Elapsed time since migration started.
    pub fn elapsed(&self) -> Option<std::time::Duration> {
        self.started_at.map(|s| s.elapsed())
    }

    pub fn vshard_id(&self) -> u32 {
        self.vshard_id
    }

    pub fn phase(&self) -> &MigrationPhase {
        &self.phase
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_migration() -> MigrationState {
        MigrationState::new(42, 0, 1, 10, 20, 1000)
    }

    #[test]
    fn full_lifecycle() {
        let mut m = test_migration();
        assert!(!m.is_active());

        // Phase 1.
        m.start_base_copy(1_000_000);
        assert!(m.is_active());
        m.update_base_copy(500_000);
        if let MigrationPhase::BaseCopy {
            bytes_transferred, ..
        } = m.phase
        {
            assert_eq!(bytes_transferred, 500_000);
        } else {
            panic!("expected BaseCopy");
        }

        // Phase 2.
        m.start_wal_catchup(100, 200);
        assert!(!m.is_catchup_ready());
        m.update_wal_catchup(195, 200);
        assert!(m.is_catchup_ready());

        // Phase 3.
        m.start_cutover(500).unwrap();
        assert!(matches!(m.phase, MigrationPhase::AtomicCutOver { .. }));

        // Complete.
        m.complete(450);
        assert!(!m.is_active());
        if let MigrationPhase::Completed {
            pause_duration_us, ..
        } = m.phase
        {
            assert_eq!(pause_duration_us, 450);
        }
    }

    #[test]
    fn pause_budget_exceeded() {
        let mut m = test_migration();
        m.start_base_copy(100);
        m.start_wal_catchup(0, 5);
        m.update_wal_catchup(5, 5);

        // Budget is 1000µs, estimated is 2000µs.
        let err = m.start_cutover(2000).unwrap_err();
        assert!(matches!(
            err,
            crate::ClusterError::MigrationPauseBudgetExceeded { .. }
        ));
    }

    #[test]
    fn failure_recovery() {
        let mut m = test_migration();
        m.start_base_copy(100);
        m.fail("network partition".into());
        assert!(!m.is_active());
        assert!(matches!(m.phase, MigrationPhase::Failed { .. }));
    }

    #[test]
    fn catchup_threshold() {
        let mut m = test_migration();
        m.start_base_copy(100);
        m.start_wal_catchup(0, 100);

        // Still 90 behind.
        m.update_wal_catchup(10, 100);
        assert!(!m.is_catchup_ready());

        // Within 10.
        m.update_wal_catchup(95, 100);
        assert!(m.is_catchup_ready());
    }
}
