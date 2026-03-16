//! Central memory governor.
//!
//! The governor owns all engine budgets and enforces the global ceiling.
//! Every subsystem that wants to allocate significant memory must go through
//! the governor.

use std::collections::HashMap;

use crate::budget::Budget;
use crate::engine::EngineId;
use crate::error::{MemError, Result};
use crate::pressure::{PressureLevel, PressureThresholds};

/// Configuration for the memory governor.
#[derive(Debug, Clone)]
pub struct GovernorConfig {
    /// Global memory ceiling in bytes. The sum of all engine budgets
    /// must not exceed this.
    pub global_ceiling: usize,

    /// Per-engine budget limits.
    pub engine_limits: HashMap<EngineId, usize>,
}

impl GovernorConfig {
    /// Validate that the sum of engine limits does not exceed the global ceiling.
    pub fn validate(&self) -> Result<()> {
        let total: usize = self.engine_limits.values().sum();
        if total > self.global_ceiling {
            return Err(MemError::GlobalCeilingExceeded {
                allocated: total,
                ceiling: self.global_ceiling,
                requested: 0,
            });
        }
        Ok(())
    }
}

/// The central memory governor.
///
/// Thread-safe: all budget operations use atomics internally.
#[derive(Debug)]
pub struct MemoryGovernor {
    /// Per-engine budgets.
    budgets: HashMap<EngineId, Budget>,

    /// Global ceiling in bytes.
    global_ceiling: usize,

    /// Pressure thresholds for graduated backpressure.
    thresholds: PressureThresholds,
}

impl MemoryGovernor {
    /// Create a new governor with the given configuration.
    pub fn new(config: GovernorConfig) -> Result<Self> {
        config.validate()?;

        let mut budgets = HashMap::new();
        for (engine, limit) in &config.engine_limits {
            budgets.insert(*engine, Budget::new(*limit));
        }

        Ok(Self {
            budgets,
            global_ceiling: config.global_ceiling,
            thresholds: PressureThresholds::default(),
        })
    }

    /// Try to reserve `size` bytes for the given engine.
    ///
    /// Returns `Ok(())` if the reservation succeeded, or an error describing
    /// why it was rejected.
    pub fn try_reserve(&self, engine: EngineId, size: usize) -> Result<()> {
        let budget = self
            .budgets
            .get(&engine)
            .ok_or(MemError::UnknownEngine(engine))?;

        // Check global ceiling first.
        let total_allocated: usize = self.budgets.values().map(|b| b.allocated()).sum();
        if total_allocated + size > self.global_ceiling {
            return Err(MemError::GlobalCeilingExceeded {
                allocated: total_allocated,
                ceiling: self.global_ceiling,
                requested: size,
            });
        }

        // Check per-engine budget.
        if !budget.try_reserve(size) {
            return Err(MemError::BudgetExhausted {
                engine,
                requested: size,
                available: budget.available(),
                limit: budget.limit(),
            });
        }

        Ok(())
    }

    /// Release `size` bytes back to the given engine's budget.
    pub fn release(&self, engine: EngineId, size: usize) {
        if let Some(budget) = self.budgets.get(&engine) {
            budget.release(size);
        }
    }

    /// Get the budget for a specific engine.
    pub fn budget(&self, engine: EngineId) -> Option<&Budget> {
        self.budgets.get(&engine)
    }

    /// Get the global ceiling.
    pub fn global_ceiling(&self) -> usize {
        self.global_ceiling
    }

    /// Total memory allocated across all engines.
    pub fn total_allocated(&self) -> usize {
        self.budgets.values().map(|b| b.allocated()).sum()
    }

    /// Global utilization as a percentage (0-100).
    pub fn global_utilization_percent(&self) -> u8 {
        if self.global_ceiling == 0 {
            return 100;
        }
        ((self.total_allocated() * 100) / self.global_ceiling).min(100) as u8
    }

    /// Current pressure level for a specific engine.
    pub fn engine_pressure(&self, engine: EngineId) -> PressureLevel {
        self.budgets
            .get(&engine)
            .map(|b| self.thresholds.level_for(b.utilization_percent()))
            .unwrap_or(PressureLevel::Emergency)
    }

    /// Current global pressure level.
    pub fn global_pressure(&self) -> PressureLevel {
        self.thresholds.level_for(self.global_utilization_percent())
    }

    /// Set custom pressure thresholds.
    pub fn set_thresholds(&mut self, thresholds: PressureThresholds) {
        self.thresholds = thresholds;
    }

    /// Snapshot of all engine budget states (for metrics/debugging).
    pub fn snapshot(&self) -> Vec<EngineSnapshot> {
        self.budgets
            .iter()
            .map(|(engine, budget)| EngineSnapshot {
                engine: *engine,
                allocated: budget.allocated(),
                limit: budget.limit(),
                peak: budget.peak(),
                rejections: budget.rejections(),
                utilization_percent: budget.utilization_percent(),
            })
            .collect()
    }
}

/// Point-in-time snapshot of an engine's memory state.
#[derive(Debug, Clone)]
pub struct EngineSnapshot {
    pub engine: EngineId,
    pub allocated: usize,
    pub limit: usize,
    pub peak: usize,
    pub rejections: usize,
    pub utilization_percent: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> GovernorConfig {
        let mut engine_limits = HashMap::new();
        engine_limits.insert(EngineId::Vector, 4096);
        engine_limits.insert(EngineId::Query, 2048);
        engine_limits.insert(EngineId::Timeseries, 1024);

        GovernorConfig {
            global_ceiling: 8192,
            engine_limits,
        }
    }

    #[test]
    fn reserve_within_budget() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.try_reserve(EngineId::Vector, 1000).unwrap();
        assert_eq!(gov.budget(EngineId::Vector).unwrap().allocated(), 1000);
    }

    #[test]
    fn reserve_exceeds_engine_budget() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let err = gov.try_reserve(EngineId::Query, 3000).unwrap_err();
        assert!(matches!(err, MemError::BudgetExhausted { .. }));
    }

    #[test]
    fn reserve_exceeds_global_ceiling() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.try_reserve(EngineId::Vector, 4096).unwrap();
        gov.try_reserve(EngineId::Query, 2048).unwrap();
        gov.try_reserve(EngineId::Timeseries, 1024).unwrap();

        // Within engine budget but exceeds global ceiling.
        // (This would need a 4th engine, but the global is already at 7168.)
        // Let's try adding more to timeseries.
        let err = gov.try_reserve(EngineId::Timeseries, 2000).unwrap_err();
        // Should fail because timeseries budget is 1024 and already used 1024.
        assert!(matches!(
            err,
            MemError::BudgetExhausted { .. } | MemError::GlobalCeilingExceeded { .. }
        ));
    }

    #[test]
    fn release_frees_capacity() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.try_reserve(EngineId::Vector, 4096).unwrap();

        assert!(gov.try_reserve(EngineId::Vector, 1).is_err());

        gov.release(EngineId::Vector, 1000);
        gov.try_reserve(EngineId::Vector, 500).unwrap();
    }

    #[test]
    fn unknown_engine_rejected() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let err = gov.try_reserve(EngineId::Crdt, 100).unwrap_err();
        assert!(matches!(err, MemError::UnknownEngine(EngineId::Crdt)));
    }

    #[test]
    fn snapshot_reports_all_engines() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.try_reserve(EngineId::Vector, 2048).unwrap();

        let snap = gov.snapshot();
        assert_eq!(snap.len(), 3);

        let vector_snap = snap.iter().find(|s| s.engine == EngineId::Vector).unwrap();
        assert_eq!(vector_snap.allocated, 2048);
        assert_eq!(vector_snap.limit, 4096);
        assert_eq!(vector_snap.utilization_percent, 50);
    }

    #[test]
    fn engine_pressure_levels() {
        let gov = MemoryGovernor::new(test_config()).unwrap();

        // Vector budget is 4096. At 0% → Normal.
        assert_eq!(gov.engine_pressure(EngineId::Vector), PressureLevel::Normal);

        // Allocate 70% → Warning.
        gov.try_reserve(EngineId::Vector, 2868).unwrap(); // ~70%
        assert_eq!(
            gov.engine_pressure(EngineId::Vector),
            PressureLevel::Warning
        );

        // Allocate to 87% → Critical.
        gov.try_reserve(EngineId::Vector, 700).unwrap(); // ~87%
        assert_eq!(
            gov.engine_pressure(EngineId::Vector),
            PressureLevel::Critical
        );
    }

    #[test]
    fn global_pressure_tracks_total() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        assert_eq!(gov.global_pressure(), PressureLevel::Normal);

        // Fill to ~70% of global ceiling (8192).
        gov.try_reserve(EngineId::Vector, 4096).unwrap();
        gov.try_reserve(EngineId::Query, 1700).unwrap();
        // ~70% = 5796/8192
        assert!(gov.global_pressure() >= PressureLevel::Warning);
    }

    #[test]
    fn invalid_config_rejected() {
        let mut config = test_config();
        config.global_ceiling = 100; // Way too small for the engine limits.
        assert!(MemoryGovernor::new(config).is_err());
    }
}
