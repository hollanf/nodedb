use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

use synapsedb_mem::EngineId;
use synapsedb_mem::governor::{GovernorConfig, MemoryGovernor};

use crate::config::engine::EngineByteBudgets;

/// Initialize the memory governor from engine byte budgets.
///
/// Called once at startup. The returned governor is shared (via `Arc`)
/// across the Control Plane and all Data Plane cores.
pub fn init_governor(
    global_ceiling: usize,
    budgets: &EngineByteBudgets,
) -> crate::Result<Arc<MemoryGovernor>> {
    let mut engine_limits = HashMap::new();
    engine_limits.insert(EngineId::Vector, budgets.vector);
    engine_limits.insert(EngineId::Sparse, budgets.sparse);
    engine_limits.insert(EngineId::Crdt, budgets.crdt);
    engine_limits.insert(EngineId::Timeseries, budgets.timeseries);
    engine_limits.insert(EngineId::Query, budgets.query);

    let config = GovernorConfig {
        global_ceiling,
        engine_limits,
    };

    let governor = MemoryGovernor::new(config).map_err(|e| crate::Error::Config {
        detail: format!("failed to initialize memory governor: {e}"),
    })?;

    info!(
        global_ceiling,
        vector = budgets.vector,
        sparse = budgets.sparse,
        crdt = budgets.crdt,
        timeseries = budgets.timeseries,
        query = budgets.query,
        "memory governor initialized"
    );

    Ok(Arc::new(governor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::engine::EngineConfig;

    #[test]
    fn init_from_default_config() {
        let cfg = EngineConfig::default();
        let budgets = cfg.to_byte_budgets(1024 * 1024 * 1024); // 1 GiB
        let gov = init_governor(1024 * 1024 * 1024, &budgets).unwrap();

        assert!(gov.budget(EngineId::Vector).is_some());
        assert!(gov.budget(EngineId::Query).is_some());
        assert!(gov.budget(EngineId::Crdt).is_some());
    }

    #[test]
    fn init_rejects_impossible_budgets() {
        let budgets = EngineByteBudgets {
            vector: 500,
            sparse: 500,
            crdt: 500,
            timeseries: 500,
            query: 500,
        };
        // Total = 2500 but ceiling = 1000.
        let result = init_governor(1000, &budgets);
        assert!(result.is_err());
    }
}
