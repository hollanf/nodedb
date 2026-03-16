use serde::{Deserialize, Serialize};

/// Per-engine memory budget allocation as fractions of the global ceiling.
///
/// These fractions control how the memory governor distributes the global
/// `memory_limit` across engines. The governor maps these to
/// `synapsedb_mem::EngineId` budgets at startup.
///
/// Fractions MUST sum to <= 1.0. Any remainder is unallocated headroom
/// for transient allocations (e.g., sort buffers, network buffers).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Fraction of global memory for the vector engine (HNSW graphs, distance buffers).
    pub vector_budget_fraction: f64,

    /// Fraction of global memory for the sparse/metadata engine (redb, BM25 indexes).
    pub sparse_budget_fraction: f64,

    /// Fraction of global memory for the CRDT engine (loro documents, DLQ buffers).
    pub crdt_budget_fraction: f64,

    /// Fraction of global memory for the timeseries engine (memtables, Gorilla buffers).
    pub timeseries_budget_fraction: f64,

    /// Fraction of global memory for query execution (DataFusion sorts, aggregations).
    pub query_budget_fraction: f64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            vector_budget_fraction: 0.30,
            sparse_budget_fraction: 0.15,
            crdt_budget_fraction: 0.10,
            timeseries_budget_fraction: 0.10,
            query_budget_fraction: 0.20,
            // Remaining 0.15 is unallocated headroom.
        }
    }
}

impl EngineConfig {
    /// Sum of all engine fractions. Must be <= 1.0.
    pub fn total_fraction(&self) -> f64 {
        self.vector_budget_fraction
            + self.sparse_budget_fraction
            + self.crdt_budget_fraction
            + self.timeseries_budget_fraction
            + self.query_budget_fraction
    }

    /// Validate that fractions are sane.
    pub fn validate(&self) -> crate::Result<()> {
        let total = self.total_fraction();
        if total > 1.0 {
            return Err(crate::Error::Config {
                detail: format!("engine budget fractions sum to {total:.2}, must be <= 1.0"),
            });
        }
        if self.vector_budget_fraction < 0.0
            || self.sparse_budget_fraction < 0.0
            || self.crdt_budget_fraction < 0.0
            || self.timeseries_budget_fraction < 0.0
            || self.query_budget_fraction < 0.0
        {
            return Err(crate::Error::Config {
                detail: "engine budget fractions must be non-negative".into(),
            });
        }
        Ok(())
    }

    /// Convert fractions to absolute byte budgets given a global memory limit.
    pub fn to_byte_budgets(&self, global_limit: usize) -> EngineByteBudgets {
        let gl = global_limit as f64;
        EngineByteBudgets {
            vector: (gl * self.vector_budget_fraction) as usize,
            sparse: (gl * self.sparse_budget_fraction) as usize,
            crdt: (gl * self.crdt_budget_fraction) as usize,
            timeseries: (gl * self.timeseries_budget_fraction) as usize,
            query: (gl * self.query_budget_fraction) as usize,
        }
    }
}

/// Absolute byte budgets for each engine, derived from fractional config.
#[derive(Debug, Clone)]
pub struct EngineByteBudgets {
    pub vector: usize,
    pub sparse: usize,
    pub crdt: usize,
    pub timeseries: usize,
    pub query: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_fractions_valid() {
        let cfg = EngineConfig::default();
        cfg.validate().unwrap();
        assert!(cfg.total_fraction() <= 1.0);
    }

    #[test]
    fn over_budget_rejected() {
        let cfg = EngineConfig {
            vector_budget_fraction: 0.90,
            ..EngineConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn negative_budget_rejected() {
        let cfg = EngineConfig {
            crdt_budget_fraction: -0.1,
            ..EngineConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn byte_budgets_derived() {
        let cfg = EngineConfig::default();
        let budgets = cfg.to_byte_budgets(1024 * 1024 * 1024); // 1 GiB
        assert_eq!(budgets.vector, (1024.0 * 1024.0 * 1024.0 * 0.30) as usize);
        assert_eq!(budgets.query, (1024.0 * 1024.0 * 1024.0 * 0.20) as usize);
    }
}
