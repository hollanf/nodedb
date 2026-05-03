//! OLLP orchestrator configuration.

use std::time::Duration;

/// Configuration for the OLLP retry orchestrator.
///
/// Controls retry caps, adaptive backoff, circuit-breaker thresholds, and
/// per-tenant budgets.
#[derive(Debug, Clone)]
pub struct OllpConfig {
    /// Maximum number of retries per submission attempt before returning
    /// `OllpError::Exhausted`.
    ///
    /// Default: 5.
    pub ollp_max_retries: u32,

    /// Initial backoff duration after the first retry.
    ///
    /// Backoff doubles on each retry (exponential), capped at `backoff_max`.
    ///
    /// Default: 10 ms.
    pub backoff_initial: Duration,

    /// Maximum backoff duration per retry.
    ///
    /// Default: 5 s.
    pub backoff_max: Duration,

    /// Rolling window for the circuit-breaker event tracker.
    ///
    /// Default: 60 s.
    pub circuit_window: Duration,

    /// Capacity of the circuit-breaker token window (max events tracked).
    ///
    /// Default: 256.
    pub circuit_capacity: usize,

    /// Retry ratio threshold (in percent) at which the circuit trips to Open.
    ///
    /// When `retries / (retries + successes) >= circuit_threshold_pct / 100`,
    /// the circuit opens.
    ///
    /// Default: 50 (50%).
    pub circuit_threshold_pct: u8,

    /// Duration the circuit stays Open before transitioning to HalfOpen.
    ///
    /// Default: 30 s.
    pub circuit_open_duration: Duration,

    /// Number of consecutive successes needed in HalfOpen to close the circuit.
    ///
    /// Default: 4.
    pub circuit_close_successes: u32,

    /// Per-tenant retry budget in retries per minute.
    ///
    /// Default: 1000.
    pub tenant_budget_per_minute: usize,
}

impl Default for OllpConfig {
    fn default() -> Self {
        Self {
            ollp_max_retries: 5,
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_secs(5),
            circuit_window: Duration::from_secs(60),
            circuit_capacity: 256,
            circuit_threshold_pct: 50,
            circuit_open_duration: Duration::from_secs(30),
            circuit_close_successes: 4,
            tenant_budget_per_minute: 1000,
        }
    }
}
