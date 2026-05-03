//! OLLP orchestrator error types.

use thiserror::Error;

use nodedb_cluster::calvin::sequencer::error::SequencerError;

/// Errors produced by the OLLP orchestrator.
#[derive(Debug, Error)]
pub enum OllpError {
    /// Exceeded `ollp_max_retries` without a successful commit.
    #[error("OLLP retry limit exhausted after {retries} retries")]
    Exhausted { retries: u32 },

    /// The circuit-breaker for this predicate class is open.
    ///
    /// New submissions are rejected until the circuit half-opens.
    #[error("OLLP circuit open for predicate class {predicate_class:#x}; retry later")]
    CircuitOpen { predicate_class: u64 },

    /// The submitting tenant has exceeded its per-minute retry budget.
    #[error("OLLP tenant retry budget exceeded for tenant {tenant}; retry later")]
    TenantBudgetExceeded { tenant: u64 },

    /// An underlying sequencer error prevented submission.
    #[error("OLLP sequencer error: {0}")]
    Sequencer(#[from] SequencerError),
}
