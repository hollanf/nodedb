//! OLLP orchestrator metrics.
//!
//! Three metrics per the checklist spec:
//!
//! - `nodedb_calvin_ollp_retries_total{predicate_class, outcome}` — counter.
//!   Outcomes: `succeeded`, `retried`, `exhausted`, `circuit_open`,
//!   `tenant_budget_exceeded`.
//! - `nodedb_calvin_ollp_circuit_state{predicate_class}` — gauge,
//!   0=closed, 1=half_open, 2=open.
//! - `nodedb_calvin_ollp_backoff_ms{predicate_class}` — current delay.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// OLLP outcome label values for `nodedb_calvin_ollp_retries_total`.
pub const OUTCOME_SUCCEEDED: &str = "succeeded";
pub const OUTCOME_RETRIED: &str = "retried";
pub const OUTCOME_EXHAUSTED: &str = "exhausted";
pub const OUTCOME_CIRCUIT_OPEN: &str = "circuit_open";
pub const OUTCOME_TENANT_BUDGET_EXCEEDED: &str = "tenant_budget_exceeded";

/// Circuit state values for `nodedb_calvin_ollp_circuit_state`.
pub const CIRCUIT_CLOSED: u8 = 0;
pub const CIRCUIT_HALF_OPEN: u8 = 1;
pub const CIRCUIT_OPEN: u8 = 2;

/// Per-`(predicate_class, outcome)` retry counter entry.
#[derive(Debug, Default)]
struct RetryCount {
    succeeded: AtomicU64,
    retried: AtomicU64,
    exhausted: AtomicU64,
    circuit_open: AtomicU64,
    tenant_budget_exceeded: AtomicU64,
}

impl RetryCount {
    fn increment(&self, outcome: &str) {
        match outcome {
            OUTCOME_SUCCEEDED => {
                self.succeeded.fetch_add(1, Ordering::Relaxed);
            }
            OUTCOME_RETRIED => {
                self.retried.fetch_add(1, Ordering::Relaxed);
            }
            OUTCOME_EXHAUSTED => {
                self.exhausted.fetch_add(1, Ordering::Relaxed);
            }
            OUTCOME_CIRCUIT_OPEN => {
                self.circuit_open.fetch_add(1, Ordering::Relaxed);
            }
            OUTCOME_TENANT_BUDGET_EXCEEDED => {
                self.tenant_budget_exceeded.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

/// Per-predicate-class state snapshot for Prometheus rendering.
#[derive(Debug, Default)]
struct PredicateClassState {
    retries: RetryCount,
    /// Latest circuit state: 0=closed, 1=half_open, 2=open.
    circuit_state: AtomicU64,
    /// Latest backoff in milliseconds.
    backoff_ms: AtomicU64,
}

/// Metrics handle for the OLLP orchestrator.
///
/// Backed by `AtomicU64` counters keyed by predicate class. A `Mutex<BTreeMap>`
/// provides deterministic iteration order for Prometheus rendering.
#[derive(Debug, Default)]
pub struct OllpMetrics {
    /// Per-predicate-class state. `BTreeMap` for deterministic iteration order.
    state: Mutex<BTreeMap<u64, PredicateClassState>>,
}

impl OllpMetrics {
    /// Record a retry outcome for `predicate_class`.
    pub fn record_retry_outcome(&self, predicate_class: u64, outcome: &str) {
        tracing::debug!(
            predicate_class = predicate_class,
            outcome = outcome,
            "nodedb_calvin_ollp_retries_total"
        );
        let mut map = self.state.lock().unwrap_or_else(|p| p.into_inner());
        map.entry(predicate_class)
            .or_default()
            .retries
            .increment(outcome);
    }

    /// Record the current circuit state for `predicate_class`.
    pub fn record_circuit_state(&self, predicate_class: u64, state: u8) {
        tracing::debug!(
            predicate_class = predicate_class,
            state = state,
            "nodedb_calvin_ollp_circuit_state"
        );
        let mut map = self.state.lock().unwrap_or_else(|p| p.into_inner());
        map.entry(predicate_class)
            .or_default()
            .circuit_state
            .store(u64::from(state), Ordering::Relaxed);
    }

    /// Record the current backoff duration in milliseconds.
    pub fn record_backoff_ms(&self, predicate_class: u64, backoff_ms: u64) {
        tracing::debug!(
            predicate_class = predicate_class,
            backoff_ms = backoff_ms,
            "nodedb_calvin_ollp_backoff_ms"
        );
        let mut map = self.state.lock().unwrap_or_else(|p| p.into_inner());
        map.entry(predicate_class)
            .or_default()
            .backoff_ms
            .store(backoff_ms, Ordering::Relaxed);
    }

    /// Emit all OLLP metrics in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_ollp_retries_total \
             OLLP retry outcomes by predicate class."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_ollp_retries_total counter");

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_ollp_circuit_state \
             OLLP circuit-breaker state per predicate class (0=closed,1=half_open,2=open)."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_ollp_circuit_state gauge");

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_ollp_backoff_ms \
             Current OLLP adaptive backoff delay in milliseconds."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_ollp_backoff_ms gauge");

        let map = self.state.lock().unwrap_or_else(|p| p.into_inner());

        if map.is_empty() {
            // Emit zero-value placeholder lines so scrapers see the metric name.
            let _ = writeln!(
                out,
                "nodedb_calvin_ollp_retries_total\
                 {{predicate_class=\"0\",outcome=\"succeeded\"}} 0"
            );
            let _ = writeln!(
                out,
                "nodedb_calvin_ollp_circuit_state{{predicate_class=\"0\"}} 0"
            );
            let _ = writeln!(
                out,
                "nodedb_calvin_ollp_backoff_ms{{predicate_class=\"0\"}} 0"
            );
        } else {
            for (&pc, state) in map.iter() {
                for outcome in &[
                    OUTCOME_SUCCEEDED,
                    OUTCOME_RETRIED,
                    OUTCOME_EXHAUSTED,
                    OUTCOME_CIRCUIT_OPEN,
                    OUTCOME_TENANT_BUDGET_EXCEEDED,
                ] {
                    let value = match *outcome {
                        OUTCOME_SUCCEEDED => state.retries.succeeded.load(Ordering::Relaxed),
                        OUTCOME_RETRIED => state.retries.retried.load(Ordering::Relaxed),
                        OUTCOME_EXHAUSTED => state.retries.exhausted.load(Ordering::Relaxed),
                        OUTCOME_CIRCUIT_OPEN => state.retries.circuit_open.load(Ordering::Relaxed),
                        OUTCOME_TENANT_BUDGET_EXCEEDED => {
                            state.retries.tenant_budget_exceeded.load(Ordering::Relaxed)
                        }
                        _ => 0,
                    };
                    let _ = writeln!(
                        out,
                        "nodedb_calvin_ollp_retries_total\
                         {{predicate_class=\"{pc}\",outcome=\"{outcome}\"}} {value}"
                    );
                }
                let _ = writeln!(
                    out,
                    "nodedb_calvin_ollp_circuit_state{{predicate_class=\"{pc}\"}} {}",
                    state.circuit_state.load(Ordering::Relaxed)
                );
                let _ = writeln!(
                    out,
                    "nodedb_calvin_ollp_backoff_ms{{predicate_class=\"{pc}\"}} {}",
                    state.backoff_ms.load(Ordering::Relaxed)
                );
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_render_retries() {
        let m = OllpMetrics::default();
        m.record_retry_outcome(42, OUTCOME_SUCCEEDED);
        m.record_retry_outcome(42, OUTCOME_RETRIED);
        m.record_circuit_state(42, CIRCUIT_OPEN);
        m.record_backoff_ms(42, 100);

        let output = m.render_prometheus();
        assert!(
            output.contains("predicate_class=\"42\",outcome=\"succeeded\"} 1"),
            "output: {output}"
        );
        assert!(
            output.contains("predicate_class=\"42\",outcome=\"retried\"} 1"),
            "output: {output}"
        );
        assert!(
            output.contains("nodedb_calvin_ollp_circuit_state{predicate_class=\"42\"} 2"),
            "output: {output}"
        );
        assert!(
            output.contains("nodedb_calvin_ollp_backoff_ms{predicate_class=\"42\"} 100"),
            "output: {output}"
        );
    }

    #[test]
    fn render_uses_nodedb_prefix() {
        let m = OllpMetrics::default();
        let output = m.render_prometheus();
        for line in output.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            assert!(
                line.starts_with("nodedb_"),
                "metric line missing nodedb_ prefix: {line}"
            );
        }
    }
}
