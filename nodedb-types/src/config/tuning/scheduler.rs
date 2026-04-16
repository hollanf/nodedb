//! Scheduler tuning — caps on concurrent jobs, per-job wall-clock
//! budget, and outbound HTTP timeouts applied by Event-Plane emitters.

use serde::{Deserialize, Serialize};

fn default_max_concurrent_jobs() -> usize {
    32
}

fn default_job_timeout_secs() -> u64 {
    // Most scheduled bodies are retention deletes or incremental MV
    // refreshes. 300s gives ample headroom while preventing a runaway
    // job from holding `Arc<SharedState>` across the shutdown deadline.
    300
}

fn default_webhook_timeout_secs() -> u64 {
    5
}

fn default_siem_webhook_timeout_secs() -> u64 {
    10
}

fn default_otel_timeout_secs() -> u64 {
    5
}

/// Tuning knobs for the Event-Plane scheduler and outbound HTTP emitters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerTuning {
    /// Hard cap on concurrent in-flight scheduled jobs.
    /// Jobs beyond the cap at a minute-boundary are rejected rather than
    /// starving request-path Tokio workers with parallel SQL.
    #[serde(default = "default_max_concurrent_jobs")]
    pub max_concurrent_jobs: usize,

    /// Wall-clock timeout applied to each scheduled job via
    /// `ExecutionBudget`. Long-running SQL bodies that exceed this are
    /// cancelled cooperatively at the next statement boundary.
    #[serde(default = "default_job_timeout_secs")]
    pub job_timeout_secs: u64,

    /// Timeout (seconds) for a single alert-webhook POST.
    #[serde(default = "default_webhook_timeout_secs")]
    pub webhook_timeout_secs: u64,

    /// Timeout (seconds) for a SIEM-webhook POST.
    #[serde(default = "default_siem_webhook_timeout_secs")]
    pub siem_webhook_timeout_secs: u64,

    /// Timeout (seconds) for an OTLP trace-span export.
    #[serde(default = "default_otel_timeout_secs")]
    pub otel_timeout_secs: u64,
}

impl Default for SchedulerTuning {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: default_max_concurrent_jobs(),
            job_timeout_secs: default_job_timeout_secs(),
            webhook_timeout_secs: default_webhook_timeout_secs(),
            siem_webhook_timeout_secs: default_siem_webhook_timeout_secs(),
            otel_timeout_secs: default_otel_timeout_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let t = SchedulerTuning::default();
        assert_eq!(t.max_concurrent_jobs, 32);
        assert_eq!(t.job_timeout_secs, 300);
        assert_eq!(t.webhook_timeout_secs, 5);
        assert_eq!(t.siem_webhook_timeout_secs, 10);
        assert_eq!(t.otel_timeout_secs, 5);
    }

    #[test]
    fn partial_override() {
        let toml_str = r#"
max_concurrent_jobs = 8
webhook_timeout_secs = 15
"#;
        let t: SchedulerTuning = toml::from_str(toml_str).unwrap();
        assert_eq!(t.max_concurrent_jobs, 8);
        assert_eq!(t.webhook_timeout_secs, 15);
        assert_eq!(t.siem_webhook_timeout_secs, 10);
        assert_eq!(t.job_timeout_secs, 300);
    }
}
