//! Bitemporal audit-retention scheduler tuning.
//!
//! Controls the cadence of the background enforcement loop that dispatches
//! `MetaOp::TemporalPurge*` for collections registered with a bitemporal
//! audit-retain window.

use std::time::Duration;

use serde::{Deserialize, Serialize};

fn default_tick_interval_secs() -> u64 {
    // One hour mirrors the timeseries retention loop default. Purge is
    // always idempotent, so a rare tick is fine for bounded audit
    // history; operators running tighter retention windows can lower
    // this to match their SLA.
    3_600
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitemporalTuning {
    /// How often the bitemporal audit-retention enforcement loop
    /// ticks. Each tick iterates the full registry; a tick finds
    /// nothing to do when the registry is empty or every entry's
    /// `audit_retain_ms` is zero (retain forever), so tightening this
    /// is cheap.
    #[serde(default = "default_tick_interval_secs")]
    pub tick_interval_secs: u64,
}

impl BitemporalTuning {
    pub fn tick_interval(&self) -> Duration {
        Duration::from_secs(self.tick_interval_secs)
    }
}

impl Default for BitemporalTuning {
    fn default() -> Self {
        Self {
            tick_interval_secs: default_tick_interval_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_one_hour() {
        assert_eq!(BitemporalTuning::default().tick_interval_secs, 3600);
        assert_eq!(
            BitemporalTuning::default().tick_interval(),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn override_via_toml() {
        let t: BitemporalTuning = toml::from_str("tick_interval_secs = 60").unwrap();
        assert_eq!(t.tick_interval_secs, 60);
    }
}
