//! Shutdown tuning — deadlines applied to the graceful shutdown
//! path by `main.rs` and `control::shutdown::LoopRegistry`.

use serde::{Deserialize, Serialize};

fn default_shutdown_deadline_ms() -> u64 {
    // 900ms: the remaining 100ms of a 1s operator budget is
    // reserved for transport close + final WAL fsync. Chosen
    // so `shutdown_all` has a chance to abort async laggards
    // before the OS hard-kills the process on a second SIGTERM.
    900
}

/// Tuning knobs for the graceful shutdown path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownTuning {
    /// Deadline in milliseconds for `LoopRegistry::shutdown_all`.
    /// Every registered background loop must exit within this
    /// window after the shutdown signal is flipped; laggards
    /// are aborted (async) or logged (blocking).
    #[serde(default = "default_shutdown_deadline_ms")]
    pub deadline_ms: u64,
}

impl Default for ShutdownTuning {
    fn default() -> Self {
        Self {
            deadline_ms: default_shutdown_deadline_ms(),
        }
    }
}

impl ShutdownTuning {
    /// Convert the deadline to a `std::time::Duration`.
    pub fn deadline(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.deadline_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_deadline_is_900ms() {
        let t = ShutdownTuning::default();
        assert_eq!(t.deadline_ms, 900);
        assert_eq!(t.deadline(), std::time::Duration::from_millis(900));
    }

    #[test]
    fn override_deadline() {
        let toml_str = r#"deadline_ms = 1500"#;
        let t: ShutdownTuning = toml::from_str(toml_str).unwrap();
        assert_eq!(t.deadline_ms, 1500);
    }
}
