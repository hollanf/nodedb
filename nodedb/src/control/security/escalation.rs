//! Auto-escalation: repeated violations → suspend → ban.
//!
//! Tracks violation counts per user. When thresholds are crossed,
//! automatically escalates the user's account status.
//!
//! Configurable thresholds:
//! - `suspend_after_violations`: N violations → auto-suspend (default: 10)
//! - `ban_after_suspensions`: N suspensions → auto-ban (default: 3)

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use tracing::info;

use super::auth_context::AuthStatus;

/// Escalation configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationConfig {
    /// Number of violations before auto-suspend. 0 = disabled.
    #[serde(default = "default_suspend_threshold")]
    pub suspend_after_violations: u32,
    /// Number of suspensions before auto-ban. 0 = disabled.
    #[serde(default = "default_ban_threshold")]
    pub ban_after_suspensions: u32,
    /// Window in seconds for counting violations (rolling). 0 = lifetime.
    #[serde(default = "default_window")]
    pub violation_window_secs: u64,
}

fn default_suspend_threshold() -> u32 {
    10
}
fn default_ban_threshold() -> u32 {
    3
}
fn default_window() -> u64 {
    3600
}

impl Default for EscalationConfig {
    fn default() -> Self {
        Self {
            suspend_after_violations: default_suspend_threshold(),
            ban_after_suspensions: default_ban_threshold(),
            violation_window_secs: default_window(),
        }
    }
}

/// Per-user violation tracker.
struct ViolationTracker {
    /// Timestamps of recent violations (for rolling window).
    violations: Vec<u64>,
    /// Total number of times this user has been suspended.
    suspension_count: u32,
}

/// Auto-escalation engine.
pub struct EscalationEngine {
    config: EscalationConfig,
    trackers: RwLock<HashMap<String, ViolationTracker>>,
}

impl EscalationEngine {
    pub fn new(config: EscalationConfig) -> Self {
        Self {
            config,
            trackers: RwLock::new(HashMap::new()),
        }
    }

    /// Record a violation for a user (rate limit hit, auth failure, etc.).
    ///
    /// Returns the new status if escalation occurred, or `None` if no change.
    pub fn record_violation(&self, user_id: &str) -> Option<AuthStatus> {
        if self.config.suspend_after_violations == 0 {
            return None; // Disabled.
        }

        let now = now_secs();
        let window_start = if self.config.violation_window_secs > 0 {
            now.saturating_sub(self.config.violation_window_secs)
        } else {
            0
        };

        let mut trackers = self.trackers.write().unwrap_or_else(|p| p.into_inner());
        let tracker = trackers
            .entry(user_id.to_string())
            .or_insert(ViolationTracker {
                violations: Vec::new(),
                suspension_count: 0,
            });

        // Prune old violations outside the window.
        if window_start > 0 {
            tracker.violations.retain(|&ts| ts >= window_start);
        }

        tracker.violations.push(now);

        // Check suspension threshold.
        if tracker.violations.len() as u32 >= self.config.suspend_after_violations {
            tracker.violations.clear(); // Reset after escalation.
            tracker.suspension_count += 1;

            // Check ban threshold.
            if self.config.ban_after_suspensions > 0
                && tracker.suspension_count >= self.config.ban_after_suspensions
            {
                info!(user_id = %user_id, suspensions = tracker.suspension_count, "auto-ban triggered");
                return Some(AuthStatus::Banned);
            }

            info!(
                user_id = %user_id,
                violations = self.config.suspend_after_violations,
                "auto-suspend triggered"
            );
            return Some(AuthStatus::Suspended);
        }

        None
    }

    /// Get current violation count for a user.
    pub fn violation_count(&self, user_id: &str) -> u32 {
        let trackers = self.trackers.read().unwrap_or_else(|p| p.into_inner());
        trackers
            .get(user_id)
            .map(|t| t.violations.len() as u32)
            .unwrap_or(0)
    }

    /// Reset violations for a user (e.g., after admin review).
    pub fn reset(&self, user_id: &str) {
        let mut trackers = self.trackers.write().unwrap_or_else(|p| p.into_inner());
        trackers.remove(user_id);
    }
}

impl Default for EscalationEngine {
    fn default() -> Self {
        Self::new(EscalationConfig::default())
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_escalation_below_threshold() {
        let engine = EscalationEngine::new(EscalationConfig {
            suspend_after_violations: 5,
            ban_after_suspensions: 3,
            violation_window_secs: 3600,
        });

        for _ in 0..4 {
            assert!(engine.record_violation("u1").is_none());
        }
    }

    #[test]
    fn auto_suspend_at_threshold() {
        let engine = EscalationEngine::new(EscalationConfig {
            suspend_after_violations: 3,
            ban_after_suspensions: 3,
            violation_window_secs: 0, // Lifetime.
        });

        assert!(engine.record_violation("u1").is_none());
        assert!(engine.record_violation("u1").is_none());
        assert_eq!(engine.record_violation("u1"), Some(AuthStatus::Suspended));
    }

    #[test]
    fn auto_ban_after_repeated_suspensions() {
        let engine = EscalationEngine::new(EscalationConfig {
            suspend_after_violations: 2,
            ban_after_suspensions: 2,
            violation_window_secs: 0,
        });

        // First suspension.
        engine.record_violation("u1");
        assert_eq!(engine.record_violation("u1"), Some(AuthStatus::Suspended));

        // Second suspension → ban.
        engine.record_violation("u1");
        assert_eq!(engine.record_violation("u1"), Some(AuthStatus::Banned));
    }

    #[test]
    fn disabled_when_zero() {
        let engine = EscalationEngine::new(EscalationConfig {
            suspend_after_violations: 0,
            ..Default::default()
        });

        for _ in 0..100 {
            assert!(engine.record_violation("u1").is_none());
        }
    }

    #[test]
    fn reset_clears_violations() {
        let engine = EscalationEngine::new(EscalationConfig {
            suspend_after_violations: 3,
            ..Default::default()
        });

        engine.record_violation("u1");
        engine.record_violation("u1");
        assert_eq!(engine.violation_count("u1"), 2);

        engine.reset("u1");
        assert_eq!(engine.violation_count("u1"), 0);
    }
}
