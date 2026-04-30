//! Per-group hysteresis state machine for alert rules.
//!
//! Prevents flapping: a brief dip below threshold doesn't clear an active alert,
//! and a brief spike above threshold doesn't fire a cleared alert.
//!
//! State machine per (alert_name, group_key):
//! - Condition true  → increment consecutive_fire, reset consecutive_recover
//! - consecutive_fire >= fire_after AND Cleared → FIRE, set Active
//! - Condition false → increment consecutive_recover, reset consecutive_fire
//! - consecutive_recover >= recover_after AND Active → CLEAR, set Cleared

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::AlertStatus;

/// Per-group alert state.
#[derive(Debug, Clone)]
pub struct AlertGroupState {
    /// Current alert status.
    pub status: AlertStatus,
    /// Consecutive evaluation windows where condition was true.
    pub consecutive_fire: u32,
    /// Consecutive evaluation windows where condition was false.
    pub consecutive_recover: u32,
    /// Timestamp (ms) when the alert last transitioned to Active.
    pub fired_at: Option<u64>,
    /// Timestamp (ms) when the alert last transitioned to Cleared.
    pub cleared_at: Option<u64>,
    /// Last evaluated aggregate value.
    pub last_value: Option<f64>,
}

impl AlertGroupState {
    fn new() -> Self {
        Self {
            status: AlertStatus::Cleared,
            consecutive_fire: 0,
            consecutive_recover: 0,
            fired_at: None,
            cleared_at: None,
            last_value: None,
        }
    }
}

/// Result of evaluating one group's condition against hysteresis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HysteresisTransition {
    /// No state change.
    NoChange,
    /// Transitioned from Cleared → Active (fire notification).
    Fired,
    /// Transitioned from Active → Cleared (recovery notification).
    Recovered,
}

/// Manages per-(alert, group) hysteresis state.
///
/// Thread-safe for access from the eval loop. State is in-memory only
/// (crash recovery: re-evaluate from data on startup, alerts re-converge
/// within fire_after windows).
pub struct HysteresisManager {
    /// Key: (tenant_id, alert_name, group_key) → state.
    states: RwLock<HashMap<(u64, String, String), AlertGroupState>>,
}

impl HysteresisManager {
    pub fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Evaluate a condition result for a group and return any state transition.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate(
        &self,
        tenant_id: u64,
        alert_name: &str,
        group_key: &str,
        condition_met: bool,
        value: f64,
        fire_after: u32,
        recover_after: u32,
        now_ms: u64,
    ) -> HysteresisTransition {
        let key = (tenant_id, alert_name.to_string(), group_key.to_string());
        let mut states = self.states.write().unwrap_or_else(|p| p.into_inner());
        let state = states.entry(key).or_insert_with(AlertGroupState::new);
        state.last_value = Some(value);

        if condition_met {
            state.consecutive_fire += 1;
            state.consecutive_recover = 0;

            if state.consecutive_fire >= fire_after && state.status == AlertStatus::Cleared {
                state.status = AlertStatus::Active;
                state.fired_at = Some(now_ms);
                return HysteresisTransition::Fired;
            }
        } else {
            state.consecutive_recover += 1;
            state.consecutive_fire = 0;

            if state.consecutive_recover >= recover_after && state.status == AlertStatus::Active {
                state.status = AlertStatus::Cleared;
                state.cleared_at = Some(now_ms);
                return HysteresisTransition::Recovered;
            }
        }

        HysteresisTransition::NoChange
    }

    /// Get the current state for a specific group.
    pub fn get_state(
        &self,
        tenant_id: u64,
        alert_name: &str,
        group_key: &str,
    ) -> Option<AlertGroupState> {
        let key = (tenant_id, alert_name.to_string(), group_key.to_string());
        self.states
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .cloned()
    }

    /// List all group states for an alert (for SHOW ALERT STATUS).
    pub fn list_states(&self, tenant_id: u64, alert_name: &str) -> Vec<(String, AlertGroupState)> {
        let prefix = (tenant_id, alert_name.to_string());
        self.states
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|((t, a, _), _)| *t == prefix.0 && a == &prefix.1)
            .map(|((_, _, g), s)| (g.clone(), s.clone()))
            .collect()
    }

    /// Remove all state for an alert (on DROP ALERT).
    pub fn remove_alert(&self, tenant_id: u64, alert_name: &str) {
        let mut states = self.states.write().unwrap_or_else(|p| p.into_inner());
        states.retain(|(t, a, _), _| !(*t == tenant_id && a == alert_name));
    }
}

impl Default for HysteresisManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_after_consecutive_windows() {
        let mgr = HysteresisManager::new();

        // fire_after = 3, need 3 consecutive true evaluations.
        let r1 = mgr.evaluate(1, "alert1", "g1", true, 91.0, 3, 2, 1000);
        assert_eq!(r1, HysteresisTransition::NoChange);

        let r2 = mgr.evaluate(1, "alert1", "g1", true, 92.0, 3, 2, 2000);
        assert_eq!(r2, HysteresisTransition::NoChange);

        let r3 = mgr.evaluate(1, "alert1", "g1", true, 93.0, 3, 2, 3000);
        assert_eq!(r3, HysteresisTransition::Fired);

        // Already Active, consecutive true should not re-fire.
        let r4 = mgr.evaluate(1, "alert1", "g1", true, 94.0, 3, 2, 4000);
        assert_eq!(r4, HysteresisTransition::NoChange);
    }

    #[test]
    fn recovers_after_consecutive_false() {
        let mgr = HysteresisManager::new();

        // Fire first.
        mgr.evaluate(1, "a", "g", true, 91.0, 1, 2, 1000);
        let _fired = mgr.evaluate(1, "a", "g", true, 92.0, 1, 2, 2000);
        // fire_after=1, so first true fires.
        assert_eq!(
            mgr.evaluate(1, "a", "g", true, 93.0, 1, 2, 1000),
            HysteresisTransition::NoChange
        );

        // Now recover: need 2 consecutive false.
        let r1 = mgr.evaluate(1, "a", "g", false, 89.0, 1, 2, 3000);
        assert_eq!(r1, HysteresisTransition::NoChange);

        let r2 = mgr.evaluate(1, "a", "g", false, 88.0, 1, 2, 4000);
        assert_eq!(r2, HysteresisTransition::Recovered);
    }

    #[test]
    fn interrupted_fire_resets() {
        let mgr = HysteresisManager::new();

        // fire_after=3: 2 true, then 1 false, then 2 true → should not fire.
        mgr.evaluate(1, "a", "g", true, 91.0, 3, 2, 1000);
        mgr.evaluate(1, "a", "g", true, 92.0, 3, 2, 2000);
        mgr.evaluate(1, "a", "g", false, 89.0, 3, 2, 3000); // resets consecutive_fire
        mgr.evaluate(1, "a", "g", true, 91.0, 3, 2, 4000);
        let r = mgr.evaluate(1, "a", "g", true, 92.0, 3, 2, 5000);
        assert_eq!(r, HysteresisTransition::NoChange); // Only 2 consecutive, not 3.
    }

    #[test]
    fn independent_groups() {
        let mgr = HysteresisManager::new();

        let r1 = mgr.evaluate(1, "a", "device-1", true, 91.0, 1, 1, 1000);
        assert_eq!(r1, HysteresisTransition::Fired);

        // Different group should be independent.
        let r2 = mgr.evaluate(1, "a", "device-2", false, 80.0, 1, 1, 1000);
        assert_eq!(r2, HysteresisTransition::NoChange); // Cleared, false → no change.

        // device-1 still Active.
        let state = mgr.get_state(1, "a", "device-1").unwrap();
        assert_eq!(state.status, AlertStatus::Active);
    }

    #[test]
    fn list_states_for_alert() {
        let mgr = HysteresisManager::new();
        mgr.evaluate(1, "a", "g1", true, 91.0, 1, 1, 1000);
        mgr.evaluate(1, "a", "g2", true, 92.0, 1, 1, 1000);
        mgr.evaluate(1, "b", "g3", true, 93.0, 1, 1, 1000);

        let states = mgr.list_states(1, "a");
        assert_eq!(states.len(), 2);
    }

    #[test]
    fn remove_alert_clears_all_groups() {
        let mgr = HysteresisManager::new();
        mgr.evaluate(1, "a", "g1", true, 91.0, 1, 1, 1000);
        mgr.evaluate(1, "a", "g2", true, 92.0, 1, 1, 1000);
        mgr.remove_alert(1, "a");
        assert!(mgr.list_states(1, "a").is_empty());
    }
}
