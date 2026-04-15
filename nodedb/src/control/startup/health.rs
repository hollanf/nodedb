//! Shared health-state formatter consumed by HTTP `/healthz` and the
//! native `STATUS` command.
//!
//! Both endpoints read from [`StartupGate`] — no separate health channel
//! is needed.

use std::sync::Arc;

use super::error::StartupError;
use super::gate::StartupGate;
use super::phase::StartupPhase;

// ---------------------------------------------------------------------------
// HealthState
// ---------------------------------------------------------------------------

/// Instantaneous health of the startup sequencer.
#[derive(Debug, Clone)]
pub enum HealthState {
    /// Still advancing through startup phases.
    Starting { phase: StartupPhase },
    /// Node has reached [`StartupPhase::GatewayEnable`] and is serving.
    Ok,
    /// Startup failed; includes the original error.
    Failed { error: Arc<StartupError> },
}

/// Read the current health from `gate`.
pub fn observe(gate: &StartupGate) -> HealthState {
    if let Some(err) = gate.is_failed() {
        return HealthState::Failed { error: err };
    }
    let phase = gate.current_phase();
    if phase >= StartupPhase::GatewayEnable {
        HealthState::Ok
    } else {
        HealthState::Starting { phase }
    }
}

// ---------------------------------------------------------------------------
// HTTP formatter
// ---------------------------------------------------------------------------

/// HTTP status code and JSON body for the given health state.
///
/// - `200 OK`                  when [`HealthState::Ok`]
/// - `503 Service Unavailable` when starting or failed
pub fn to_http_response(state: &HealthState) -> (axum::http::StatusCode, serde_json::Value) {
    use axum::http::StatusCode;
    match state {
        HealthState::Ok => (
            StatusCode::OK,
            serde_json::json!({
                "status": "ok",
                "phase": StartupPhase::GatewayEnable.name(),
            }),
        ),
        HealthState::Starting { phase } => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "status": "starting",
                "phase": phase.name(),
            }),
        ),
        HealthState::Failed { error } => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "status": "failed",
                "error": error.to_string(),
            }),
        ),
    }
}

// ---------------------------------------------------------------------------
// Native protocol formatter
// ---------------------------------------------------------------------------

/// Native protocol status for the given health state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeStatus {
    Starting,
    Ok,
    Failed,
}

/// Convert a [`HealthState`] to a [`NativeStatus`].
pub fn to_native_status(state: &HealthState) -> NativeStatus {
    match state {
        HealthState::Ok => NativeStatus::Ok,
        HealthState::Starting { .. } => NativeStatus::Starting,
        HealthState::Failed { .. } => NativeStatus::Failed,
    }
}

impl std::fmt::Display for NativeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => f.write_str("OK"),
            Self::Starting => f.write_str("Starting"),
            Self::Failed => f.write_str("Failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::startup::StartupSequencer;

    #[test]
    fn observe_starting_before_gateway_enable() {
        // A pre-fired gate (used by test helpers) reports Ok immediately.
        let gate = StartupGate::pre_fired();
        let state = observe(&gate);
        assert!(matches!(state, HealthState::Ok));

        // With a pending gate the sequencer stays at Boot — reports Starting.
        let (seq3, gate3) = StartupSequencer::new();
        let _g = seq3.register_gate(StartupPhase::WalRecovery, "test-subsystem");
        let state = observe(&gate3);
        assert!(matches!(state, HealthState::Starting { .. }));
    }

    #[test]
    fn observe_failed_returns_failed_state() {
        let (seq, gate) = StartupSequencer::new();
        seq.fail(StartupError::SubsystemFailed {
            phase: StartupPhase::WalRecovery,
            subsystem: "test".into(),
            reason: "injected failure".into(),
        });
        let state = observe(&gate);
        assert!(matches!(state, HealthState::Failed { .. }));
    }

    #[test]
    fn to_http_response_503_when_starting() {
        let (seq, gate) = StartupSequencer::new();
        let _g = seq.register_gate(StartupPhase::WalRecovery, "test");
        let state = observe(&gate);
        let (code, body) = to_http_response(&state);
        assert_eq!(code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "starting");
    }

    #[test]
    fn to_http_response_200_when_ready() {
        let gate = StartupGate::pre_fired();
        let state = observe(&gate);
        let (code, _body) = to_http_response(&state);
        assert_eq!(code, axum::http::StatusCode::OK);
    }

    #[test]
    fn native_status_display() {
        assert_eq!(NativeStatus::Ok.to_string(), "OK");
        assert_eq!(NativeStatus::Starting.to_string(), "Starting");
        assert_eq!(NativeStatus::Failed.to_string(), "Failed");
    }
}
