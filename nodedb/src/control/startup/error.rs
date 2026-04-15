//! Startup error types for the gate-based [`StartupSequencer`].
//!
//! [`StartupError`] is the runtime error produced when a subsystem fails,
//! times out, or its [`ReadyGate`] is dropped without being fired.
//!
//! [`StartupSequencer`]: super::startup_sequencer::StartupSequencer
//! [`ReadyGate`]: super::gate::ReadyGate

use super::phase::StartupPhase;

/// Runtime errors raised by the gate-based [`StartupSequencer`].
///
/// Every variant carries enough context for operators to identify the
/// failing subsystem and the phase it failed in without reading source
/// code.
///
/// [`StartupSequencer`]: super::startup_sequencer::StartupSequencer
#[derive(Debug, Clone, thiserror::Error)]
pub enum StartupError {
    /// A registered subsystem reported a failure while the sequencer
    /// was in `phase`. Startup is aborted; the node exits non-zero.
    #[error("subsystem '{subsystem}' failed during {phase:?}: {reason}")]
    SubsystemFailed {
        /// Phase the sequencer was in when the failure was reported.
        phase: StartupPhase,
        /// Human-readable name of the failing subsystem (e.g. `"raft"`,
        /// `"catalog-hydration"`).
        subsystem: String,
        /// Diagnostic message from the subsystem.
        reason: String,
    },

    /// A phase gate was dropped without ever being fired. This is a
    /// programming bug — a subsystem panicked or returned early without
    /// signaling readiness, which would otherwise deadlock startup
    /// forever. The drop implementation converts the silent hang into a
    /// loud failure.
    #[error(
        "ReadyGate for subsystem '{subsystem}' at {phase:?} was dropped without firing — \
         startup would have deadlocked"
    )]
    GateDroppedWithoutFire {
        /// Phase the unfired gate was registered for.
        phase: StartupPhase,
        /// Subsystem name supplied at registration time.
        subsystem: String,
    },

    /// The [`StartupSequencer`] has already entered a terminal state
    /// (either `GatewayEnable` success or a prior `Failed` transition).
    ///
    /// [`StartupSequencer`]: super::startup_sequencer::StartupSequencer
    #[error("startup sequencer already terminated")]
    AlreadyTerminated,
}

impl From<StartupError> for crate::Error {
    fn from(e: StartupError) -> Self {
        crate::Error::Config {
            detail: e.to_string(),
        }
    }
}
