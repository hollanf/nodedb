//! Sequencer error types. A `SequencerError` is always a
//! programming bug — the sequencer never returns an error
//! for legitimate runtime reasons, so callers `?` and the
//! error propagates to startup abort.

use super::phase::StartupPhase;

/// Reasons the sequencer can reject an `advance_to` call.
#[derive(Debug, thiserror::Error)]
pub enum SequencerError {
    /// The new phase is strictly less than `current`. Always a
    /// programming bug — phases move forward, never back.
    #[error("startup phase regression: current is {current}, attempted to advance to {attempted}")]
    Regression {
        current: StartupPhase,
        attempted: StartupPhase,
    },

    /// The new phase is further than one step from `current`.
    /// The sequencer enforces strict sequential advance to
    /// surface "forgot to advance intermediate phase" bugs
    /// at the moment they happen rather than during a later
    /// snapshot.
    #[error(
        "startup phase skip: current is {current}, attempted to jump to {attempted} — \
         phases must advance sequentially"
    )]
    Skip {
        current: StartupPhase,
        attempted: StartupPhase,
    },

    /// Advanced past `GatewayEnable`. Terminal states cannot
    /// be left.
    #[error("startup phase already at terminal state {current}")]
    AlreadyTerminal { current: StartupPhase },
}

impl From<SequencerError> for crate::Error {
    fn from(e: SequencerError) -> Self {
        crate::Error::Config {
            detail: e.to_string(),
        }
    }
}
