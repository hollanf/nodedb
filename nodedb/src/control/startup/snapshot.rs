//! Observational snapshot of the startup sequencer state.
//!
//! Consumed by `/health` and `/metrics` to render "where is
//! this node in its startup pipeline and how long has each
//! phase taken". Split from `sequencer.rs` so format impls
//! can grow without crossing file-size limits on the hot
//! path.

use std::fmt;
use std::time::{Duration, Instant};

use super::phase::StartupPhase;

/// Startup snapshot — the current phase plus the full
/// transition log up to now.
#[derive(Debug, Clone)]
pub struct StartupStatus {
    /// Phase the sequencer is currently in.
    pub current: StartupPhase,
    /// Every transition recorded so far, in chronological
    /// order. The entry for `current` has `dwell = None`
    /// because the phase hasn't ended yet.
    pub transitions: Vec<PhaseEntry>,
    /// Wall-clock elapsed since the sequencer was constructed.
    pub total_elapsed: Duration,
}

impl StartupStatus {
    /// Whether the sequencer has reached `GatewayEnable`.
    pub fn is_ready(&self) -> bool {
        self.current >= StartupPhase::GatewayEnable
    }

    /// Whether the sequencer has transitioned to `Failed`.
    pub fn is_failed(&self) -> bool {
        self.current == StartupPhase::Failed
    }

    /// Dwell time for `phase`, if it was recorded and has
    /// ended. Returns `None` for the current phase (still
    /// ticking) or a phase that was never reached.
    pub fn dwell_of(&self, phase: StartupPhase) -> Option<Duration> {
        self.transitions
            .iter()
            .find(|e| e.phase == phase)
            .and_then(|e| e.dwell)
    }
}

impl fmt::Display for StartupStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "startup: phase={} total={:?} transitions={}",
            self.current,
            self.total_elapsed,
            self.transitions.len()
        )
    }
}

/// Single entry in the transition log.
#[derive(Debug, Clone)]
pub struct PhaseEntry {
    pub phase: StartupPhase,
    pub reached_at: Instant,
    /// Time spent in this phase — `None` if this is the
    /// currently-active phase. Always `Some` for every phase
    /// older than `current`.
    pub dwell: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(phase: StartupPhase, dwell: Option<Duration>) -> PhaseEntry {
        PhaseEntry {
            phase,
            reached_at: Instant::now(),
            dwell,
        }
    }

    #[test]
    fn is_ready_true_at_gateway_enable() {
        let s = StartupStatus {
            current: StartupPhase::GatewayEnable,
            transitions: vec![],
            total_elapsed: Duration::from_secs(1),
        };
        assert!(s.is_ready());
        assert!(!s.is_failed());
    }

    #[test]
    fn is_failed_only_on_failed() {
        let s = StartupStatus {
            current: StartupPhase::Failed,
            transitions: vec![],
            total_elapsed: Duration::ZERO,
        };
        assert!(s.is_failed());
    }

    #[test]
    fn dwell_of_returns_recorded_duration() {
        let d = Duration::from_millis(42);
        let s = StartupStatus {
            current: StartupPhase::ClusterCatalogOpen,
            transitions: vec![
                entry(StartupPhase::Boot, Some(Duration::from_millis(5))),
                entry(StartupPhase::WalRecovery, Some(d)),
                entry(StartupPhase::ClusterCatalogOpen, None),
            ],
            total_elapsed: Duration::from_millis(100),
        };
        assert_eq!(s.dwell_of(StartupPhase::WalRecovery), Some(d));
        assert_eq!(s.dwell_of(StartupPhase::ClusterCatalogOpen), None);
        assert_eq!(s.dwell_of(StartupPhase::GatewayEnable), None);
    }

    #[test]
    fn display_includes_phase_name() {
        let s = StartupStatus {
            current: StartupPhase::WalRecovery,
            transitions: vec![],
            total_elapsed: Duration::from_millis(7),
        };
        let out = s.to_string();
        assert!(out.contains("wal_recovery"));
    }
}
