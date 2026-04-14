//! The startup sequencer — a single shared `Arc<Sequencer>`
//! held on `SharedState`. Writers call [`advance_to`] at each
//! phase boundary; readers call [`await_phase`] to block
//! until a target phase has been reached.
//!
//! Transitions are logged at `info!` with the elapsed time
//! since the previous phase, so a slow bootstrap is visible
//! in the startup log without extra instrumentation.
//!
//! [`advance_to`]: Sequencer::advance_to
//! [`await_phase`]: Sequencer::await_phase

use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use super::error::SequencerError;
use super::phase::StartupPhase;
use super::snapshot::{PhaseEntry, StartupStatus};

/// Recorded phase transition for snapshot reporting.
#[derive(Debug, Clone)]
struct Transition {
    phase: StartupPhase,
    reached_at: Instant,
}

#[derive(Debug)]
pub struct Sequencer {
    /// Current phase, encoded as `u8` for atomic CAS.
    current: AtomicU8,
    /// Watch channel used by `await_phase` subscribers.
    /// Written on every `advance_to`.
    tx: watch::Sender<StartupPhase>,
    /// Wall-clock of construction, for `total_elapsed` in
    /// snapshots.
    start: Instant,
    /// Chronological transition log. Writer = `advance_to`,
    /// reader = `snapshot()`. Rare enough (11 entries max)
    /// that a Mutex is fine.
    transitions: Mutex<Vec<Transition>>,
}

impl Sequencer {
    /// Create a fresh sequencer at `StartupPhase::Boot`.
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(StartupPhase::Boot);
        let now = Instant::now();
        Self {
            current: AtomicU8::new(StartupPhase::Boot.as_u8()),
            tx,
            start: now,
            transitions: Mutex::new(vec![Transition {
                phase: StartupPhase::Boot,
                reached_at: now,
            }]),
        }
    }

    /// Current phase. Atomic, cheap.
    pub fn current(&self) -> StartupPhase {
        StartupPhase::from_u8(self.current.load(Ordering::Acquire)).unwrap_or(StartupPhase::Boot)
    }

    /// Advance the sequencer to `target`. Rejects regressions,
    /// skips, and advances from terminal states.
    ///
    /// On success, `info!` logs the phase name and the
    /// elapsed time since the previous advance.
    pub fn advance_to(&self, target: StartupPhase) -> Result<(), SequencerError> {
        let current = self.current();
        if target == current {
            // Idempotent — calling `advance_to` with the
            // already-current phase is a no-op, not an
            // error. This keeps `main.rs` simpler in the
            // conditional phase-advance paths.
            return Ok(());
        }
        if matches!(current, StartupPhase::GatewayEnable | StartupPhase::Failed) {
            return Err(SequencerError::AlreadyTerminal { current });
        }
        if target < current {
            return Err(SequencerError::Regression {
                current,
                attempted: target,
            });
        }
        // Strict sequential advance: only the immediate next
        // phase is allowed. `Failed` is an exception — any
        // phase may jump directly to Failed via `fail()`.
        let expected_next = current.next();
        if expected_next != Some(target) {
            return Err(SequencerError::Skip {
                current,
                attempted: target,
            });
        }

        let reached_at = Instant::now();
        self.current.store(target.as_u8(), Ordering::Release);
        self.tx.send_replace(target);

        let dwell = {
            let mut guard = lock_transitions(&self.transitions);
            let prev = guard
                .last()
                .map(|t| reached_at.duration_since(t.reached_at))
                .unwrap_or_default();
            guard.push(Transition {
                phase: target,
                reached_at,
            });
            prev
        };

        tracing::info!(
            phase = target.name(),
            dwell_prev = ?dwell,
            total = ?reached_at.duration_since(self.start),
            "startup phase advanced"
        );
        Ok(())
    }

    /// Transition directly to the `Failed` terminal state
    /// from any non-terminal phase. Used by the startup
    /// driver when an unrecoverable error is reported during
    /// bootstrap.
    ///
    /// After `fail()`, every `await_phase` call returns
    /// immediately (because `Failed > GatewayEnable`) and the
    /// gateway guard rejects new client connections.
    pub fn fail(&self) {
        let current = self.current();
        if matches!(current, StartupPhase::GatewayEnable | StartupPhase::Failed) {
            // GatewayEnable is already serving; failing at
            // that point would be a lie. Failed is idempotent.
            return;
        }
        let reached_at = Instant::now();
        self.current
            .store(StartupPhase::Failed.as_u8(), Ordering::Release);
        self.tx.send_replace(StartupPhase::Failed);
        {
            let mut guard = lock_transitions(&self.transitions);
            guard.push(Transition {
                phase: StartupPhase::Failed,
                reached_at,
            });
        }
        tracing::error!(
            previous = current.name(),
            total = ?reached_at.duration_since(self.start),
            "startup aborted — sequencer transitioned to Failed"
        );
    }

    /// Resolves once the sequencer reaches `target` or a
    /// later phase. Fast path: if `current >= target` at the
    /// first check, returns immediately.
    ///
    /// Cancel-safe: dropping the future in a `select!`
    /// losing arm does not miss a subsequent advance because
    /// the underlying `watch::Receiver::changed` is cancel-safe
    /// and the state is re-checked on every wake.
    pub async fn await_phase(&self, target: StartupPhase) {
        if self.current() >= target {
            return;
        }
        let mut rx = self.tx.subscribe();
        loop {
            if *rx.borrow() >= target {
                return;
            }
            if rx.changed().await.is_err() {
                // Every sender dropped — nothing will ever
                // advance the phase again. Break rather than
                // park forever.
                return;
            }
        }
    }

    /// Observational snapshot for `/health`, metrics, and
    /// tests. Cheap — one mutex acquisition, bounded-size
    /// vector clone.
    pub fn snapshot(&self) -> StartupStatus {
        let guard = lock_transitions(&self.transitions);
        let current = self.current();
        let now = Instant::now();
        let mut entries: Vec<PhaseEntry> = Vec::with_capacity(guard.len());
        for i in 0..guard.len() {
            let t = &guard[i];
            let dwell = match guard.get(i + 1) {
                Some(next) => Some(next.reached_at.duration_since(t.reached_at)),
                None if t.phase == current => None, // still in this phase
                None => Some(now.duration_since(t.reached_at)),
            };
            entries.push(PhaseEntry {
                phase: t.phase,
                reached_at: t.reached_at,
                dwell,
            });
        }
        StartupStatus {
            current,
            transitions: entries,
            total_elapsed: now.duration_since(self.start),
        }
    }

    /// Wall-clock elapsed since the sequencer was constructed.
    /// Useful for comparing phase dwell to total boot time.
    pub fn total_elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for Sequencer {
    fn default() -> Self {
        Self::new()
    }
}

fn lock_transitions<'a>(
    mu: &'a Mutex<Vec<Transition>>,
) -> std::sync::MutexGuard<'a, Vec<Transition>> {
    match mu.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!(
                "startup Sequencer transitions mutex poisoned — a previous holder \
                 panicked. Recovering the guard so startup can still produce a \
                 snapshot, but this is a bug."
            );
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn full_chain() -> Vec<StartupPhase> {
        let mut chain = vec![StartupPhase::Boot];
        let mut cur = StartupPhase::Boot;
        while let Some(next) = cur.next() {
            chain.push(next);
            cur = next;
        }
        chain
    }

    #[test]
    fn starts_at_boot() {
        let s = Sequencer::new();
        assert_eq!(s.current(), StartupPhase::Boot);
    }

    #[test]
    fn monotonic_advance_to_gateway() {
        let s = Sequencer::new();
        for phase in full_chain().into_iter().skip(1) {
            s.advance_to(phase).expect("advance");
            assert_eq!(s.current(), phase);
        }
        assert_eq!(s.current(), StartupPhase::GatewayEnable);
    }

    #[test]
    fn regression_rejected() {
        let s = Sequencer::new();
        s.advance_to(StartupPhase::WalRecovery).unwrap();
        s.advance_to(StartupPhase::ClusterCatalogOpen).unwrap();
        let err = s.advance_to(StartupPhase::WalRecovery).unwrap_err();
        assert!(matches!(err, SequencerError::Regression { .. }));
    }

    #[test]
    fn skip_rejected() {
        let s = Sequencer::new();
        let err = s.advance_to(StartupPhase::GatewayEnable).unwrap_err();
        assert!(matches!(err, SequencerError::Skip { .. }));
    }

    #[test]
    fn idempotent_same_phase_advance() {
        let s = Sequencer::new();
        s.advance_to(StartupPhase::WalRecovery).unwrap();
        s.advance_to(StartupPhase::WalRecovery).unwrap();
        assert_eq!(s.current(), StartupPhase::WalRecovery);
    }

    #[test]
    fn terminal_state_rejects_advance() {
        // GatewayEnable is terminal: any attempt to advance
        // past it (including to Failed) is rejected as
        // AlreadyTerminal. Idempotent same-phase advance is
        // NOT an error — that path is covered elsewhere.
        let s = Sequencer::new();
        for phase in full_chain().into_iter().skip(1) {
            s.advance_to(phase).unwrap();
        }
        assert_eq!(s.current(), StartupPhase::GatewayEnable);
        let err = s.advance_to(StartupPhase::Failed).unwrap_err();
        assert!(matches!(err, SequencerError::AlreadyTerminal { .. }));

        // fail() from GatewayEnable is a no-op (already
        // serving — failing at that point would be a lie).
        s.fail();
        assert_eq!(s.current(), StartupPhase::GatewayEnable);

        // Direct fail() transitions from any non-terminal
        // phase to Failed, and further advances are rejected.
        let s2 = Sequencer::new();
        s2.advance_to(StartupPhase::WalRecovery).unwrap();
        s2.fail();
        assert_eq!(s2.current(), StartupPhase::Failed);
        let err = s2.advance_to(StartupPhase::ClusterCatalogOpen).unwrap_err();
        assert!(matches!(err, SequencerError::AlreadyTerminal { .. }));
    }

    #[tokio::test]
    async fn await_phase_returns_immediately_when_reached() {
        let s = Arc::new(Sequencer::new());
        s.advance_to(StartupPhase::WalRecovery).unwrap();
        s.advance_to(StartupPhase::ClusterCatalogOpen).unwrap();
        tokio::time::timeout(
            Duration::from_millis(10),
            s.await_phase(StartupPhase::WalRecovery),
        )
        .await
        .expect("already-reached phase blocked");
    }

    #[tokio::test]
    async fn await_phase_blocks_until_advance() {
        let s = Arc::new(Sequencer::new());
        let s2 = Arc::clone(&s);
        let handle = tokio::spawn(async move {
            s2.await_phase(StartupPhase::ClusterCatalogOpen).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());
        s.advance_to(StartupPhase::WalRecovery).unwrap();
        s.advance_to(StartupPhase::ClusterCatalogOpen).unwrap();
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("waiter did not wake")
            .expect("waiter panicked");
    }

    #[tokio::test]
    async fn concurrent_waiters_all_wake() {
        let s = Arc::new(Sequencer::new());
        let mut handles = Vec::new();
        for _ in 0..5 {
            let s2 = Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                s2.await_phase(StartupPhase::GatewayEnable).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        for p in full_chain().into_iter().skip(1) {
            s.advance_to(p).unwrap();
        }
        for h in handles {
            tokio::time::timeout(Duration::from_millis(100), h)
                .await
                .expect("waiter did not wake")
                .expect("waiter panicked");
        }
    }

    #[test]
    fn snapshot_reports_transitions() {
        let s = Sequencer::new();
        s.advance_to(StartupPhase::WalRecovery).unwrap();
        s.advance_to(StartupPhase::ClusterCatalogOpen).unwrap();
        let snap = s.snapshot();
        assert_eq!(snap.current, StartupPhase::ClusterCatalogOpen);
        assert_eq!(snap.transitions.len(), 3);
        assert_eq!(snap.transitions[0].phase, StartupPhase::Boot);
        assert_eq!(snap.transitions[1].phase, StartupPhase::WalRecovery);
        assert_eq!(snap.transitions[2].phase, StartupPhase::ClusterCatalogOpen);
        // Middle entry has `dwell = Some(...)`, current phase
        // has `None`.
        assert!(snap.transitions[1].dwell.is_some());
        assert!(snap.transitions[2].dwell.is_none());
    }

    #[tokio::test]
    async fn fail_wakes_await_phase() {
        let s = Arc::new(Sequencer::new());
        let s2 = Arc::clone(&s);
        let handle = tokio::spawn(async move {
            s2.await_phase(StartupPhase::GatewayEnable).await;
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        s.fail();
        tokio::time::timeout(Duration::from_millis(50), handle)
            .await
            .expect("waiter did not wake on fail")
            .expect("waiter panicked");
    }
}
