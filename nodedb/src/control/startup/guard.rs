//! Gateway guard — the gate every client-facing listener
//! waits on before processing requests.
//!
//! Wired into each listener so that a node in the middle of
//! startup accepts TCP connections but does not proceed to
//! wire-protocol handshake until
//! [`GatewayGuard::await_ready`] returns. If shutdown fires
//! during startup, the guard short-circuits with
//! [`GatewayRefusal::ShuttingDown`] and the listener closes
//! the stream cleanly instead of hanging.

use std::sync::Arc;

use super::phase::StartupPhase;
use super::sequencer::Sequencer;
use crate::control::shutdown::ShutdownWatch;

/// Reasons the gateway guard can refuse a pending connection.
#[derive(Debug, thiserror::Error)]
pub enum GatewayRefusal {
    /// Shutdown was signaled while the listener was waiting
    /// for `GatewayEnable`. Treat as a clean close.
    #[error("gateway refusing new connections: shutdown in progress")]
    ShuttingDown,
    /// The startup sequencer transitioned to `Failed` before
    /// `GatewayEnable`. The operator must inspect the startup
    /// log; new connections are rejected to avoid serving
    /// against a half-bootstrapped node.
    #[error("gateway refusing new connections: startup failed ({detail})")]
    StartupFailed { detail: String },
}

/// Gateway guard. Cheap to clone — all state lives in two
/// `Arc`s shared with `SharedState`.
#[derive(Debug, Clone)]
pub struct GatewayGuard {
    sequencer: Arc<Sequencer>,
    shutdown: Arc<ShutdownWatch>,
}

impl GatewayGuard {
    /// Construct a guard from the canonical sequencer + watch.
    /// Usually created on-demand via
    /// `GatewayGuard::from_state(&shared)` so listeners don't
    /// need to pass both Arcs individually.
    pub fn new(sequencer: Arc<Sequencer>, shutdown: Arc<ShutdownWatch>) -> Self {
        Self {
            sequencer,
            shutdown,
        }
    }

    /// Block until the sequencer reaches `GatewayEnable`,
    /// shutdown fires, or the sequencer fails. Returns
    /// `Ok(())` on successful start, `Err(ShuttingDown)` if
    /// shutdown wins, or `Err(StartupFailed)` if the
    /// sequencer transitioned to `Failed`.
    ///
    /// Fast path: if the sequencer is already at
    /// `GatewayEnable`, returns immediately without a
    /// `select!`.
    pub async fn await_ready(&self) -> Result<(), GatewayRefusal> {
        // Fast path.
        let current = self.sequencer.current();
        if current == StartupPhase::Failed {
            return Err(GatewayRefusal::StartupFailed {
                detail: "sequencer already in Failed state".into(),
            });
        }
        if current >= StartupPhase::GatewayEnable {
            return Ok(());
        }
        if self.shutdown.is_shutdown() {
            return Err(GatewayRefusal::ShuttingDown);
        }

        // Slow path: race phase advance against shutdown.
        let mut rx = self.shutdown.subscribe();
        tokio::select! {
            () = self.sequencer.await_phase(StartupPhase::GatewayEnable) => {
                // Could be GatewayEnable *or* Failed (both
                // satisfy `>= GatewayEnable` for the inner
                // watch compare). Re-read current to decide.
                match self.sequencer.current() {
                    StartupPhase::Failed => Err(GatewayRefusal::StartupFailed {
                        detail: "sequencer transitioned to Failed during startup".into(),
                    }),
                    _ => Ok(()),
                }
            }
            _ = rx.wait_cancelled() => Err(GatewayRefusal::ShuttingDown),
        }
    }

    /// Non-blocking readiness probe. Used by `/health/ready`
    /// to return 503 until startup completes.
    pub fn is_ready(&self) -> bool {
        self.sequencer.current() >= StartupPhase::GatewayEnable
            && self.sequencer.current() != StartupPhase::Failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn advance_to_gateway(s: &Sequencer) {
        let mut cur = s.current();
        while let Some(next) = cur.next() {
            s.advance_to(next).unwrap();
            cur = next;
            if cur == StartupPhase::GatewayEnable {
                break;
            }
        }
    }

    #[tokio::test]
    async fn await_ready_unblocks_on_gateway_enable() {
        let seq = Arc::new(Sequencer::new());
        let watch = Arc::new(ShutdownWatch::new());
        let guard = GatewayGuard::new(Arc::clone(&seq), Arc::clone(&watch));

        let g2 = guard.clone();
        let handle = tokio::spawn(async move { g2.await_ready().await });
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!handle.is_finished());

        advance_to_gateway(&seq);
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("guard did not unblock on GatewayEnable")
            .expect("task panicked")
            .expect("await_ready returned error");
        assert!(guard.is_ready());
    }

    #[tokio::test]
    async fn await_ready_returns_shutting_down_on_signal() {
        let seq = Arc::new(Sequencer::new());
        let watch = Arc::new(ShutdownWatch::new());
        let guard = GatewayGuard::new(seq, Arc::clone(&watch));

        let g2 = guard.clone();
        let handle = tokio::spawn(async move { g2.await_ready().await });
        tokio::time::sleep(Duration::from_millis(5)).await;

        watch.signal();
        let result = tokio::time::timeout(Duration::from_millis(50), handle)
            .await
            .expect("guard did not react to shutdown")
            .expect("task panicked");
        assert!(matches!(result, Err(GatewayRefusal::ShuttingDown)));
    }

    #[tokio::test]
    async fn await_ready_fast_path_when_already_ready() {
        let seq = Arc::new(Sequencer::new());
        advance_to_gateway(&seq);
        let watch = Arc::new(ShutdownWatch::new());
        let guard = GatewayGuard::new(seq, watch);
        tokio::time::timeout(Duration::from_millis(5), guard.await_ready())
            .await
            .expect("fast path blocked")
            .expect("await_ready returned error on ready guard");
    }

    #[tokio::test]
    async fn await_ready_fails_when_sequencer_failed() {
        let seq = Arc::new(Sequencer::new());
        let watch = Arc::new(ShutdownWatch::new());
        let guard = GatewayGuard::new(Arc::clone(&seq), watch);

        let g2 = guard.clone();
        let handle = tokio::spawn(async move { g2.await_ready().await });
        tokio::time::sleep(Duration::from_millis(5)).await;
        seq.fail();

        let result = tokio::time::timeout(Duration::from_millis(50), handle)
            .await
            .expect("guard did not react to fail()")
            .expect("task panicked");
        assert!(matches!(result, Err(GatewayRefusal::StartupFailed { .. })));
        assert!(!guard.is_ready());
    }

    #[tokio::test]
    async fn await_ready_fast_path_when_already_failed() {
        let seq = Arc::new(Sequencer::new());
        seq.fail();
        let watch = Arc::new(ShutdownWatch::new());
        let guard = GatewayGuard::new(seq, watch);
        let result = guard.await_ready().await;
        assert!(matches!(result, Err(GatewayRefusal::StartupFailed { .. })));
    }

    #[tokio::test]
    async fn await_ready_fast_path_when_already_shutting_down() {
        let seq = Arc::new(Sequencer::new());
        let watch = Arc::new(ShutdownWatch::new());
        watch.signal();
        let guard = GatewayGuard::new(seq, watch);
        let result = guard.await_ready().await;
        assert!(matches!(result, Err(GatewayRefusal::ShuttingDown)));
    }
}
