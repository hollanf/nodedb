//! The canonical shutdown watch. One instance per `SharedState`.
//!
//! Producers signal via [`ShutdownWatch::signal`]. Consumers
//! subscribe via [`ShutdownWatch::subscribe`], which returns a
//! fresh [`ShutdownReceiver`] that reflects the current state
//! — so a late subscriber whose spawn raced the signal still
//! observes it rather than hanging forever.

use tokio::sync::watch;

use super::ShutdownReceiver;

/// Canonical shutdown signal for every background loop on this
/// node.
///
/// Cheap to clone (holds an `Arc` internally via `watch::Sender`).
/// Usually wrapped in an `Arc` on `SharedState` so shutdown can
/// be signaled from the ctrl-c handler without reaching into
/// `SharedState` with a mutable reference.
#[derive(Debug)]
pub struct ShutdownWatch {
    tx: watch::Sender<bool>,
}

impl ShutdownWatch {
    /// Construct a fresh watch in the "running" state (flag
    /// `false`). Every subscriber starts out un-signaled.
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx }
    }

    /// Subscribe a new receiver. Always reflects the current
    /// state — a receiver created after [`signal`] has been
    /// called observes the signal on its first poll of
    /// `wait_cancelled`, so there is no "signal arrived before I
    /// subscribed" race.
    ///
    /// [`signal`]: Self::signal
    pub fn subscribe(&self) -> ShutdownReceiver {
        ShutdownReceiver::from_watch(self.tx.subscribe())
    }

    /// Flip the shutdown flag. Idempotent — calling twice is a
    /// no-op, and the second call does NOT wake subscribers
    /// again (they are already awake from the first signal).
    pub fn signal(&self) {
        // `send_replace` does not fail even when every receiver
        // has been dropped — shutdown must proceed whether or
        // not anyone is listening.
        self.tx.send_replace(true);
    }

    /// Whether shutdown has been signaled. Non-blocking;
    /// intended for diagnostics and "should I even start this
    /// expensive work?" fast paths.
    pub fn is_shutdown(&self) -> bool {
        *self.tx.borrow()
    }

    /// Escape hatch for external subsystems that already take
    /// a raw `tokio::sync::watch::Receiver<bool>` in their
    /// public constructor (currently `event::webhook::WebhookManager`
    /// and `event::kafka::KafkaManager`). New code MUST use
    /// [`subscribe`] instead.
    ///
    /// [`subscribe`]: Self::subscribe
    pub fn raw_receiver(&self) -> watch::Receiver<bool> {
        self.tx.subscribe()
    }
}

impl Default for ShutdownWatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn signal_wakes_all_subscribers() {
        let watch = ShutdownWatch::new();
        let mut r1 = watch.subscribe();
        let mut r2 = watch.subscribe();
        let mut r3 = watch.subscribe();

        watch.signal();

        // Every receiver must observe the signal within a tight
        // timeout — the watch is already flagged, so
        // `wait_cancelled` should return immediately.
        for r in [&mut r1, &mut r2, &mut r3] {
            tokio::time::timeout(Duration::from_millis(50), r.wait_cancelled())
                .await
                .expect("subscriber did not observe signal");
        }
    }

    #[tokio::test]
    async fn late_subscriber_sees_already_signaled() {
        let watch = ShutdownWatch::new();
        watch.signal();

        let mut late = watch.subscribe();
        tokio::time::timeout(Duration::from_millis(50), late.wait_cancelled())
            .await
            .expect("late subscriber did not observe prior signal");
    }

    #[tokio::test]
    async fn signal_is_idempotent() {
        let watch = ShutdownWatch::new();
        watch.signal();
        watch.signal();
        watch.signal();

        let mut r = watch.subscribe();
        tokio::time::timeout(Duration::from_millis(50), r.wait_cancelled())
            .await
            .expect("triple-signaled watch did not fire");
    }

    #[test]
    fn is_shutdown_reflects_state() {
        let watch = ShutdownWatch::new();
        assert!(!watch.is_shutdown());
        watch.signal();
        assert!(watch.is_shutdown());
    }
}
