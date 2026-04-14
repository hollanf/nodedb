//! Per-loop consumer side of the shutdown watch.
//!
//! Held by every background loop. Expected usage pattern:
//!
//! ```ignore
//! loop {
//!     tokio::select! {
//!         _ = shutdown.wait_cancelled() => break,
//!         _ = tick.tick() => { /* work */ }
//!     }
//! }
//! ```
//!
//! `wait_cancelled` is cancel-safe — dropping the future in a
//! `select!` losing arm does not miss a subsequent signal
//! because the underlying `watch::Receiver::changed` is itself
//! cancel-safe and the state is re-checked on every poll.

use tokio::sync::watch;

/// Consumer handle for [`super::ShutdownWatch`]. One per
/// subscribed loop — clone-cheap but there is no reason to
/// clone: each loop gets its own handle from `subscribe()`.
#[derive(Debug)]
pub struct ShutdownReceiver {
    rx: watch::Receiver<bool>,
}

impl ShutdownReceiver {
    /// Internal constructor used by [`super::ShutdownWatch::subscribe`].
    /// Not public — the canonical way to obtain a receiver is
    /// through `ShutdownWatch::subscribe` so the subscribe-fresh
    /// guarantee is preserved.
    pub(super) fn from_watch(rx: watch::Receiver<bool>) -> Self {
        Self { rx }
    }

    /// Resolves as soon as the watch reports the shutdown flag.
    /// Cancel-safe for use inside `tokio::select!`.
    ///
    /// If the watch has already been signaled before this is
    /// first polled, it returns immediately — the subscribe-fresh
    /// guarantee on `ShutdownWatch` means `*rx.borrow() == true`
    /// on the first check fires without waiting for a future
    /// `changed()` notification.
    pub async fn wait_cancelled(&mut self) {
        // Fast path: already signaled.
        if *self.rx.borrow() {
            return;
        }
        // Slow path: wait for a state change, then re-check.
        // `changed()` can return `Err` if every sender was
        // dropped — treat that as "shutdown is no longer
        // observable", which in practice only happens during
        // destructor races. Returning from the future preserves
        // the "loop exits promptly" contract.
        while self.rx.changed().await.is_ok() {
            if *self.rx.borrow() {
                return;
            }
        }
    }

    /// Non-blocking check. Useful in the prelude of an expensive
    /// operation — "don't start a compaction if we're already
    /// shutting down".
    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }
}

#[cfg(test)]
mod tests {
    use super::super::ShutdownWatch;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_cancelled_returns_on_signal() {
        let watch = ShutdownWatch::new();
        let mut rx = watch.subscribe();

        let handle = tokio::spawn(async move {
            rx.wait_cancelled().await;
        });

        // Let the task park on wait_cancelled.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());

        watch.signal();
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("task did not wake after signal")
            .expect("task panicked");
    }

    #[tokio::test]
    async fn wait_cancelled_returns_immediately_if_already_signaled() {
        let watch = ShutdownWatch::new();
        watch.signal();

        let mut rx = watch.subscribe();
        tokio::time::timeout(Duration::from_millis(10), rx.wait_cancelled())
            .await
            .expect("already-signaled receiver did not return immediately");
    }

    #[tokio::test]
    async fn wait_cancelled_survives_select() {
        // Prove the drop-on-losing-arm semantics: a signal that
        // never arrives leaves the select free to resolve the
        // other future. This is the primary usage pattern in
        // real loops.
        let watch = ShutdownWatch::new();
        let mut rx = watch.subscribe();

        let result = tokio::select! {
            _ = rx.wait_cancelled() => "shutdown",
            _ = tokio::time::sleep(Duration::from_millis(20)) => "tick",
        };
        assert_eq!(result, "tick");
        assert!(!rx.is_cancelled());
    }

    #[tokio::test]
    async fn is_cancelled_reflects_state() {
        let watch = ShutdownWatch::new();
        let rx = watch.subscribe();
        assert!(!rx.is_cancelled());
        watch.signal();
        assert!(rx.is_cancelled());
    }
}
