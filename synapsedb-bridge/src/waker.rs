//! Cross-runtime waker integration.
//!
//! The hardest part of the bridge: when the SPSC queue transitions between
//! states (full→not-full, empty→not-empty), the sleeping side must be woken
//! across the runtime boundary.
//!
//! ## Problem
//!
//! - Tokio wakers are `Send + Sync` (they post to Tokio's work-stealing scheduler).
//! - Glommio/monoio wakers are `!Send` (they are core-local eventfd or io_uring CQE).
//!
//! We cannot store a `!Send` Glommio waker inside the shared ring buffer state
//! (which is `Send + Sync`).
//!
//! ## Solution
//!
//! Use an **eventfd** as the cross-runtime signal. Both runtimes can poll an fd:
//!
//! - Producer (Tokio) writes 1 to eventfd when it pushes to a previously-empty queue.
//! - Consumer (TPC) reads from eventfd to wake up.
//! - Consumer (TPC) writes 1 to a *separate* eventfd when it pops from a previously-full queue.
//! - Producer (Tokio) reads from that eventfd to wake up.
//!
//! This avoids storing any `!Send` waker in shared state. The eventfd is an OS
//! primitive that both runtimes can integrate with their respective event loops.

use std::sync::atomic::{AtomicBool, Ordering};

/// Signal for waking the consumer when new data is available.
///
/// In production this will wrap an `eventfd` file descriptor.
/// For testing without io_uring, it uses a simple atomic flag + thread parking.
pub struct WakeSignal {
    /// Atomic flag: true = there's a pending wake notification.
    signaled: AtomicBool,
}

impl WakeSignal {
    /// Create a new wake signal.
    pub fn new() -> Self {
        Self {
            signaled: AtomicBool::new(false),
        }
    }

    /// Signal the other side that it should wake up and check the queue.
    pub fn notify(&self) {
        self.signaled.store(true, Ordering::Release);
    }

    /// Check if a notification is pending and clear it.
    /// Returns `true` if there was a pending notification.
    pub fn take(&self) -> bool {
        self.signaled.swap(false, Ordering::Acquire)
    }

    /// Check if a notification is pending without clearing it.
    pub fn is_signaled(&self) -> bool {
        self.signaled.load(Ordering::Acquire)
    }
}

impl Default for WakeSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_notify_and_take() {
        let signal = WakeSignal::new();

        assert!(!signal.is_signaled());
        assert!(!signal.take());

        signal.notify();
        assert!(signal.is_signaled());
        assert!(signal.take());

        // take() should have cleared it.
        assert!(!signal.is_signaled());
        assert!(!signal.take());
    }

    #[test]
    fn multiple_notifies_coalesce() {
        let signal = WakeSignal::new();

        signal.notify();
        signal.notify();
        signal.notify();

        // Single take() clears all pending notifications.
        assert!(signal.take());
        assert!(!signal.take());
    }
}
