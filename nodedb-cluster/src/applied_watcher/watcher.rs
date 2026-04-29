//! Sync wait primitive used to block on a Raft group's local apply
//! watermark.
//!
//! Uses `std::sync::Condvar` so it is safe to call from synchronous
//! pgwire handler code without entering a tokio reactor. A `closed`
//! flag distinguishes "still waiting for apply" from "the group is
//! no longer hosted on this node" — a follower that gets removed via
//! conf-change must wake outstanding waiters with a terminal status
//! rather than time them out.

use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Outcome of [`AppliedIndexWatcher::wait_for`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The applied watermark reached the requested target.
    Reached,
    /// The deadline elapsed before the target was reached.
    TimedOut,
    /// The group was closed (removed from this node) while waiting.
    GroupGone,
}

impl WaitOutcome {
    pub fn is_reached(self) -> bool {
        matches!(self, Self::Reached)
    }
}

/// Tracks the highest Raft log index applied on this node for one
/// Raft group.
#[derive(Debug, Default)]
pub struct AppliedIndexWatcher {
    state: Mutex<State>,
    cv: Condvar,
}

#[derive(Debug, Default)]
struct State {
    applied: u64,
    closed: bool,
}

impl AppliedIndexWatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the watermark. Idempotent: smaller indices are ignored.
    /// Bumps on a closed watcher are also ignored (the group is gone).
    pub fn bump(&self, applied_index: u64) {
        let mut guard = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if guard.closed {
            return;
        }
        if applied_index > guard.applied {
            guard.applied = applied_index;
            self.cv.notify_all();
        }
    }

    /// Read the current watermark without blocking.
    pub fn current(&self) -> u64 {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).applied
    }

    /// True once [`Self::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).closed
    }

    /// Mark this watcher closed and wake every waiter with
    /// [`WaitOutcome::GroupGone`]. Idempotent.
    pub fn close(&self) {
        let mut guard = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !guard.closed {
            guard.closed = true;
            self.cv.notify_all();
        }
    }

    /// Block until the watermark reaches `target`, the timeout elapses,
    /// or the watcher is closed.
    pub fn wait_for(&self, target: u64, timeout: Duration) -> WaitOutcome {
        let deadline = Instant::now() + timeout;
        let mut guard = self.state.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if guard.applied >= target {
                return WaitOutcome::Reached;
            }
            if guard.closed {
                return WaitOutcome::GroupGone;
            }
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(r) if !r.is_zero() => r,
                _ => return WaitOutcome::TimedOut,
            };
            let wait = self
                .cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|p| p.into_inner());
            guard = wait.0;
            if wait.1.timed_out() && guard.applied < target && !guard.closed {
                return WaitOutcome::TimedOut;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn bump_notifies_waiter() {
        let w = Arc::new(AppliedIndexWatcher::new());
        let w2 = w.clone();
        let h = thread::spawn(move || w2.wait_for(5, Duration::from_secs(2)));
        thread::sleep(Duration::from_millis(20));
        w.bump(3);
        thread::sleep(Duration::from_millis(20));
        w.bump(5);
        assert_eq!(h.join().unwrap(), WaitOutcome::Reached);
    }

    #[test]
    fn times_out_if_never_bumped() {
        let w = AppliedIndexWatcher::new();
        assert_eq!(
            w.wait_for(1, Duration::from_millis(30)),
            WaitOutcome::TimedOut
        );
    }

    #[test]
    fn already_past_target_returns_immediately() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        assert_eq!(
            w.wait_for(5, Duration::from_millis(10)),
            WaitOutcome::Reached
        );
    }

    #[test]
    fn close_wakes_waiter_with_group_gone() {
        let w = Arc::new(AppliedIndexWatcher::new());
        let w2 = w.clone();
        let h = thread::spawn(move || w2.wait_for(99, Duration::from_secs(2)));
        thread::sleep(Duration::from_millis(20));
        w.close();
        assert_eq!(h.join().unwrap(), WaitOutcome::GroupGone);
    }

    #[test]
    fn bump_after_close_is_ignored() {
        let w = AppliedIndexWatcher::new();
        w.close();
        w.bump(5);
        assert_eq!(w.current(), 0);
        assert!(w.is_closed());
    }

    #[test]
    fn close_is_idempotent() {
        let w = AppliedIndexWatcher::new();
        w.close();
        w.close();
        assert!(w.is_closed());
    }
}
