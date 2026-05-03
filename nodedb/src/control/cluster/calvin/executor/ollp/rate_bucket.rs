//! Sliding-window rate counter for per-tenant OLLP budgets.
//!
//! `RateBucket` tracks event timestamps in a `VecDeque<Instant>` and
//! counts how many events fall within the trailing `window` duration.
//!
//! `Instant::now()` is used here for the rate window (observability /
//! off-WAL path only).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A sliding-window event counter.
pub struct RateBucket {
    /// Recent event timestamps, front = oldest, back = newest.
    events: VecDeque<Instant>,
    /// Width of the sliding window.
    window: Duration,
    /// Maximum events within the window before the budget is exceeded.
    capacity: usize,
}

impl RateBucket {
    /// Create a new `RateBucket`.
    pub fn new(capacity: usize, window: Duration) -> Self {
        Self {
            events: VecDeque::new(),
            window,
            capacity,
        }
    }

    /// Record one event at `Instant::now()` and return `true` if the budget
    /// has been exceeded (i.e., more than `capacity` events in the window
    /// AFTER recording this one).
    ///
    /// `Instant::now()` — observability/off-WAL path only.
    pub fn record_and_check(&mut self) -> bool {
        // no-determinism: OLLP rate-limiter observability, not Calvin WAL data
        let now = Instant::now();
        self.evict_old(now);
        self.events.push_back(now);
        self.events.len() > self.capacity
    }

    /// Current count of events within the window.
    pub fn count(&self) -> usize {
        self.events.len()
    }

    fn evict_old(&mut self, now: Instant) {
        while let Some(&front) = self.events.front() {
            if now.duration_since(front) > self.window {
                self.events.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_under_capacity_returns_false() {
        let mut bucket = RateBucket::new(5, Duration::from_secs(60));
        for _ in 0..5 {
            assert!(!bucket.record_and_check());
        }
    }

    #[test]
    fn bucket_over_capacity_returns_true() {
        let mut bucket = RateBucket::new(5, Duration::from_secs(60));
        for _ in 0..5 {
            bucket.record_and_check();
        }
        assert!(bucket.record_and_check(), "6th event should exceed cap=5");
    }
}
