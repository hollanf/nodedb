//! Hybrid Logical Clock.
//!
//! Canonical timestamp for replicated metadata entries, descriptor
//! modification times, and descriptor lease expiry. Monotonic across
//! wall-clock skew: `update` folds a remote HLC into the local clock so
//! causally later events always receive a strictly greater timestamp.
//!
//! Metadata DDL is not a hot path (hundreds per second at most), so the
//! clock is backed by a short-lived mutex rather than atomics.

use std::cmp::Ordering;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Hybrid Logical Clock timestamp.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Hlc {
    /// Wall-clock component: nanoseconds since the Unix epoch.
    pub wall_ns: u64,
    /// Logical counter incremented when two events share a wall-clock tick.
    pub logical: u32,
}

impl Hlc {
    pub const ZERO: Hlc = Hlc {
        wall_ns: 0,
        logical: 0,
    };

    pub const fn new(wall_ns: u64, logical: u32) -> Self {
        Self { wall_ns, logical }
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.wall_ns.cmp(&other.wall_ns) {
            Ordering::Equal => self.logical.cmp(&other.logical),
            other => other,
        }
    }
}

/// Thread-safe HLC source. One instance per node.
#[derive(Debug, Default)]
pub struct HlcClock {
    state: Mutex<Hlc>,
}

impl HlcClock {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(Hlc::ZERO),
        }
    }

    /// Return a new HLC strictly greater than any previously returned value
    /// and ≥ the current wall clock.
    pub fn now(&self) -> Hlc {
        let wall = wall_now_ns();
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // Clamp wall to at least st.wall_ns so the HLC never regresses on
        // clock skew or NTP adjustments (T4-15).
        let wall = wall.max(st.wall_ns);
        let next = if wall > st.wall_ns {
            Hlc::new(wall, 0)
        } else {
            Hlc::new(st.wall_ns, st.logical.saturating_add(1))
        };
        *st = next;
        next
    }

    /// Fold a remote HLC into the local clock and return a strictly greater HLC
    /// than both the prior local state and the remote observation.
    pub fn update(&self, remote: Hlc) -> Hlc {
        let wall = wall_now_ns();
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let prev = *st;
        let max_wall = wall.max(prev.wall_ns).max(remote.wall_ns);
        let next_logical = if max_wall == prev.wall_ns && max_wall == remote.wall_ns {
            prev.logical.max(remote.logical).saturating_add(1)
        } else if max_wall == prev.wall_ns {
            prev.logical.saturating_add(1)
        } else if max_wall == remote.wall_ns {
            remote.logical.saturating_add(1)
        } else {
            0
        };
        let next = Hlc::new(max_wall, next_logical);
        *st = next;
        next
    }

    /// Read the last observed HLC without advancing.
    pub fn peek(&self) -> Hlc {
        *self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

fn wall_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_within_tick() {
        let clock = HlcClock::new();
        let mut prev = clock.now();
        for _ in 0..1_000 {
            let next = clock.now();
            assert!(next > prev);
            prev = next;
        }
    }

    #[test]
    fn update_produces_strictly_greater() {
        let clock = HlcClock::new();
        let local = clock.now();
        let remote = Hlc::new(local.wall_ns + 1_000_000, 7);
        let merged = clock.update(remote);
        assert!(merged > remote);
        assert!(merged > local);
    }

    #[test]
    fn ordering_is_total() {
        let a = Hlc::new(10, 0);
        let b = Hlc::new(10, 1);
        let c = Hlc::new(11, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }
}
