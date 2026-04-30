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
///
/// `#[non_exhaustive]` — a `epoch_id` discriminant for multi-cluster
/// logical epochs may be added in a future release.
///
/// # Wire format note
///
/// `logical` was widened from `u32` to `u64` (T4-11) to eliminate
/// saturation under high-frequency burst writes. Persisted zerompk
/// blobs from before this change carry a 32-bit logical counter;
/// MessagePack integers decode by value (not by declared width), so
/// existing small values deserialise correctly. New writes encode
/// logical as a 64-bit integer, which is forward-incompatible with
/// readers compiled against the old type. See `CATALOG_FORMAT_VERSION`
/// in `nodedb-cluster` for the persisted-format bump that gates this.
#[non_exhaustive]
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
    pub logical: u64,
}

impl Hlc {
    pub const ZERO: Hlc = Hlc {
        wall_ns: 0,
        logical: 0,
    };

    pub const fn new(wall_ns: u64, logical: u64) -> Self {
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
            Hlc::new(st.wall_ns, st.logical + 1)
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
            prev.logical.max(remote.logical) + 1
        } else if max_wall == prev.wall_ns {
            prev.logical + 1
        } else if max_wall == remote.wall_ns {
            remote.logical + 1
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
        .unwrap_or_else(|_| {
            use std::sync::atomic::{AtomicBool, Ordering};
            static LOGGED: AtomicBool = AtomicBool::new(false);
            if !LOGGED.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    module = module_path!(),
                    "system clock is before UNIX_EPOCH; using 0 (epoch) \
                     — check NTP/RTC configuration"
                );
            }
            0
        })
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

    /// 100K rapid-fire `now()` calls must each be strictly greater than
    /// the previous. Verifies the u64 logical counter advances without
    /// saturation.
    #[test]
    fn burst_strictly_monotonic() {
        let clock = HlcClock::new();
        let mut prev = clock.now();
        for _ in 0..100_000 {
            let next = clock.now();
            assert!(
                next > prev,
                "monotonicity violated: prev={prev:?} next={next:?}"
            );
            prev = next;
        }
    }

    /// Directly set the clock's logical counter to `u32::MAX` (the old
    /// saturation ceiling) and assert that the next `now()` returns a
    /// strictly greater HLC. Under the old `u32::saturating_add(1)` this
    /// would have pinned and returned an equal value.
    #[test]
    fn saturating_add_regression() {
        let clock = HlcClock::new();
        // Prime the clock so wall_ns is fixed.
        let seed = clock.now();
        // Inject logical = u32::MAX at the same wall_ns to reproduce the
        // old saturation point.
        {
            let mut st = clock.state.lock().unwrap_or_else(|p| p.into_inner());
            *st = Hlc::new(seed.wall_ns, u32::MAX as u64);
        }
        let next = clock.now();
        assert!(
            next > Hlc::new(seed.wall_ns, u32::MAX as u64),
            "logical counter pinned at u32::MAX: next={next:?}"
        );
    }

    /// 100K `update(remote)` calls with a fixed remote must each produce
    /// a strictly greater HLC than the prior call.
    #[test]
    fn update_burst_strictly_monotonic() {
        let clock = HlcClock::new();
        let remote = clock.now();
        let mut prev = clock.update(remote);
        for _ in 0..100_000 {
            let next = clock.update(remote);
            assert!(
                next > prev,
                "update monotonicity violated: prev={prev:?} next={next:?}"
            );
            prev = next;
        }
    }

    /// An `Hlc` with `logical` above `u32::MAX` must survive a zerompk
    /// encode/decode roundtrip with the value preserved exactly.
    #[test]
    fn roundtrip_logical_gt_u32_max() {
        let logical_val = (u32::MAX as u64) + 1;
        let original = Hlc::new(1_700_000_000_000_000_000_u64, logical_val);
        let bytes = zerompk::to_msgpack_vec(&original).expect("encode");
        let decoded: Hlc = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, original);
        assert_eq!(decoded.logical, logical_val);
    }
}
