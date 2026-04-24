//! Bitemporal interval types.
//!
//! Two orthogonal time dimensions:
//!
//! - **Valid time** — application time. Client/device-assigned. Stored in value payload.
//! - **System time** — Origin time. Derived from WAL LSN at Raft commit. Stored in key.
//!
//! Both use closed-open semantics `[from, to)`. The open-upper sentinel is
//! [`OPEN_UPPER`] (`i64::MAX`), which represents "still current / never closed".

use serde::{Deserialize, Serialize};

/// Sentinel value marking an interval's upper bound as open ("until further notice").
pub const OPEN_UPPER: i64 = i64::MAX;

/// A two-dimensional interval carrying both valid-time and system-time bounds.
///
/// All fields are milliseconds since the Unix epoch (signed). Upper bounds
/// equal to [`OPEN_UPPER`] mean "currently open".
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BitemporalInterval {
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub system_from_ms: i64,
    pub system_until_ms: i64,
}

impl BitemporalInterval {
    /// Construct an interval that is open on both upper bounds ("current, still valid").
    pub const fn current(valid_from_ms: i64, system_from_ms: i64) -> Self {
        Self {
            valid_from_ms,
            valid_until_ms: OPEN_UPPER,
            system_from_ms,
            system_until_ms: OPEN_UPPER,
        }
    }

    /// Whether the system-time upper bound is still open
    /// (i.e., this version has not been superseded).
    pub const fn is_system_current(&self) -> bool {
        self.system_until_ms == OPEN_UPPER
    }

    /// Whether the valid-time upper bound is still open
    /// (i.e., the fact is still asserted as applicable).
    pub const fn is_valid_current(&self) -> bool {
        self.valid_until_ms == OPEN_UPPER
    }

    /// Whether both bounds are open — the tuple is live and currently asserted.
    pub const fn is_current(&self) -> bool {
        self.is_system_current() && self.is_valid_current()
    }

    /// Whether `t` falls within `[valid_from_ms, valid_until_ms)`.
    pub const fn contains_valid(&self, t: i64) -> bool {
        t >= self.valid_from_ms && t < self.valid_until_ms
    }

    /// Whether `t` falls within `[system_from_ms, system_until_ms)`.
    pub const fn contains_system(&self, t: i64) -> bool {
        t >= self.system_from_ms && t < self.system_until_ms
    }

    /// Whether this interval's valid-time range overlaps `[from, to)`.
    pub const fn overlaps_valid(&self, from: i64, to: i64) -> bool {
        self.valid_from_ms < to && from < self.valid_until_ms
    }

    /// Whether this interval's system-time range overlaps `[from, to)`.
    pub const fn overlaps_system(&self, from: i64, to: i64) -> bool {
        self.system_from_ms < to && from < self.system_until_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_is_both_open() {
        let i = BitemporalInterval::current(100, 200);
        assert!(i.is_current());
        assert!(i.is_valid_current());
        assert!(i.is_system_current());
    }

    #[test]
    fn closed_open_contains() {
        let i = BitemporalInterval {
            valid_from_ms: 10,
            valid_until_ms: 20,
            system_from_ms: 100,
            system_until_ms: 200,
        };
        assert!(i.contains_valid(10));
        assert!(i.contains_valid(19));
        assert!(!i.contains_valid(20));
        assert!(!i.contains_valid(9));
        assert!(i.contains_system(150));
        assert!(!i.contains_system(200));
    }

    #[test]
    fn overlap_semantics() {
        let i = BitemporalInterval {
            valid_from_ms: 10,
            valid_until_ms: 20,
            system_from_ms: 100,
            system_until_ms: 200,
        };
        assert!(i.overlaps_valid(15, 25));
        assert!(i.overlaps_valid(5, 15));
        // Touching at the upper bound is not overlap (closed-open).
        assert!(!i.overlaps_valid(20, 30));
        // Touching at the lower bound IS overlap.
        assert!(i.overlaps_valid(0, 11));
    }

    #[test]
    fn open_upper_is_i64_max() {
        assert_eq!(OPEN_UPPER, i64::MAX);
        let i = BitemporalInterval::current(0, 0);
        assert!(i.contains_valid(i64::MAX - 1));
        assert!(!i.contains_valid(i64::MAX));
    }
}
