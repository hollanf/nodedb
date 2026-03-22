//! Log Sequence Number — monotonically increasing identifier for WAL records.
//!
//! LSNs are the universal ordering primitive across both Origin and Lite.
//! On Origin: WAL record ordering. On Lite: sync watermark tracking.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Log Sequence Number.
///
/// On Origin: anchors every write, snapshot, and consistency check.
/// On Lite: tracks sync progress (last-seen LSN from Origin).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Lsn(u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next LSN (self + 1).
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Returns true if this LSN is ahead of `other`.
    pub const fn is_ahead_of(self, other: Self) -> bool {
        self.0 > other.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lsn:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_ordering() {
        let a = Lsn::new(1);
        let b = Lsn::new(2);
        assert!(a < b);
        assert_eq!(a.next(), b);
        assert!(b.is_ahead_of(a));
        assert!(!a.is_ahead_of(b));
    }

    #[test]
    fn lsn_display() {
        assert_eq!(Lsn::new(42).to_string(), "lsn:42");
    }

    #[test]
    fn lsn_zero() {
        assert_eq!(Lsn::ZERO.as_u64(), 0);
    }
}
