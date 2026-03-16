use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Log Sequence Number — monotonically increasing identifier for WAL records.
///
/// LSNs are the universal ordering primitive. Every write, every snapshot,
/// every consistency check is anchored to an LSN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lsn:{}", self.0)
    }
}

/// Thread-safe monotonic LSN generator.
pub struct LsnGenerator {
    current: AtomicU64,
}

impl LsnGenerator {
    pub const fn new(start: u64) -> Self {
        Self {
            current: AtomicU64::new(start),
        }
    }

    /// Atomically increment and return the next LSN.
    pub fn next(&self) -> Lsn {
        Lsn(self.current.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Current value without incrementing (for snapshot watermarks).
    pub fn current(&self) -> Lsn {
        Lsn(self.current.load(Ordering::Relaxed))
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
    }

    #[test]
    fn lsn_generator_monotonic() {
        let generator = LsnGenerator::new(0);
        let a = generator.next();
        let b = generator.next();
        let c = generator.next();
        assert_eq!(a, Lsn::new(1));
        assert_eq!(b, Lsn::new(2));
        assert_eq!(c, Lsn::new(3));
        assert_eq!(generator.current(), Lsn::new(3));
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
