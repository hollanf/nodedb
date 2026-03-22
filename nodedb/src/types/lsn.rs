use std::sync::atomic::{AtomicU64, Ordering};

// ── Re-export shared Lsn from nodedb-types ──
pub use nodedb_types::Lsn;

// ── Origin-only: thread-safe LSN generator (uses AtomicU64, not WASM-safe) ──

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
        Lsn::new(self.current.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Current value without incrementing (for snapshot watermarks).
    pub fn current(&self) -> Lsn {
        Lsn::new(self.current.load(Ordering::Relaxed))
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
