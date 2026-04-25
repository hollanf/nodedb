//! `SurrogateBitmap` — a roaring bitmap of `Surrogate` values used as the
//! universal cross-engine prefilter currency.
//!
//! Every engine that produces candidate row sets returns (or accepts) a
//! `SurrogateBitmap`. Cross-engine prefilter reduces to bitmap intersection
//! with zero per-query ID translation.

use roaring::RoaringBitmap;

use crate::surrogate::Surrogate;

/// A dense set of [`Surrogate`] values backed by a roaring bitmap.
///
/// Used as the prefilter currency flowing between engines in a
/// `PhysicalPlan`: the planner may embed a `SurrogateBitmap` produced
/// by one engine sub-plan directly into another engine's prefilter
/// slot without any ID translation step.
#[derive(Debug, Clone, PartialEq)]
pub struct SurrogateBitmap(pub RoaringBitmap);

impl SurrogateBitmap {
    /// Create an empty bitmap.
    pub fn new() -> Self {
        Self(RoaringBitmap::new())
    }

    // `from_iter` is provided by the `FromIterator<Surrogate>` impl below.

    /// Return `true` if `surrogate` is present in the bitmap.
    pub fn contains(&self, surrogate: Surrogate) -> bool {
        self.0.contains(surrogate.0)
    }

    /// Insert `surrogate` into the bitmap.
    pub fn insert(&mut self, surrogate: Surrogate) {
        self.0.insert(surrogate.0);
    }

    /// Remove `surrogate` from the bitmap. No-op when absent.
    pub fn remove(&mut self, surrogate: Surrogate) {
        self.0.remove(surrogate.0);
    }

    /// Number of elements in the bitmap.
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    /// Return `true` if the bitmap contains no elements.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate over the surrogates in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = Surrogate> + '_ {
        self.0.iter().map(Surrogate)
    }
}

impl Default for SurrogateBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<Surrogate> for SurrogateBitmap {
    fn from_iter<I: IntoIterator<Item = Surrogate>>(iter: I) -> Self {
        let mut inner = RoaringBitmap::new();
        for s in iter {
            inner.insert(s.0);
        }
        Self(inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let b = SurrogateBitmap::new();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn insert_and_contains() {
        let mut b = SurrogateBitmap::new();
        b.insert(Surrogate(1));
        b.insert(Surrogate(100));
        assert!(b.contains(Surrogate(1)));
        assert!(b.contains(Surrogate(100)));
        assert!(!b.contains(Surrogate(2)));
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn remove_is_noop_when_absent() {
        let mut b = SurrogateBitmap::new();
        b.remove(Surrogate(99));
        assert!(b.is_empty());
    }

    #[test]
    fn from_iter_deduplicates() {
        let b = SurrogateBitmap::from_iter([Surrogate(1), Surrogate(1), Surrogate(2)]);
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn iter_yields_ascending_order() {
        let b = SurrogateBitmap::from_iter([Surrogate(5), Surrogate(1), Surrogate(3)]);
        let v: Vec<u32> = b.iter().map(|s| s.0).collect();
        assert_eq!(v, vec![1, 3, 5]);
    }

    #[test]
    fn large_bitmap_roundtrip() {
        let b = SurrogateBitmap::from_iter((1u32..=10_000).map(Surrogate));
        assert_eq!(b.len(), 10_000);
        assert!(b.contains(Surrogate(1)));
        assert!(b.contains(Surrogate(10_000)));
        assert!(!b.contains(Surrogate(10_001)));
    }
}
