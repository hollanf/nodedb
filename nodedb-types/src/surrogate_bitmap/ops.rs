//! Set operations on [`SurrogateBitmap`].
//!
//! All operations are pure roaring bitmap delegations. Consuming variants
//! are provided for the hot in-place paths; cloning variants exist for
//! the few planner paths that must produce a new bitmap without mutating
//! an existing one.

use super::bitmap::SurrogateBitmap;

impl SurrogateBitmap {
    /// Return the intersection of `self` and `other` as a new bitmap.
    pub fn intersect(&self, other: &Self) -> Self {
        Self(&self.0 & &other.0)
    }

    /// Return the union of `self` and `other` as a new bitmap.
    pub fn union(&self, other: &Self) -> Self {
        Self(&self.0 | &other.0)
    }

    /// Return the set difference `self \ other` as a new bitmap.
    pub fn difference(&self, other: &Self) -> Self {
        Self(&self.0 - &other.0)
    }

    /// Return `true` if every element of `self` is also in `other`.
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    /// Mutate `self` in place to hold only elements also in `other`.
    pub fn intersect_in_place(&mut self, other: &Self) {
        self.0 &= &other.0;
    }

    /// Mutate `self` in place to hold the union with `other`.
    pub fn union_in_place(&mut self, other: &Self) {
        self.0 |= &other.0;
    }
}

#[cfg(test)]
mod tests {
    use crate::surrogate::Surrogate;
    use crate::surrogate_bitmap::SurrogateBitmap;

    fn bmp(vals: &[u32]) -> SurrogateBitmap {
        SurrogateBitmap::from_iter(vals.iter().copied().map(Surrogate))
    }

    #[test]
    fn intersect_returns_common_elements() {
        let a = bmp(&[1, 2, 3, 4]);
        let b = bmp(&[3, 4, 5, 6]);
        let c = a.intersect(&b);
        assert_eq!(c.len(), 2);
        assert!(c.contains(Surrogate(3)));
        assert!(c.contains(Surrogate(4)));
        assert!(!c.contains(Surrogate(1)));
    }

    #[test]
    fn union_contains_all_elements() {
        let a = bmp(&[1, 2]);
        let b = bmp(&[2, 3]);
        let c = a.union(&b);
        assert_eq!(c.len(), 3);
        for v in [1u32, 2, 3] {
            assert!(c.contains(Surrogate(v)));
        }
    }

    #[test]
    fn difference_removes_other_elements() {
        let a = bmp(&[1, 2, 3]);
        let b = bmp(&[2, 3]);
        let c = a.difference(&b);
        assert_eq!(c.len(), 1);
        assert!(c.contains(Surrogate(1)));
        assert!(!c.contains(Surrogate(2)));
    }

    #[test]
    fn is_subset_of() {
        let a = bmp(&[1, 2]);
        let b = bmp(&[1, 2, 3]);
        assert!(a.is_subset_of(&b));
        assert!(!b.is_subset_of(&a));
    }

    #[test]
    fn intersect_in_place_mutates() {
        let mut a = bmp(&[1, 2, 3]);
        let b = bmp(&[2, 3, 4]);
        a.intersect_in_place(&b);
        assert_eq!(a.len(), 2);
        assert!(a.contains(Surrogate(2)));
        assert!(a.contains(Surrogate(3)));
    }

    #[test]
    fn union_in_place_mutates() {
        let mut a = bmp(&[1, 2]);
        let b = bmp(&[3, 4]);
        a.union_in_place(&b);
        assert_eq!(a.len(), 4);
    }
}
