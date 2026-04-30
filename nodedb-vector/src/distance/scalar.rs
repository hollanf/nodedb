//! Scalar (non-SIMD) distance metric implementations.
//!
//! Re-exports scalar implementations from `nodedb-types`.
//! Works on all targets: native, WASM, iOS, Android.

pub use nodedb_types::vector_distance::*;

/// Compute distance between two vectors using scalar implementations.
pub fn scalar_distance(a: &[f32], b: &[f32], metric: super::DistanceMetric) -> f32 {
    use super::DistanceMetric::*;
    match metric {
        L2 => l2_squared(a, b),
        Cosine => cosine_distance(a, b),
        InnerProduct => neg_inner_product(a, b),
        Manhattan => manhattan(a, b),
        Chebyshev => chebyshev(a, b),
        Hamming => hamming_f32(a, b),
        Jaccard => jaccard(a, b),
        Pearson => pearson(a, b),
        // Unknown future metric — fall back to L2.
        _ => l2_squared(a, b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_identical_is_zero() {
        let v = [1.0, 2.0, 3.0];
        assert_eq!(l2_squared(&v, &v), 0.0);
    }

    #[test]
    fn l2_known_distance() {
        let a = [0.0, 0.0];
        let b = [3.0, 4.0];
        assert_eq!(l2_squared(&a, &b), 25.0);
    }

    #[test]
    fn cosine_identical_is_zero() {
        let v = [1.0, 2.0, 3.0];
        assert!(cosine_distance(&v, &v) < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_is_one() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn neg_ip_basic() {
        let a = [1.0, 2.0];
        let b = [3.0, 4.0];
        assert_eq!(neg_inner_product(&a, &b), -11.0);
    }

    #[test]
    fn manhattan_basic() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 6.0, 3.0];
        assert_eq!(manhattan(&a, &b), 7.0);
    }

    #[test]
    fn chebyshev_basic() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 6.0, 3.0];
        assert_eq!(chebyshev(&a, &b), 4.0);
    }

    #[test]
    fn hamming_basic() {
        let a = [1.0, 0.0, 1.0, 0.0];
        let b = [1.0, 1.0, 0.0, 0.0];
        assert_eq!(hamming_f32(&a, &b), 2.0);
    }

    #[test]
    fn jaccard_basic() {
        let a = [1.0, 0.0, 1.0, 0.0];
        let b = [1.0, 1.0, 0.0, 0.0];
        let j = jaccard(&a, &b);
        assert!((j - (1.0 - 1.0 / 3.0)).abs() < 1e-6);
    }

    #[test]
    fn pearson_identical_is_zero() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!(pearson(&v, &v) < 1e-6);
    }

    #[test]
    fn pearson_opposite_is_high() {
        let a = [1.0, 2.0, 3.0, 4.0, 5.0];
        let b = [5.0, 4.0, 3.0, 2.0, 1.0];
        assert!(pearson(&a, &b) > 1.5);
    }
}
