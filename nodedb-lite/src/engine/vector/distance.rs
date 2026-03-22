//! Distance metrics for vector similarity search.
//!
//! Pure scalar implementations that the compiler auto-vectorizes.
//! No SIMD intrinsics — works on all targets (native, WASM, iOS, Android).

/// Distance metric selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean (L2) squared distance.
    L2,
    /// Cosine distance (1 - cosine_similarity).
    Cosine,
    /// Negative inner product (for max-inner-product search via min-heap).
    InnerProduct,
}

/// Compute distance using the specified metric.
#[inline]
pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_squared(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => neg_inner_product(a, b),
    }
}

/// Euclidean (L2) squared distance.
///
/// Loop structure is auto-vectorization-friendly — the compiler emits
/// SIMD on native targets with `-C target-cpu=native`.
#[inline]
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

/// Cosine distance: 1.0 - cosine_similarity(a, b).
///
/// Returns 0.0 for identical directions, 2.0 for opposite directions.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denom = (norm_a * norm_b).sqrt();
    if denom < f32::EPSILON {
        return 1.0;
    }
    (1.0 - (dot / denom)).max(0.0)
}

/// Negative inner product (for max-inner-product search via min-heap).
#[inline]
pub fn neg_inner_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
    }
    -dot
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
        let d = cosine_distance(&v, &v);
        assert!(d.abs() < 1e-6, "expected ~0, got {d}");
    }

    #[test]
    fn cosine_orthogonal_is_one() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        let d = cosine_distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-6, "expected ~1, got {d}");
    }

    #[test]
    fn cosine_opposite_is_two() {
        let a = [1.0, 0.0];
        let b = [-1.0, 0.0];
        let d = cosine_distance(&a, &b);
        assert!((d - 2.0).abs() < 1e-6, "expected ~2, got {d}");
    }

    #[test]
    fn cosine_zero_vector() {
        let a = [0.0, 0.0];
        let b = [1.0, 1.0];
        assert_eq!(cosine_distance(&a, &b), 1.0);
    }

    #[test]
    fn inner_product_positive() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        assert_eq!(neg_inner_product(&a, &b), -32.0);
    }

    #[test]
    fn dispatch_works() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        assert!(distance(&a, &b, DistanceMetric::L2) > 0.0);
        assert!(distance(&a, &b, DistanceMetric::Cosine) > 0.0);
        assert!(distance(&a, &b, DistanceMetric::InnerProduct).abs() < 1e-6);
    }

    #[test]
    fn high_dimensional() {
        let a: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01).collect();
        let b: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01 + 0.001).collect();
        let d = l2_squared(&a, &b);
        assert!((0.0..1.0).contains(&d));
        let c = cosine_distance(&a, &b);
        assert!((0.0..0.01).contains(&c));
    }
}
