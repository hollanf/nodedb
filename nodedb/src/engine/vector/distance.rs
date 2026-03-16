/// Distance metrics for vector similarity search.
///
/// Provides both scalar and SIMD-accelerated implementations for use in
/// HNSW traversal (hot path) and batch operations.
/// Distance metric selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance.
    L2,
    /// Cosine distance (1 - cosine_similarity).
    Cosine,
    /// Inner product (negated for min-heap compatibility).
    InnerProduct,
}

/// Compute L2 squared distance between two vectors.
///
/// Uses auto-vectorization-friendly loop. The compiler will emit SIMD
/// when compiled with `-C target-cpu=native` or AVX2/AVX-512 target features.
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

/// Compute cosine distance: 1.0 - cosine_similarity(a, b).
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
        return 1.0; // Degenerate: zero vector.
    }
    (1.0 - (dot / denom)).max(0.0)
}

/// Compute negative inner product (for max-inner-product search via min-heap).
#[inline]
pub fn neg_inner_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
    }
    -dot
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

/// Batch distance computation: compute distances from `query` to each vector in `candidates`.
///
/// Returns a Vec of (index, distance) pairs sorted by distance (ascending).
pub fn batch_distances(
    query: &[f32],
    candidates: &[&[f32]],
    metric: DistanceMetric,
    top_k: usize,
) -> Vec<(usize, f32)> {
    let mut dists: Vec<(usize, f32)> = candidates
        .iter()
        .enumerate()
        .map(|(i, c)| (i, distance(query, c, metric)))
        .collect();

    // Partial sort for top_k — cheaper than full sort for large candidate sets.
    if top_k < dists.len() {
        dists.select_nth_unstable_by(top_k, |a, b| a.1.partial_cmp(&b.1).unwrap());
        dists.truncate(top_k);
    }
    dists.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    dists
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
        assert_eq!(l2_squared(&a, &b), 25.0); // 3² + 4² = 25
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
    fn inner_product_positive() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        // dot = 4+10+18 = 32, negated = -32
        assert_eq!(neg_inner_product(&a, &b), -32.0);
    }

    #[test]
    fn batch_distances_returns_top_k() {
        let query = [1.0, 0.0];
        let c0: &[f32] = &[1.0, 0.0]; // dist 0
        let c1: &[f32] = &[0.0, 1.0]; // dist 2
        let c2: &[f32] = &[0.5, 0.5]; // dist ~0.29
        let c3: &[f32] = &[-1.0, 0.0]; // dist 4

        let results = batch_distances(&query, &[c0, c1, c2, c3], DistanceMetric::L2, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0); // closest
        assert_eq!(results[1].0, 2); // second closest
    }

    #[test]
    fn cosine_zero_vector() {
        let a = [0.0, 0.0];
        let b = [1.0, 1.0];
        let d = cosine_distance(&a, &b);
        assert_eq!(d, 1.0); // degenerate case
    }

    #[test]
    fn high_dimensional() {
        // 768-dim vectors (typical embedding size).
        let a: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01).collect();
        let b: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01 + 0.001).collect();

        let d = l2_squared(&a, &b);
        assert!(d > 0.0);
        assert!(d < 1.0); // small perturbation

        let c = cosine_distance(&a, &b);
        assert!(c >= 0.0);
        assert!(c < 0.01); // nearly identical directions
    }
}
