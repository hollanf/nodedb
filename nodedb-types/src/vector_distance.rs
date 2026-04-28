//! Shared scalar distance metric implementations.
//!
//! Pure scalar functions that the compiler auto-vectorizes. Used by both
//! `nodedb` (with optional SIMD dispatch) and `nodedb-lite` (scalar only).

/// Distance metric selection.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DistanceMetric {
    /// Euclidean (L2) squared distance.
    L2 = 0,
    /// Cosine distance (1 - cosine_similarity).
    Cosine = 1,
    /// Negative inner product (for max-inner-product search via min-heap).
    InnerProduct = 2,
    /// Manhattan (L1) distance: sum of absolute differences.
    Manhattan = 3,
    /// Chebyshev (L-infinity) distance: max absolute difference.
    Chebyshev = 4,
    /// Hamming distance for binary-like vectors (threshold > 0.5).
    Hamming = 5,
    /// Jaccard distance for binary-like vectors (threshold > 0.5).
    Jaccard = 6,
    /// Pearson distance: 1 - Pearson correlation coefficient.
    Pearson = 7,
}

/// Euclidean (L2) squared distance.
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

/// Manhattan (L1) distance: sum of absolute differences.
#[inline]
pub fn manhattan(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        sum += (a[i] - b[i]).abs();
    }
    sum
}

/// Chebyshev (L-infinity) distance: max absolute difference.
#[inline]
pub fn chebyshev(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut max = 0.0f32;
    for i in 0..a.len() {
        let d = (a[i] - b[i]).abs();
        if d > max {
            max = d;
        }
    }
    max
}

/// Hamming distance for f32 vectors (values > 0.5 treated as 1).
#[inline]
pub fn hamming_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut count = 0u32;
    for i in 0..a.len() {
        let ba = a[i] > 0.5;
        let bb = b[i] > 0.5;
        if ba != bb {
            count += 1;
        }
    }
    count as f32
}

/// Jaccard distance for f32 vectors (values > 0.5 treated as set membership).
///
/// Returns 1 - |intersection|/|union|. If both are zero-sets, returns 0.0.
#[inline]
pub fn jaccard(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut intersection = 0u32;
    let mut union = 0u32;
    for i in 0..a.len() {
        let ba = a[i] > 0.5;
        let bb = b[i] > 0.5;
        if ba || bb {
            union += 1;
        }
        if ba && bb {
            intersection += 1;
        }
    }
    if union == 0 {
        0.0
    } else {
        1.0 - (intersection as f32 / union as f32)
    }
}

/// Pearson distance: 1 - Pearson correlation coefficient.
///
/// Returns 0.0 for perfectly correlated, 1.0 for uncorrelated, ~2.0 for
/// perfectly anti-correlated.
#[inline]
pub fn pearson(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len() as f32;
    if n < 2.0 {
        return 1.0;
    }
    let mut sum_a = 0.0f32;
    let mut sum_b = 0.0f32;
    for i in 0..a.len() {
        sum_a += a[i];
        sum_b += b[i];
    }
    let mean_a = sum_a / n;
    let mean_b = sum_b / n;

    let mut cov = 0.0f32;
    let mut var_a = 0.0f32;
    let mut var_b = 0.0f32;
    for i in 0..a.len() {
        let da = a[i] - mean_a;
        let db = b[i] - mean_b;
        cov += da * db;
        var_a += da * da;
        var_b += db * db;
    }
    let denom = (var_a * var_b).sqrt();
    if denom < f32::EPSILON {
        return 1.0;
    }
    (1.0 - cov / denom).max(0.0)
}

/// Compute distance using the specified metric (scalar dispatch).
#[inline]
pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_squared(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => neg_inner_product(a, b),
        DistanceMetric::Manhattan => manhattan(a, b),
        DistanceMetric::Chebyshev => chebyshev(a, b),
        DistanceMetric::Hamming => hamming_f32(a, b),
        DistanceMetric::Jaccard => jaccard(a, b),
        DistanceMetric::Pearson => pearson(a, b),
    }
}
