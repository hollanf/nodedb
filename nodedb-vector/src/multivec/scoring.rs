//! MaxSim aggregation for multi-vector late interaction (ColBERT / MetaEmbed).
//!
//! `score(Q, D) = Σᵢ maxⱼ sim(qᵢ, dⱼ)`
//!
//! `budgeted_maxsim` restricts the query side to the first `budget` vectors,
//! enabling Matryoshka ordering for latency-elastic late interaction.

use nodedb_types::vector_distance::DistanceMetric;

use crate::distance::scalar::scalar_distance;

// ---------------------------------------------------------------------------
// Similarity helpers
// ---------------------------------------------------------------------------

/// Convert a distance value produced by `scalar_distance` into a similarity
/// score where higher is better.
///
/// | Metric         | Conversion          |
/// |----------------|---------------------|
/// | L2             | `−distance`         |
/// | Cosine         | `1 − distance`      |
/// | InnerProduct   | `−distance` (scalar_distance returns neg-IP) |
/// | All others     | `−distance`         |
fn dist_to_sim(d: f32, metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => 1.0 - d,
        // All other metrics (and unknown future metrics) use negated distance.
        _ => -d,
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compute the MaxSim score between a query multi-vector and a document
/// multi-vector.
///
/// `score(Q, D) = Σᵢ maxⱼ sim(qᵢ, dⱼ)`
///
/// Returns 0.0 if either side is empty.
pub fn maxsim(query: &[Vec<f32>], doc: &[Vec<f32>], metric: DistanceMetric) -> f32 {
    if query.is_empty() || doc.is_empty() {
        return 0.0;
    }
    query
        .iter()
        .map(|q| {
            doc.iter()
                .map(|d| dist_to_sim(scalar_distance(q, d, metric), metric))
                .fold(f32::NEG_INFINITY, f32::max)
        })
        .sum()
}

/// Budgeted MaxSim: only uses the first `budget` query vectors (Matryoshka
/// ordering).  When `budget` equals or exceeds `query.len()` this is
/// equivalent to `maxsim`.
///
/// Returns 0.0 if either side is empty or budget is 0.
pub fn budgeted_maxsim(
    query: &[Vec<f32>],
    doc: &[Vec<f32>],
    budget: u8,
    metric: DistanceMetric,
) -> f32 {
    let effective = (budget as usize).min(query.len());
    maxsim(&query[..effective], doc, metric)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a unit vector of dimension `dim` with a single non-zero entry at
    /// position `pos`.
    fn unit_vec(dim: usize, pos: usize) -> Vec<f32> {
        let mut v = vec![0.0f32; dim];
        v[pos] = 1.0;
        v
    }

    #[test]
    fn maxsim_identical_query_doc_l2() {
        // When every query vector equals the corresponding doc vector the
        // MaxSim for each query vector is dist_to_sim(0.0, L2) = 0.0.
        // Sum across 3 query vectors = 0.0.
        let q = vec![unit_vec(4, 0), unit_vec(4, 1), unit_vec(4, 2)];
        let d = q.clone();
        let score = maxsim(&q, &d, DistanceMetric::L2);
        // L2 squared of identical vectors is 0; sim = -0 = 0.
        assert!((score - 0.0).abs() < 1e-6, "score={score}");
    }

    #[test]
    fn maxsim_identical_query_doc_cosine() {
        // Cosine of identical unit vectors = 0 distance → sim = 1.
        // 3 query vectors → sum = 3.
        let q = vec![unit_vec(4, 0), unit_vec(4, 1), unit_vec(4, 2)];
        let d = q.clone();
        let score = maxsim(&q, &d, DistanceMetric::Cosine);
        assert!((score - 3.0).abs() < 1e-5, "score={score}");
    }

    #[test]
    fn maxsim_empty_query_returns_zero() {
        let d = vec![unit_vec(4, 0)];
        assert_eq!(maxsim(&[], &d, DistanceMetric::Cosine), 0.0);
    }

    #[test]
    fn maxsim_empty_doc_returns_zero() {
        let q = vec![unit_vec(4, 0)];
        assert_eq!(maxsim(&q, &[], DistanceMetric::Cosine), 0.0);
    }

    #[test]
    fn budgeted_vs_full_differ_when_extra_tokens_are_relevant() {
        // 4 query vectors: first 2 match doc perfectly (cosine dist ≈ 0 → sim = 1),
        // last 2 are orthogonal to everything in doc (sim < 1).
        let q = vec![
            unit_vec(4, 0),
            unit_vec(4, 1),
            unit_vec(4, 2),
            unit_vec(4, 3),
        ];
        let d = vec![unit_vec(4, 0), unit_vec(4, 1)];

        let full = maxsim(&q, &d, DistanceMetric::Cosine);
        let budgeted = budgeted_maxsim(&q, &d, 2, DistanceMetric::Cosine);

        // Full uses all 4 query vectors; budgeted uses only the first 2.
        // The third and fourth query vectors are orthogonal to d[0] and d[1]
        // so their max-sim is 0 (cosine dist = 1 → sim = 0).
        // Therefore full = budgeted = 2.0 in this specific case.
        // To see a difference, add a doc vector that matches q[2].
        let d2 = vec![unit_vec(4, 0), unit_vec(4, 1), unit_vec(4, 2)];
        let full2 = maxsim(&q, &d2, DistanceMetric::Cosine);
        let budgeted2 = budgeted_maxsim(&q, &d2, 2, DistanceMetric::Cosine);

        // full2 uses q[2] which now matches d2[2] → full2 = 3.0 (q[3] still 0).
        // budgeted2 only uses q[0..2] → 2.0.
        assert!(
            (full - budgeted).abs() < 1e-5,
            "first case should be equal: full={full} budgeted={budgeted}"
        );
        assert!(
            full2 > budgeted2,
            "full2 should exceed budgeted2: full2={full2} budgeted2={budgeted2}"
        );
    }

    #[test]
    fn budgeted_zero_budget_returns_zero() {
        let q = vec![unit_vec(4, 0)];
        let d = vec![unit_vec(4, 0)];
        assert_eq!(budgeted_maxsim(&q, &d, 0, DistanceMetric::Cosine), 0.0);
    }

    #[test]
    fn budgeted_exceeds_query_length_equals_full() {
        let q = vec![unit_vec(4, 0), unit_vec(4, 1)];
        let d = q.clone();
        let full = maxsim(&q, &d, DistanceMetric::Cosine);
        let budgeted = budgeted_maxsim(&q, &d, 255, DistanceMetric::Cosine);
        assert!((full - budgeted).abs() < 1e-6);
    }
}
