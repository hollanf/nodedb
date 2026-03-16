/// Reciprocal Rank Fusion (RRF) for combining ranked results from multiple engines.
///
/// RRF is used when a query hits multiple engines (e.g., vector similarity +
/// metadata filter + BM25 text search). Each engine returns a ranked list;
/// RRF combines them into a single ranked list.
///
/// Formula: RRF_score(d) = Σ 1 / (k + rank_i(d))
/// where k is a smoothing constant (default 60).
const DEFAULT_K: f64 = 60.0;

/// A scored result from a single engine.
#[derive(Debug, Clone)]
pub struct RankedResult {
    /// Document identifier (engine-specific).
    pub document_id: String,
    /// Rank within the engine's result list (0-based).
    pub rank: usize,
    /// Original score from the engine (for diagnostics).
    pub score: f32,
    /// Source engine identifier.
    pub source: &'static str,
}

/// A fused result after RRF combination.
#[derive(Debug, Clone)]
pub struct FusedResult {
    pub document_id: String,
    pub rrf_score: f64,
    /// Per-engine contributions for explainability.
    pub contributions: Vec<(&'static str, f64)>,
}

/// Fuse multiple ranked result lists using Reciprocal Rank Fusion.
///
/// Each inner Vec is a ranked list from one engine (ordered by relevance).
/// Returns the top_k fused results sorted by RRF score (descending).
pub fn reciprocal_rank_fusion(
    ranked_lists: &[Vec<RankedResult>],
    k: Option<f64>,
    top_k: usize,
) -> Vec<FusedResult> {
    let k = k.unwrap_or(DEFAULT_K);

    // Collect all document IDs and their per-engine contributions.
    let mut scores: std::collections::HashMap<String, Vec<(&'static str, f64)>> =
        std::collections::HashMap::new();

    for list in ranked_lists {
        for result in list {
            let contribution = 1.0 / (k + result.rank as f64 + 1.0);
            scores
                .entry(result.document_id.clone())
                .or_default()
                .push((result.source, contribution));
        }
    }

    // Build fused results.
    let mut fused: Vec<FusedResult> = scores
        .into_iter()
        .map(|(doc_id, contributions)| {
            let rrf_score = contributions.iter().map(|(_, s)| s).sum();
            FusedResult {
                document_id: doc_id,
                rrf_score,
                contributions,
            }
        })
        .collect();

    // Sort by RRF score descending.
    fused.sort_unstable_by(|a, b| b.rrf_score.partial_cmp(&a.rrf_score).unwrap());
    fused.truncate(top_k);
    fused
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ranked(doc_ids: &[&str], source: &'static str) -> Vec<RankedResult> {
        doc_ids
            .iter()
            .enumerate()
            .map(|(rank, &id)| RankedResult {
                document_id: id.to_string(),
                rank,
                score: 1.0 - (rank as f32 * 0.1),
                source,
            })
            .collect()
    }

    #[test]
    fn single_list_preserves_order() {
        let list = make_ranked(&["d1", "d2", "d3"], "vector");
        let fused = reciprocal_rank_fusion(&[list], None, 10);

        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].document_id, "d1");
        assert_eq!(fused[1].document_id, "d2");
        assert_eq!(fused[2].document_id, "d3");
    }

    #[test]
    fn overlapping_lists_boost_common_docs() {
        let vector = make_ranked(&["d1", "d2", "d3"], "vector");
        let sparse = make_ranked(&["d2", "d1", "d4"], "sparse");

        let fused = reciprocal_rank_fusion(&[vector, sparse], None, 10);

        // d1 and d2 appear in both lists → higher RRF scores.
        // d1: 1/(61) + 1/(62) ≈ 0.01639 + 0.01613 = 0.03252
        // d2: 1/(62) + 1/(61) ≈ 0.01613 + 0.01639 = 0.03252
        // They tie, but both should be above d3 and d4 (single list only).
        let top2_ids: Vec<&str> = fused[..2].iter().map(|f| f.document_id.as_str()).collect();
        assert!(top2_ids.contains(&"d1"));
        assert!(top2_ids.contains(&"d2"));
    }

    #[test]
    fn top_k_truncates() {
        let list = make_ranked(&["d1", "d2", "d3", "d4", "d5"], "vector");
        let fused = reciprocal_rank_fusion(&[list], None, 2);
        assert_eq!(fused.len(), 2);
    }

    #[test]
    fn empty_lists() {
        let fused = reciprocal_rank_fusion(&[], None, 10);
        assert!(fused.is_empty());
    }

    #[test]
    fn contributions_tracked() {
        let vector = make_ranked(&["d1"], "vector");
        let sparse = make_ranked(&["d1"], "sparse");

        let fused = reciprocal_rank_fusion(&[vector, sparse], None, 10);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].contributions.len(), 2);
    }
}
