//! Reciprocal Rank Fusion (RRF) for hybrid vector + FTS / vector + graph.
//!
//! Each ranked list contributes `1 / (k + rank)` per document ID, where `k`
//! is a smoothing constant (commonly 60).  Scores are summed across lists and
//! the resulting aggregated scores are returned sorted descending.

use std::collections::HashMap;

/// A single entry in a ranked result list.
#[derive(Debug, Clone)]
pub struct RankedResult {
    /// Internal document / vector ID.
    pub id: u32,
    /// 1-based rank within the source list.
    pub rank: u32,
    /// Raw score from the source retriever (e.g. BM25 score, cosine similarity).
    /// Not used by RRF itself but preserved for downstream reranking.
    pub raw_score: f32,
}

/// Tuning options for Reciprocal Rank Fusion.
pub struct RrfOptions {
    /// RRF smoothing constant.  The standard value is 60 (Cormack & Clarke, 2009).
    /// Larger values flatten the contribution curve; smaller values amplify top ranks.
    pub k: f32,
}

impl Default for RrfOptions {
    fn default() -> Self {
        Self { k: 60.0 }
    }
}

/// Fuse multiple ranked result lists using Reciprocal Rank Fusion.
///
/// For each document that appears in any list, the fused score is:
/// ```text
/// fused(id) = Σ_lists  1 / (k + rank(id, list))
/// ```
/// Documents absent from a list contribute nothing for that list.
///
/// Returns up to `top_k` `(id, fused_score)` pairs sorted by descending score.
/// If `top_k` is 0 or larger than the total number of unique IDs, all IDs are
/// returned.
pub fn rrf_fuse(
    lists: &[Vec<RankedResult>],
    options: &RrfOptions,
    top_k: usize,
) -> Vec<(u32, f32)> {
    // Accumulate fused scores keyed by document ID.
    let mut scores: HashMap<u32, f32> = HashMap::new();

    for list in lists {
        for entry in list {
            let contribution = 1.0 / (options.k + entry.rank as f32);
            *scores.entry(entry.id).or_insert(0.0) += contribution;
        }
    }

    // Collect and sort descending by score, then ascending by id for tie-breaking.
    let mut ranked: Vec<(u32, f32)> = scores.into_iter().collect();
    ranked.sort_unstable_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    if top_k > 0 {
        ranked.truncate(top_k);
    }

    ranked
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f32, b: f32, eps: f32) -> bool {
        (a - b).abs() < eps
    }

    #[test]
    fn two_lists_top_ranked_in_both() {
        // ID=5 is rank 1 in both lists → fused = 2 / (60 + 1) ≈ 0.032786
        let list_a = vec![
            RankedResult {
                id: 5,
                rank: 1,
                raw_score: 0.9,
            },
            RankedResult {
                id: 3,
                rank: 2,
                raw_score: 0.7,
            },
        ];
        let list_b = vec![
            RankedResult {
                id: 5,
                rank: 1,
                raw_score: 0.85,
            },
            RankedResult {
                id: 7,
                rank: 2,
                raw_score: 0.6,
            },
        ];

        let opts = RrfOptions::default();
        let result = rrf_fuse(&[list_a, list_b], &opts, 10);

        // ID=5 must be first.
        assert_eq!(result[0].0, 5);
        let expected = 2.0 / 61.0;
        assert!(
            approx_eq(result[0].1, expected, 1e-5),
            "expected ≈{expected:.6}, got {:.6}",
            result[0].1
        );
    }

    #[test]
    fn top_k_limits_output() {
        let list: Vec<RankedResult> = (1..=20)
            .map(|i| RankedResult {
                id: i,
                rank: i,
                raw_score: 1.0 / i as f32,
            })
            .collect();

        let opts = RrfOptions::default();
        let result = rrf_fuse(&[list], &opts, 5);

        assert_eq!(result.len(), 5);
    }

    #[test]
    fn empty_input_lists_returns_empty() {
        let opts = RrfOptions::default();
        let result = rrf_fuse(&[], &opts, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn empty_individual_lists_ignored() {
        let list_a: Vec<RankedResult> = vec![];
        let list_b = vec![RankedResult {
            id: 1,
            rank: 1,
            raw_score: 1.0,
        }];

        let opts = RrfOptions::default();
        let result = rrf_fuse(&[list_a, list_b], &opts, 10);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 1);
    }
}
