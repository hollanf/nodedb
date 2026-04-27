//! PLAID-style centroid-based candidate pruning for multi-vector search.
//!
//! Builds K-means centroids over all document vectors.  Each document is
//! encoded as a sorted bag of centroid IDs.  At query time the query's
//! centroid bag is computed and only documents whose centroid bag overlaps
//! the query bag are returned as candidates.
//!
//! Reference: Santhanam et al., "PLAID: An Efficient Engine for Late
//! Interaction Retrieval", CIKM 2022.

use std::collections::{HashMap, HashSet};

use crate::distance::scalar::scalar_distance;
use nodedb_types::vector_distance::DistanceMetric;

use super::storage::MultiVectorStore;

// ---------------------------------------------------------------------------
// Internal Lloyd's K-means (tiny, self-contained)
// ---------------------------------------------------------------------------

/// Assign each vector to its nearest centroid index.
fn assign(vectors: &[Vec<f32>], centroids: &[Vec<f32>]) -> Vec<usize> {
    vectors
        .iter()
        .map(|v| {
            centroids
                .iter()
                .enumerate()
                .map(|(i, c)| (i, scalar_distance(v, c, DistanceMetric::L2)))
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(i, _)| i)
                .unwrap_or(0)
        })
        .collect()
}

/// Recompute centroids as the mean of their assigned vectors.
fn recompute(
    vectors: &[Vec<f32>],
    assignments: &[usize],
    num_centroids: usize,
    dim: usize,
) -> Vec<Vec<f32>> {
    let mut sums = vec![vec![0.0f32; dim]; num_centroids];
    let mut counts = vec![0usize; num_centroids];

    for (v, &c) in vectors.iter().zip(assignments.iter()) {
        for (s, x) in sums[c].iter_mut().zip(v.iter()) {
            *s += x;
        }
        counts[c] += 1;
    }

    sums.iter_mut().zip(counts.iter()).for_each(|(s, &n)| {
        if n > 0 {
            s.iter_mut().for_each(|x| *x /= n as f32);
        }
    });

    sums
}

/// Run Lloyd's K-means with a deterministic (LCG) seed.
///
/// Returns `num_centroids` centroids of dimension `dim`.
fn kmeans(
    vectors: &[Vec<f32>],
    num_centroids: usize,
    iters: usize,
    seed: u64,
    dim: usize,
) -> Vec<Vec<f32>> {
    if vectors.is_empty() || num_centroids == 0 {
        return Vec::new();
    }

    let k = num_centroids.min(vectors.len());

    // Deterministic initialisation: pick k vectors by LCG stride.
    let mut state = seed.wrapping_add(1);
    let lcg_next = |s: &mut u64| -> u64 {
        *s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *s
    };

    let mut centroids: Vec<Vec<f32>> = (0..k)
        .map(|_| {
            let idx = (lcg_next(&mut state) as usize) % vectors.len();
            vectors[idx].clone()
        })
        .collect();

    // Lloyd iterations.
    for _ in 0..iters {
        let assignments = assign(vectors, &centroids);
        let new_centroids = recompute(vectors, &assignments, k, dim);
        centroids = new_centroids;
    }

    centroids
}

// ---------------------------------------------------------------------------
// PlaidPruner
// ---------------------------------------------------------------------------

/// PLAID centroid-based candidate pruner.
///
/// After `train`, call `candidates` at query time to get the set of document
/// IDs whose centroid bag overlaps the query's centroid bag.
pub struct PlaidPruner {
    pub centroids: Vec<Vec<f32>>,
    /// Sorted list of centroid IDs for each document.
    doc_centroids: HashMap<u32, Vec<u16>>,
}

impl PlaidPruner {
    /// Train the pruner from a `MultiVectorStore`.
    ///
    /// * `num_centroids` — number of K-means clusters.
    /// * `kmeans_iters` — Lloyd iterations.
    /// * `seed` — deterministic seed for centroid initialisation.
    pub fn train(
        store: &MultiVectorStore,
        num_centroids: u16,
        kmeans_iters: usize,
        seed: u64,
    ) -> Self {
        let dim = store.dim;
        let nc = num_centroids as usize;

        // Collect all document vectors for K-means training.
        let all_vectors: Vec<Vec<f32>> = store
            .iter()
            .flat_map(|doc| doc.vectors.iter().cloned())
            .collect();

        let centroids = kmeans(&all_vectors, nc, kmeans_iters, seed, dim);

        // Encode each document as a sorted, deduplicated bag of centroid IDs.
        let doc_centroids: HashMap<u32, Vec<u16>> = store
            .iter()
            .map(|doc| {
                let mut ids: Vec<u16> = doc
                    .vectors
                    .iter()
                    .map(|v| {
                        centroids
                            .iter()
                            .enumerate()
                            .map(|(i, c)| (i as u16, scalar_distance(v, c, DistanceMetric::L2)))
                            .min_by(|a, b| {
                                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                            })
                            .map(|(i, _)| i)
                            .unwrap_or(0)
                    })
                    .collect();
                ids.sort_unstable();
                ids.dedup();
                (doc.doc_id, ids)
            })
            .collect();

        Self {
            centroids,
            doc_centroids,
        }
    }

    /// Return candidate doc IDs whose centroid bag overlaps the query's
    /// centroid bag.
    ///
    /// The query centroid bag is the set of nearest centroids for each query
    /// vector.
    pub fn candidates(&self, query: &[Vec<f32>]) -> Vec<u32> {
        if self.centroids.is_empty() || query.is_empty() {
            return Vec::new();
        }

        // Build query centroid bag.
        let query_bag: HashSet<u16> = query
            .iter()
            .filter_map(|v| {
                self.centroids
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (i as u16, scalar_distance(v, c, DistanceMetric::L2)))
                    .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(id, _)| id)
            })
            .collect();

        // Collect docs that share at least one centroid with the query.
        self.doc_centroids
            .iter()
            .filter(|(_, doc_ids)| doc_ids.iter().any(|id| query_bag.contains(id)))
            .map(|(&doc_id, _)| doc_id)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multivec::storage::{MultiVecMode, MultiVectorDoc, MultiVectorStore};

    fn build_store() -> MultiVectorStore {
        let mut store = MultiVectorStore::new(2, MultiVecMode::PerToken);

        // Three well-separated clusters of documents.
        // Cluster A: docs 0–2 near (0,0).
        // Cluster B: docs 3–5 near (10,0).
        // Cluster C: docs 6–8 near (0,10).
        for i in 0u32..3 {
            store
                .insert(MultiVectorDoc {
                    doc_id: i,
                    vectors: vec![vec![i as f32 * 0.1, i as f32 * 0.1]],
                })
                .unwrap();
        }
        for i in 3u32..6 {
            store
                .insert(MultiVectorDoc {
                    doc_id: i,
                    vectors: vec![vec![10.0 + i as f32 * 0.1, 0.0]],
                })
                .unwrap();
        }
        for i in 6u32..9 {
            store
                .insert(MultiVectorDoc {
                    doc_id: i,
                    vectors: vec![vec![0.0, 10.0 + i as f32 * 0.1]],
                })
                .unwrap();
        }
        store
    }

    #[test]
    fn train_produces_correct_centroid_count() {
        let store = build_store();
        let pruner = PlaidPruner::train(&store, 3, 10, 42);
        assert_eq!(pruner.centroids.len(), 3);
    }

    #[test]
    fn centroids_have_correct_dim() {
        let store = build_store();
        let pruner = PlaidPruner::train(&store, 3, 10, 42);
        for c in &pruner.centroids {
            assert_eq!(c.len(), 2);
        }
    }

    #[test]
    fn candidates_non_empty_for_matching_query() {
        let store = build_store();
        let pruner = PlaidPruner::train(&store, 3, 10, 42);

        // A query near cluster A should return at least some candidates.
        let query = vec![vec![0.0f32, 0.0f32]];
        let cands = pruner.candidates(&query);
        assert!(!cands.is_empty(), "expected at least one candidate");
    }

    #[test]
    fn candidates_empty_when_no_centroids() {
        // An empty store produces a pruner with no centroids.
        let store = MultiVectorStore::new(2, MultiVecMode::PerToken);
        let pruner = PlaidPruner::train(&store, 3, 5, 1);
        let query = vec![vec![0.0f32, 0.0f32]];
        assert!(pruner.candidates(&query).is_empty());
    }

    #[test]
    fn candidates_cover_input_range() {
        // After training, the set of all-doc candidates (using multiple query
        // vectors spanning the whole space) should cover all 9 documents.
        let store = build_store();
        let pruner = PlaidPruner::train(&store, 3, 15, 7);
        let query = vec![
            vec![0.0f32, 0.0f32],
            vec![10.0f32, 0.0f32],
            vec![0.0f32, 10.0f32],
        ];
        let mut cands = pruner.candidates(&query);
        cands.sort_unstable();
        cands.dedup();
        assert_eq!(cands.len(), 9, "all docs should be candidates: {:?}", cands);
    }
}
