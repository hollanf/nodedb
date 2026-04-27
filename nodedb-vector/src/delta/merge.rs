//! Query-time merge of main HNSW and delta result sets.
//!
//! Filters tombstones from both lists, tags each result with its origin,
//! deduplicates by id (delta wins), and returns the top-k sorted ascending
//! by distance.

use std::collections::{HashMap, HashSet};

/// A single result in the merged candidate set.
#[derive(Debug, Clone, PartialEq)]
pub struct MergedResult {
    /// Vector id.
    pub id: u32,
    /// Distance from the query vector.
    pub distance: f32,
    /// `true` if this result came from the delta index.
    pub from_delta: bool,
}

/// Merge `main` and `delta` result lists into a single sorted top-k list.
///
/// Rules:
/// - Any id present in `tombstones` is excluded.
/// - If the same id appears in both lists, the delta entry wins (its distance
///   and `from_delta = true` are used).
/// - Output is sorted ascending by distance and truncated to `k`.
pub fn merge_results(
    main: Vec<(u32, f32)>,
    delta: Vec<(u32, f32)>,
    tombstones: &HashSet<u32>,
    k: usize,
) -> Vec<MergedResult> {
    if k == 0 {
        return Vec::new();
    }

    // Collect delta entries first (they win on collision).
    let mut by_id: HashMap<u32, MergedResult> = HashMap::new();

    for (id, dist) in delta {
        if tombstones.contains(&id) {
            continue;
        }
        by_id.insert(
            id,
            MergedResult {
                id,
                distance: dist,
                from_delta: true,
            },
        );
    }

    // Main results only inserted when id not already present (delta wins).
    for (id, dist) in main {
        if tombstones.contains(&id) {
            continue;
        }
        by_id.entry(id).or_insert(MergedResult {
            id,
            distance: dist,
            from_delta: false,
        });
    }

    let mut merged: Vec<MergedResult> = by_id.into_values().collect();

    // Partial sort to get top-k cheaply.
    if k < merged.len() {
        merged.select_nth_unstable_by(k, |a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(k);
    }

    merged.sort_unstable_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_beats_main_wins_collision() {
        let main = vec![(1, 1.0), (2, 2.0)];
        let delta = vec![(3, 0.5), (1, 0.9)]; // id=1 in both; delta wins
        let tombstones = HashSet::new();
        let results = merge_results(main, delta, &tombstones, 3);

        // Top-3: 3@0.5, 1@0.9, 2@2.0
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 3);
        assert!(results[0].from_delta);
        assert_eq!(results[1].id, 1);
        assert!(results[1].from_delta);
        assert!((results[1].distance - 0.9).abs() < 1e-6);
        assert_eq!(results[2].id, 2);
        assert!(!results[2].from_delta);
    }

    #[test]
    fn top_two_delta_first() {
        // Spec example: main {1:1.0, 2:2.0}, delta {3:0.5} → top-2 = [3,1]
        let main = vec![(1, 1.0f32), (2, 2.0f32)];
        let delta = vec![(3, 0.5f32)];
        let tombstones = HashSet::new();
        let results = merge_results(main, delta, &tombstones, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 3);
        assert_eq!(results[1].id, 1);
    }

    #[test]
    fn tombstone_excludes_from_both() {
        let main = vec![(1, 1.0f32), (2, 2.0f32)];
        let delta = vec![(3, 0.5f32)];
        let mut tombstones = HashSet::new();
        tombstones.insert(2u32);
        let results = merge_results(main, delta, &tombstones, 10);
        assert!(results.iter().all(|r| r.id != 2));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn empty_inputs_returns_empty() {
        let results = merge_results(vec![], vec![], &HashSet::new(), 10);
        assert!(results.is_empty());
    }

    #[test]
    fn k_zero_returns_empty() {
        let main = vec![(1, 0.5f32)];
        let results = merge_results(main, vec![], &HashSet::new(), 0);
        assert!(results.is_empty());
    }
}
