//! Local selectivity estimation for NaviX adaptive-local filtered traversal.
//!
//! At each hop during HNSW traversal, NaviX computes the fraction of 1-hop
//! neighbors that pass the filter (local selectivity) and picks an expansion
//! heuristic accordingly.  This is the key difference from static ACORN-1,
//! which always expands to 2-hop regardless of local density.

use roaring::RoaringBitmap;

/// Compute the local selectivity at a graph node.
///
/// Returns the fraction of 1-hop neighbors that are present in `allowed`.
/// Range: `[0.0, 1.0]`.  An empty neighborhood returns `0.0`.
pub fn local_selectivity_at(node_neighbors: &[u32], allowed: &RoaringBitmap) -> f32 {
    if node_neighbors.is_empty() {
        return 0.0;
    }
    let matched = node_neighbors
        .iter()
        .filter(|&&n| allowed.contains(n))
        .count();
    matched as f32 / node_neighbors.len() as f32
}

/// Heuristic chosen for expanding a given hop based on local selectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NavixHeuristic {
    /// High selectivity (> 50%) — normal HNSW expansion; score every neighbor
    /// in `allowed` and add to the candidate heap.
    Standard,
    /// Medium-low selectivity (1% ≤ sel ≤ 50%) — score 1-hop neighbors, pick
    /// the single best (lowest distance), then expand that neighbor's 2-hop
    /// neighbors into the candidate heap.
    Directed,
    /// Very low selectivity (< 1%) — skip 1-hop scoring entirely; sample all
    /// 2-hop neighbors of every 1-hop neighbor and add those that are in
    /// `allowed` to the candidate heap directly.
    Blind,
}

/// Pick the expansion heuristic for a node given its local selectivity.
pub fn pick_heuristic(local_selectivity: f32) -> NavixHeuristic {
    if local_selectivity > 0.50 {
        NavixHeuristic::Standard
    } else if local_selectivity >= 0.01 {
        NavixHeuristic::Directed
    } else {
        NavixHeuristic::Blind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── pick_heuristic boundary tests ──────────────────────────────────────

    #[test]
    fn heuristic_zero() {
        assert_eq!(pick_heuristic(0.0), NavixHeuristic::Blind);
    }

    #[test]
    fn heuristic_below_directed_threshold() {
        assert_eq!(pick_heuristic(0.005), NavixHeuristic::Blind);
    }

    #[test]
    fn heuristic_at_directed_lower_boundary() {
        // 0.01 is inclusive in Directed range
        assert_eq!(pick_heuristic(0.01), NavixHeuristic::Directed);
    }

    #[test]
    fn heuristic_mid_directed() {
        assert_eq!(pick_heuristic(0.30), NavixHeuristic::Directed);
    }

    #[test]
    fn heuristic_at_standard_lower_boundary() {
        // 0.50 is still Directed (not strictly greater than 0.50)
        assert_eq!(pick_heuristic(0.50), NavixHeuristic::Directed);
    }

    #[test]
    fn heuristic_just_above_standard_boundary() {
        assert_eq!(pick_heuristic(0.51), NavixHeuristic::Standard);
    }

    #[test]
    fn heuristic_full_selectivity() {
        assert_eq!(pick_heuristic(1.0), NavixHeuristic::Standard);
    }

    // ── local_selectivity_at tests ─────────────────────────────────────────

    #[test]
    fn empty_neighbors_returns_zero() {
        let allowed = RoaringBitmap::new();
        assert_eq!(local_selectivity_at(&[], &allowed), 0.0);
    }

    #[test]
    fn all_matching_returns_one() {
        let mut allowed = RoaringBitmap::new();
        allowed.insert(0);
        allowed.insert(1);
        allowed.insert(2);
        let neighbors = [0u32, 1, 2];
        assert!((local_selectivity_at(&neighbors, &allowed) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn half_matching_returns_half() {
        let mut allowed = RoaringBitmap::new();
        allowed.insert(0);
        allowed.insert(2);
        let neighbors = [0u32, 1, 2, 3];
        assert!((local_selectivity_at(&neighbors, &allowed) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn none_matching_returns_zero() {
        let allowed = RoaringBitmap::new();
        let neighbors = [0u32, 1, 2, 3];
        assert_eq!(local_selectivity_at(&neighbors, &allowed), 0.0);
    }
}
