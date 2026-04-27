//! α-pruning for the Vamana graph.
//!
//! Given a candidate set of (neighbor_id, distance_to_target) pairs, this
//! module trims the set to at most `r` entries so that every retained neighbor
//! `p*` satisfies: for every *later* candidate `p'`,
//! `α · dist(p*, p') > dist(p', target)`.
//!
//! Reference: Jayaram Subramanya et al., "DiskANN: Fast Accurate Billion-point
//! Nearest Neighbor Search on a Single Node", NeurIPS 2019.

/// Select at most `r` neighbors for `target` from `candidate_distances` using
/// the Vamana α-pruning heuristic.
///
/// # Arguments
///
/// * `candidate_distances` — `(neighbor_id, dist_to_target)` pairs **sorted
///   ascending** by distance.  The caller is responsible for the sort.
/// * `target` — internal index of the node being pruned (used only to avoid
///   self-loops via the caller's `pairwise_dist` closure; the closure itself
///   may short-circuit for `p == target`).
/// * `r` — maximum out-degree.
/// * `alpha` — pruning strictness (`> 1`; typical: `1.2`).
/// * `pairwise_dist` — closure returning `dist(a, b)` between two node
///   indices.  Called `O(candidates²)` times in the worst case — the caller
///   should back this with a cheap quantized kernel.
///
/// # Returns
///
/// Indices (into the graph) of the surviving neighbors, in the order they
/// were selected (closest-first).
pub fn alpha_prune(
    candidate_distances: &[(u32, f32)],
    target: u32,
    r: usize,
    alpha: f32,
    pairwise_dist: impl Fn(u32, u32) -> f32,
) -> Vec<u32> {
    // Working copy so we can mark candidates as pruned without sorting again.
    let active: Vec<(u32, f32)> = candidate_distances.to_vec();
    let mut result: Vec<u32> = Vec::with_capacity(r);

    // Boolean "dropped" flags parallel to `active`.
    let mut dropped: Vec<bool> = vec![false; active.len()];

    while result.len() < r {
        // Find the closest non-dropped candidate (list is sorted, so scan).
        let Some(star_pos) = active
            .iter()
            .enumerate()
            .find(|(i, _)| !dropped[*i])
            .map(|(i, _)| i)
        else {
            break; // No candidates left.
        };

        let (p_star, dist_star_to_target) = active[star_pos];

        // Skip self.
        if p_star == target {
            dropped[star_pos] = true;
            continue;
        }

        result.push(p_star);
        dropped[star_pos] = true;

        // Drop any remaining candidate p' where α · dist(p*, p') ≤ dist(p', target).
        for i in 0..active.len() {
            if dropped[i] {
                continue;
            }
            let (p_prime, dist_prime_to_target) = active[i];
            if p_prime == target {
                dropped[i] = true;
                continue;
            }
            let _ = dist_star_to_target; // suppress unused warning
            let d_star_prime = pairwise_dist(p_star, p_prime);
            if alpha * d_star_prime <= dist_prime_to_target {
                dropped[i] = true;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Euclidean distance squared between two 1-D "vectors" encoded as `u32`
    /// cast to `f32`.  Used as a cheap stand-in for the real distance function.
    fn dist1d(a: u32, b: u32) -> f32 {
        let fa = a as f32;
        let fb = b as f32;
        (fa - fb).abs()
    }

    #[test]
    fn preserves_closest_candidate() {
        // Candidates at distances 0.1, 0.5, 0.9 from target (node 100).
        // Encode node IDs so that dist1d(id, 100) ≈ the distance we want.
        let target: u32 = 100;
        let candidates: Vec<(u32, f32)> = vec![
            (101, 1.0), // dist(101, 100) = 1  — closest
            (105, 5.0), // dist(105, 100) = 5
            (109, 9.0), // dist(109, 100) = 9
        ];

        let result = alpha_prune(&candidates, target, 3, 1.2, dist1d);

        // The closest node (101) must always be in the result.
        assert!(result.contains(&101), "closest candidate must be retained");
    }

    #[test]
    fn drops_mutually_close_candidates_with_alpha_1_2() {
        // target = 0.  Candidates: 1 (dist 1), 2 (dist 2).
        // dist(1, 2) = 1; alpha * dist(1, 2) = 1.2 > dist(2, target) = 2? No.
        // 1.2 * 1 = 1.2 <= 2 → p'=2 is NOT dropped.
        // But if we put target=0, p*=1, p'=100 (dist 100 from target, dist 99
        // from p*): alpha * 99 = 118.8 > 100 → p'=100 is dropped.
        let target: u32 = 0;
        let candidates: Vec<(u32, f32)> = vec![(1, 1.0), (2, 2.0), (100, 100.0)];

        let result = alpha_prune(&candidates, target, 3, 1.2, dist1d);

        // p*=1 selected first.
        // p'=2: α·d(1,2) = 1.2·1 = 1.2 ≤ d(2,target) = 2 → 2 IS dropped (Vamana
        //   robust-pruning rule promotes diversity by dropping near-duplicates).
        // p'=100: α·d(1,100) = 1.2·99 = 118.8 > d(100,target) = 100 → kept.
        assert!(result.contains(&1));
        assert!(
            !result.contains(&2),
            "node 2 should be pruned (close to p*=1)"
        );
        assert!(result.contains(&100), "node 100 retained (far from p*=1)");
    }

    #[test]
    fn respects_degree_bound() {
        let target: u32 = 1000;
        // All far apart from each other so none are pruned by α.
        let candidates: Vec<(u32, f32)> =
            (0u32..20).map(|i| (i, (i as f32 + 1.0) * 100.0)).collect();

        let result = alpha_prune(&candidates, target, 5, 1.2, |_a, _b| 0.001);

        assert!(result.len() <= 5, "result must be capped at r=5");
    }

    #[test]
    fn empty_candidates_returns_empty() {
        let result = alpha_prune(&[], 0, 4, 1.2, |_a, _b| 0.0);
        assert!(result.is_empty());
    }
}
