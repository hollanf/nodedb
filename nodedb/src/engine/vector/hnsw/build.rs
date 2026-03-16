use super::graph::{Candidate, HnswIndex, Node};
use super::search::search_layer;

impl HnswIndex {
    /// Insert a vector into the index.
    ///
    /// Implements the HNSW insert algorithm (Malkov & Yashunin, Algorithm 1):
    /// 1. Assign a random layer using the exponential distribution
    /// 2. Greedily descend from the entry point to the new node's layer + 1
    /// 3. At each layer from the node's layer down to 0, search for nearest
    ///    neighbors, select via the diversity heuristic, and add bidirectional edges
    /// 4. Prune over-connected nodes to maintain the M/M0 invariant
    ///
    /// Construction uses full-precision FP32 vectors.
    pub fn insert(&mut self, vector: Vec<f32>) {
        assert_eq!(
            vector.len(),
            self.dim,
            "vector dimension mismatch: expected {}, got {}",
            self.dim,
            vector.len()
        );

        let new_id = self.nodes.len() as u32;
        let new_layer = self.random_layer();

        // Create the node with empty neighbor lists for each layer.
        let node = Node {
            vector,
            neighbors: (0..=new_layer).map(|_| Vec::new()).collect(),
        };
        self.nodes.push(node);

        // First node: becomes the entry point.
        let Some(ep) = self.entry_point else {
            self.entry_point = Some(new_id);
            self.max_layer = new_layer;
            return;
        };

        let query = &self.nodes[new_id as usize].vector as *const Vec<f32>;
        // SAFETY: we need to borrow the query vector while mutating neighbor lists
        // of other nodes. The query vector itself is never mutated during insert.
        let query: &[f32] = unsafe { &*query };

        let mut current_ep = ep;

        // Phase 1: Greedy descent from top layer to new_layer + 1.
        // At each layer above the new node's layer, find the single closest node.
        if self.max_layer > new_layer {
            for layer in (new_layer + 1..=self.max_layer).rev() {
                let results = search_layer(self, query, current_ep, 1, layer, None);
                if let Some(nearest) = results.first() {
                    current_ep = nearest.id;
                }
            }
        }

        // Phase 2: Insert at each layer from min(new_layer, max_layer) down to 0.
        let insert_top = new_layer.min(self.max_layer);
        for layer in (0..=insert_top).rev() {
            let ef = self.params.ef_construction;
            let candidates = search_layer(self, query, current_ep, ef, layer, None);

            // Select neighbors using the diversity heuristic (Algorithm 4).
            let m = self.max_neighbors(layer);
            let selected = select_neighbors_heuristic(self, &candidates, m);

            // Set the new node's neighbors at this layer.
            self.nodes[new_id as usize].neighbors[layer] = selected.iter().map(|c| c.id).collect();

            // Add reverse edges (bidirectional connectivity).
            for neighbor in &selected {
                let nid = neighbor.id as usize;
                self.nodes[nid].neighbors[layer].push(new_id);

                // Prune if over-connected.
                if self.nodes[nid].neighbors[layer].len() > m {
                    let node_vec = &self.nodes[nid].vector as *const Vec<f32>;
                    // SAFETY: We hold &mut self but only mutate neighbors[layer],
                    // never the vector data. The pointer remains valid because
                    // self.nodes is not reallocated during insert (no push here).
                    let node_vec: &[f32] = unsafe { &*node_vec };
                    self.prune_neighbors(nid, layer, node_vec, m);
                }
            }

            // Use the closest found node as the entry point for the next layer down.
            if let Some(nearest) = candidates.first() {
                current_ep = nearest.id;
            }
        }

        // If the new node's layer exceeds the current max, promote it to entry point.
        if new_layer > self.max_layer {
            self.entry_point = Some(new_id);
            self.max_layer = new_layer;
        }
    }

    /// Prune a node's neighbor list at a given layer using the diversity heuristic.
    fn prune_neighbors(&mut self, node_idx: usize, layer: usize, node_vec: &[f32], m: usize) {
        let neighbor_ids: Vec<u32> = self.nodes[node_idx].neighbors[layer].clone();

        let mut candidates: Vec<Candidate> = neighbor_ids
            .iter()
            .map(|&nid| Candidate {
                id: nid,
                dist: self.dist_to_node(node_vec, nid),
            })
            .collect();
        candidates.sort_unstable_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap());

        let selected = select_neighbors_heuristic(self, &candidates, m);
        self.nodes[node_idx].neighbors[layer] = selected.iter().map(|c| c.id).collect();
    }
}

/// Heuristic neighbor selection (Malkov & Yashunin, Algorithm 4).
///
/// Selects up to `m` neighbors that are both close to `query` AND diverse
/// (not all clustered together). A candidate is kept only if it is closer
/// to `query` than to every already-selected neighbor. This produces a
/// graph with better long-range connectivity than simple closest selection.
fn select_neighbors_heuristic(
    index: &HnswIndex,
    candidates: &[Candidate],
    m: usize,
) -> Vec<Candidate> {
    // Callers guarantee candidates are sorted by distance ascending
    // (search_layer sorts its output; prune_neighbors pre-sorts before calling).
    let mut selected: Vec<Candidate> = Vec::with_capacity(m);

    for candidate in candidates {
        if selected.len() >= m {
            break;
        }

        // Check the diversity condition: candidate must be closer to query
        // than to any already-selected neighbor.
        let dist_to_query = candidate.dist;
        let is_diverse = selected.iter().all(|s| {
            let dist_to_selected = super::super::distance::distance(
                &index.nodes[candidate.id as usize].vector,
                &index.nodes[s.id as usize].vector,
                index.params.metric,
            );
            dist_to_query <= dist_to_selected
        });

        if is_diverse {
            selected.push(*candidate);
        }
    }

    // If the heuristic is too aggressive and we have fewer than m neighbors,
    // backfill with the closest remaining candidates.
    if selected.len() < m {
        let selected_ids: std::collections::HashSet<u32> = selected.iter().map(|c| c.id).collect();
        for candidate in candidates {
            if selected.len() >= m {
                break;
            }
            if !selected_ids.contains(&candidate.id) {
                selected.push(*candidate);
            }
        }
    }

    selected
}

#[cfg(test)]
mod tests {
    use super::super::graph::{HnswIndex, HnswParams};
    use crate::engine::vector::distance::DistanceMetric;

    fn make_index() -> HnswIndex {
        HnswIndex::with_seed(
            3,
            HnswParams {
                m: 4,
                m0: 8,
                ef_construction: 32,
                metric: DistanceMetric::L2,
            },
            12345,
        )
    }

    #[test]
    fn insert_single() {
        let mut idx = make_index();
        idx.insert(vec![1.0, 0.0, 0.0]);
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.entry_point, Some(0));
    }

    #[test]
    fn insert_many_maintains_invariants() {
        let mut idx = make_index();
        for i in 0..100 {
            let v = vec![(i as f32) * 0.1, (i as f32) * 0.2, (i as f32) * 0.3];
            idx.insert(v);
        }
        assert_eq!(idx.len(), 100);
        assert!(idx.entry_point.is_some());

        // Every node at layer 0 should have at most m0 neighbors.
        for node in &idx.nodes {
            assert!(
                node.neighbors[0].len() <= idx.params.m0,
                "layer 0 neighbor count {} exceeds m0={}",
                node.neighbors[0].len(),
                idx.params.m0,
            );
        }

        // Nodes at higher layers should have at most m neighbors.
        for node in &idx.nodes {
            for (layer, neighbors) in node.neighbors.iter().enumerate().skip(1) {
                assert!(
                    neighbors.len() <= idx.params.m,
                    "layer {layer} neighbor count {} exceeds m={}",
                    neighbors.len(),
                    idx.params.m,
                );
            }
        }
    }

    #[test]
    fn all_nodes_reachable_from_entry() {
        let mut idx = make_index();
        for i in 0..20 {
            idx.insert(vec![i as f32, 0.0, 0.0]);
        }

        // Every node should be reachable via search from the entry point.
        // This is a stronger property than bidirectionality (which pruning
        // can legitimately break for diversity).
        for target in 0..20u32 {
            let query = idx.get_vector(target).unwrap().to_vec();
            let results = idx.search(&query, 1, 32);
            assert_eq!(
                results[0].id, target,
                "node {target} not reachable via search"
            );
        }
    }

    #[test]
    fn layer_distribution_is_exponential() {
        let mut idx = HnswIndex::with_seed(
            2,
            HnswParams {
                m: 16,
                m0: 32,
                ef_construction: 100,
                metric: DistanceMetric::L2,
            },
            99,
        );
        for i in 0..1000 {
            idx.insert(vec![i as f32, (i as f32) * 0.5]);
        }

        // Count nodes per layer. Layer 0 should have all nodes.
        // Higher layers should have exponentially fewer.
        let max_l = idx.nodes.iter().map(|n| n.neighbors.len()).max().unwrap();
        let mut counts = vec![0usize; max_l];
        for node in &idx.nodes {
            for count in counts.iter_mut().take(node.neighbors.len()) {
                *count += 1;
            }
        }
        assert_eq!(counts[0], 1000);
        // Layer 1 should have roughly 1000/ln(16) ≈ 361, but it's stochastic.
        // Just check it's significantly fewer than layer 0.
        if counts.len() > 1 {
            assert!(counts[1] < 800, "layer 1 has too many nodes: {}", counts[1]);
        }
    }

    #[test]
    fn dimension_mismatch_panics() {
        let mut idx = make_index();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            idx.insert(vec![1.0, 2.0]); // dim=2 but index expects dim=3
        }));
        assert!(result.is_err());
    }
}
