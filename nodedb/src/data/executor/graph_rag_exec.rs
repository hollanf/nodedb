//! GraphRAG fusion execution handler for `CoreLoop`.
//!
//! A single query fuses semantic similarity with structural graph
//! relationships. Steps 1-3 execute entirely on the Data Plane within a single
//! SPSC request-response cycle when all nodes are shard-local.
//!
//! Pipeline:
//! 1. Vector engine returns top-K semantically similar nodes.
//! 2. Result node IDs feed into graph traversal as start nodes.
//! 3. Graph-expanded result set is scored by hop distance.
//! 4. RRF fuses vector_score and graph_score into unified ranking.
//! 5. Final top-N results are materialized.

use std::collections::{HashMap, HashSet, VecDeque};

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::engine::graph::edge_store::Direction;

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

impl CoreLoop {
    /// Execute a GraphRAG fusion query: vector search → graph expansion → RRF.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_rag_fusion(
        &self,
        task: &ExecutionTask,
        collection: &str,
        query_vector: &[f32],
        vector_top_k: usize,
        edge_label: &Option<String>,
        direction: Direction,
        expansion_depth: usize,
        final_top_k: usize,
        rrf_k: (f64, f64),
        max_visited: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            vector_top_k,
            expansion_depth,
            final_top_k,
            "graph rag fusion"
        );

        // Step 1: Vector search — top-K semantically similar nodes.
        let Some(index) = self.vector_indexes.get(collection) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if index.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }

        let ef = vector_top_k.saturating_mul(4).max(64);
        let vector_results = index.search(query_vector, vector_top_k, ef);

        if vector_results.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }

        // Build vector score map: node_id → (rank, distance).
        // HNSW returns sequential IDs; we use them as string node IDs for graph lookup.
        let mut vector_scores: HashMap<String, (usize, f32)> = HashMap::new();
        for (rank, result) in vector_results.iter().enumerate() {
            let node_id = result.id.to_string();
            vector_scores.insert(node_id, (rank, result.distance));
        }

        // Step 2: Graph expansion — BFS from vector result IDs.
        let start_ids: Vec<&str> = vector_scores.keys().map(String::as_str).collect();
        let (expanded_nodes, hop_distances) = self.bfs_with_distances(
            &start_ids,
            edge_label.as_deref(),
            direction,
            expansion_depth,
            max_visited,
        );

        // Step 3: Score by hop distance. Closer nodes get higher graph_score.
        // graph_score = 1.0 / (1 + hop_distance). Start nodes get hop_distance=0 → score=1.0.

        // Step 4: RRF fusion of vector_score and graph_score.
        let (vector_k, graph_k) = rrf_k;

        // Build ranked lists for RRF.
        // Vector ranking: original ranks from vector search.
        // Graph ranking: rank by hop distance (ascending), then alphabetical for ties.
        let mut graph_ranked: Vec<(&str, usize)> = expanded_nodes
            .iter()
            .map(|node| {
                let dist = hop_distances
                    .get(node.as_str())
                    .copied()
                    .unwrap_or(usize::MAX);
                (node.as_str(), dist)
            })
            .collect();
        graph_ranked.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));

        // Compute RRF scores.
        let mut rrf_scores: HashMap<&str, f64> = HashMap::new();

        // Vector contribution: only for nodes in the original vector results.
        for (node_id, (rank, _)) in &vector_scores {
            let contribution = 1.0 / (vector_k + *rank as f64 + 1.0);
            *rrf_scores.entry(node_id.as_str()).or_default() += contribution;
        }

        // Graph contribution: for all expanded nodes.
        for (graph_rank, (node_id, _)) in graph_ranked.iter().enumerate() {
            let contribution = 1.0 / (graph_k + graph_rank as f64 + 1.0);
            *rrf_scores.entry(node_id).or_default() += contribution;
        }

        // Step 5: Sort by RRF score descending, take final_top_k.
        let mut fused: Vec<_> = rrf_scores.into_iter().collect();
        fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        fused.truncate(final_top_k);

        // Build response payload.
        let results: Vec<serde_json::Value> = fused
            .iter()
            .map(|(node_id, rrf_score)| {
                let mut entry = serde_json::json!({
                    "node_id": node_id,
                    "rrf_score": rrf_score,
                });
                if let Some((rank, distance)) = vector_scores.get(*node_id) {
                    entry["vector_rank"] = serde_json::json!(rank);
                    entry["vector_distance"] = serde_json::json!(distance);
                }
                if let Some(&hops) = hop_distances.get(*node_id) {
                    entry["hop_distance"] = serde_json::json!(hops);
                }
                entry
            })
            .collect();

        match serde_json::to_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "graph rag fusion serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// BFS traversal that also tracks hop distances from start nodes.
    ///
    /// Returns (all_reachable_nodes, hop_distances_map).
    fn bfs_with_distances(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> (Vec<String>, HashMap<String, usize>) {
        let mut visited: HashSet<String> = HashSet::new();
        let mut distances: HashMap<String, usize> = HashMap::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();

        for &node in start_nodes {
            let owned = node.to_string();
            if visited.insert(owned.clone()) {
                distances.insert(owned.clone(), 0);
                queue.push_back((owned, 0));
            }
        }

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= max_visited {
                continue;
            }

            let neighbors = self.csr.neighbors(&node, label_filter, direction);
            for (_, neighbor) in &neighbors {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor.clone());
                    distances.insert(neighbor.clone(), depth + 1);
                    queue.push_back((neighbor.clone(), depth + 1));
                }
            }
        }

        let nodes: Vec<String> = visited.into_iter().collect();
        (nodes, distances)
    }
}
