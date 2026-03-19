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
//!
//! # Memory Safety
//!
//! BFS expansion can produce massive intermediate result sets (supernodes
//! with 100K+ edges fanning out to millions of nodes). All allocations
//! are bounded by a per-query memory budget derived from the node count.
//! If the budget is exceeded, expansion stops early and results are marked
//! as truncated.

use std::collections::{HashMap, HashSet, VecDeque};

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::engine::graph::edge_store::Direction;

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

/// Maximum bytes a single GraphRAG BFS expansion may allocate for
/// intermediate node tracking (visited set + distance map + queue).
///
/// Estimated at ~128 bytes per node (String key ~40 bytes + HashSet entry
/// overhead + HashMap entry + VecDeque entry). At 256 KiB this allows
/// ~2,000 expanded nodes which is generous for most GraphRAG queries.
/// The `max_visited` parameter may impose a tighter limit.
const BFS_MEMORY_BUDGET_BYTES: usize = 256 * 1024;

/// Estimated per-node memory cost in the BFS working set.
///
/// Accounts for: String node ID (~40 bytes avg including heap allocation),
/// HashSet<String> entry overhead (~24 bytes), HashMap<String, usize>
/// entry overhead (~56 bytes), VecDeque<(String, usize)> entry (~48 bytes).
/// Total: ~168 bytes, rounded up to 192 for alignment and safety.
const BFS_BYTES_PER_NODE: usize = 192;

impl CoreLoop {
    /// Execute a GraphRAG fusion query: vector search → graph expansion → RRF.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_graph_rag_fusion(
        &self,
        task: &ExecutionTask,
        tenant_id: u32,
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
        let index_key = CoreLoop::vector_index_key(tenant_id, collection);
        let Some(index) = self.vector_indexes.get(&index_key) else {
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
        let (expanded_nodes, hop_distances, bfs_truncated) = self.bfs_with_distances(
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

        // Build response payload with metadata.
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

        let response_body = serde_json::json!({
            "results": results,
            "metadata": {
                "vector_candidates": vector_results.len(),
                "graph_expanded": expanded_nodes.len(),
                "truncated": bfs_truncated,
            }
        });

        match serde_json::to_vec(&response_body) {
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
    /// Returns `(all_reachable_nodes, hop_distances_map, truncated)`.
    ///
    /// Expansion is bounded by three limits, whichever triggers first:
    /// - `max_depth`: maximum BFS hop count from start nodes.
    /// - `max_visited`: maximum number of nodes to visit.
    /// - Memory budget: `BFS_MEMORY_BUDGET_BYTES` caps total working set
    ///   to prevent OOM from supernode fan-out.
    fn bfs_with_distances(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> (Vec<String>, HashMap<String, usize>, bool) {
        // Derive node limit from memory budget. The tighter of max_visited
        // and the budget-derived limit wins.
        let budget_node_limit = BFS_MEMORY_BUDGET_BYTES / BFS_BYTES_PER_NODE;
        let effective_limit = max_visited.min(budget_node_limit);

        let mut visited: HashSet<String> = HashSet::with_capacity(effective_limit.min(1024));
        let mut distances: HashMap<String, usize> =
            HashMap::with_capacity(effective_limit.min(1024));
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        let mut truncated = false;

        for &node in start_nodes {
            let owned = node.to_string();
            if visited.insert(owned.clone()) {
                distances.insert(owned.clone(), 0);
                queue.push_back((owned, 0));
            }
        }

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            if visited.len() >= effective_limit {
                truncated = true;
                break;
            }

            let neighbors = self.csr.neighbors(&node, label_filter, direction);
            for (_, neighbor) in &neighbors {
                if visited.len() >= effective_limit {
                    truncated = true;
                    break;
                }
                if !visited.contains(neighbor) {
                    visited.insert(neighbor.clone());
                    distances.insert(neighbor.clone(), depth + 1);
                    queue.push_back((neighbor.clone(), depth + 1));
                }
            }

            if truncated {
                break;
            }
        }

        if truncated {
            warn!(
                core = self.core_id,
                visited = visited.len(),
                limit = effective_limit,
                budget_limit = budget_node_limit,
                max_visited,
                "GraphRAG BFS truncated: memory budget or max_visited reached"
            );
        }

        let nodes: Vec<String> = visited.into_iter().collect();
        (nodes, distances, truncated)
    }
}
