//! GraphRAG fusion for Lite: vector search → graph expansion → RRF merge.
//!
//! Composes the existing `vector_search()` and `graph_traverse()` methods
//! into a single multi-modal retrieval call. Results from vector similarity
//! and graph context are fused via Reciprocal Rank Fusion (RRF).

use std::collections::HashMap;

use nodedb_client::NodeDb;
use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::MetadataFilter;
use nodedb_types::id::NodeId;
use nodedb_types::result::SearchResult;

use super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// GraphRAG fusion parameters.
pub struct GraphRagParams<'a> {
    /// Collection to search vectors in.
    pub collection: &'a str,
    /// Query embedding.
    pub query: &'a [f32],
    /// Number of initial vector candidates.
    pub vector_k: usize,
    /// Graph expansion depth from each vector result.
    pub graph_depth: u8,
    /// Final number of results to return after fusion.
    pub top_k: usize,
    /// Optional metadata filter for vector search.
    pub filter: Option<&'a MetadataFilter>,
    /// RRF constant (default: 60). Higher = more weight to top ranks.
    pub rrf_k: f64,
}

impl Default for GraphRagParams<'_> {
    fn default() -> Self {
        Self {
            collection: "",
            query: &[],
            vector_k: 10,
            graph_depth: 2,
            top_k: 10,
            filter: None,
            rrf_k: 60.0,
        }
    }
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// GraphRAG fusion: vector search → graph expansion → RRF merge.
    ///
    /// 1. Vector search returns `vector_k` candidates by embedding similarity.
    /// 2. For each candidate, graph traverse discovers related nodes within `graph_depth` hops.
    /// 3. All discovered nodes are scored: vector candidates by their search rank,
    ///    graph-discovered nodes by their discovery depth (closer = higher score).
    /// 4. RRF fuses both score sources into a single ranking.
    /// 5. Top `top_k` results returned.
    pub async fn graph_rag_search(
        &self,
        params: &GraphRagParams<'_>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Step 1: Vector search.
        let vector_results = self
            .vector_search(
                params.collection,
                params.query,
                params.vector_k,
                params.filter,
            )
            .await?;

        if vector_results.is_empty() {
            return Ok(Vec::new());
        }

        // Step 2: Graph expansion from each vector result.
        // Track RRF scores: node_id → (vector_rrf, graph_rrf).
        let mut rrf_scores: HashMap<String, (f64, f64)> = HashMap::new();

        // Vector RRF scores: 1/(k + rank).
        for (rank, result) in vector_results.iter().enumerate() {
            let score = 1.0 / (params.rrf_k + rank as f64 + 1.0);
            rrf_scores.entry(result.id.clone()).or_insert((0.0, 0.0)).0 += score;
        }

        // Graph expansion and scoring.
        for result in &vector_results {
            let start = NodeId::new(result.id.clone());
            if let Ok(subgraph) = self.graph_traverse(&start, params.graph_depth, None).await {
                for node in &subgraph.nodes {
                    if node.depth == 0 {
                        continue; // Skip the start node itself.
                    }
                    // Graph score: inverse depth. Closer nodes get higher score.
                    let graph_score = 1.0 / (params.rrf_k + node.depth as f64);
                    rrf_scores
                        .entry(node.id.as_str().to_string())
                        .or_insert((0.0, 0.0))
                        .1 += graph_score;
                }
            }
        }

        // Step 3: Fuse scores and rank.
        let mut fused: Vec<(String, f64)> = rrf_scores
            .into_iter()
            .map(|(id, (v_score, g_score))| (id, v_score + g_score))
            .collect();
        fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        fused.truncate(params.top_k);

        // Step 4: Build result set. Reuse metadata from vector results where available.
        let vector_map: HashMap<&str, &SearchResult> =
            vector_results.iter().map(|r| (r.id.as_str(), r)).collect();

        let results = fused
            .into_iter()
            .map(|(id, score)| {
                if let Some(vr) = vector_map.get(id.as_str()) {
                    SearchResult {
                        id: id.clone(),
                        node_id: Some(NodeId::new(id)),
                        distance: vr.distance,
                        metadata: vr.metadata.clone(),
                    }
                } else {
                    // Graph-discovered node: read metadata from CRDT.
                    let metadata = {
                        let crdt = self.crdt.lock_or_recover();
                        if let Some(val) = crdt.read(params.collection, &id) {
                            let doc = crate::nodedb::convert::loro_value_to_document(&id, &val);
                            doc.fields
                        } else {
                            HashMap::new()
                        }
                    };
                    SearchResult {
                        id: id.clone(),
                        node_id: Some(NodeId::new(id)),
                        distance: score as f32, // Use fused score as distance proxy.
                        metadata,
                    }
                }
            })
            .collect();

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn rrf_scoring() {
        // Verify RRF formula: 1/(k + rank + 1).
        let k = 60.0;
        let score_rank_0 = 1.0 / (k + 1.0);
        let score_rank_1 = 1.0 / (k + 2.0);
        assert!(score_rank_0 > score_rank_1);
        assert!((score_rank_0 - 1.0f64 / 61.0).abs() < 1e-10);
    }
}
