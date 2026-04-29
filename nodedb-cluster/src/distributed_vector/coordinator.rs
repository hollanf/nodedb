//! Vector scatter-gather coordinator for cross-shard k-NN search.
//!
//! Same pattern as graph BSP and timeseries scatter-gather:
//! coordinator → VShardEnvelope per shard → collect responses → merge.

use super::merge::{ShardSearchResult, VectorHit, VectorMerger};
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Wire message for vector scatter request payload (zerompk).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct VectorScatterPayload {
    pub collection: String,
    pub query_vector: Vec<f32>,
    pub top_k: u32,
    pub ef_search: u32,
    pub has_filter: bool,
}

/// Scatter-gather coordinator for distributed k-NN vector search.
pub struct VectorScatterGather {
    /// Source node ID (this coordinator's node).
    pub source_node: u64,
    /// Target shard IDs to fan out to.
    pub shard_ids: Vec<u32>,
    /// Merger collecting shard responses.
    merger: VectorMerger,
}

impl VectorScatterGather {
    pub fn new(source_node: u64, shard_ids: Vec<u32>) -> Self {
        let count = shard_ids.len();
        Self {
            source_node,
            shard_ids,
            merger: VectorMerger::new(count),
        }
    }

    /// Build scatter envelopes for a k-NN search query.
    ///
    /// Returns one `VShardEnvelope` per shard. Each contains the query
    /// vector + parameters as JSON payload.
    pub fn build_scatter_envelopes(
        &self,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<&[u8]>,
    ) -> Vec<(u32, VShardEnvelope)> {
        let msg = VectorScatterPayload {
            collection: collection.to_string(),
            query_vector: query_vector.to_vec(),
            top_k: top_k as u32,
            ef_search: ef_search as u32,
            has_filter: filter_bitmap.is_some(),
        };
        let mut payload_bytes =
            zerompk::to_msgpack_vec(&msg).expect("VectorScatterPayload is always serializable");

        // Append filter bitmap as raw bytes after JSON (length-prefixed).
        if let Some(bitmap) = filter_bitmap {
            payload_bytes.extend_from_slice(&(bitmap.len() as u32).to_le_bytes());
            payload_bytes.extend_from_slice(bitmap);
        }

        self.shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::VectorScatterRequest,
                    self.source_node,
                    0, // target_node resolved by routing table
                    shard_id,
                    payload_bytes.clone(),
                );
                (shard_id, env)
            })
            .collect()
    }

    /// Record a shard's response.
    pub fn record_response(&mut self, result: &ShardSearchResult) {
        self.merger.add_shard_result(result);
    }

    /// Whether all shards have responded.
    pub fn all_responded(&self) -> bool {
        self.merger.all_responded()
    }

    /// Merge all shard results and return the global top-K.
    pub fn merge_top_k(&mut self, top_k: usize) -> Vec<VectorHit> {
        self.merger.top_k(top_k)
    }

    /// Number of shards that have responded.
    pub fn response_count(&self) -> usize {
        self.merger.response_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_envelopes_built() {
        let coord = VectorScatterGather::new(1, vec![0, 1, 2]);
        let query = vec![0.1f32, 0.2, 0.3];
        let envs = coord.build_scatter_envelopes("embeddings", &query, 10, 100, None);
        assert_eq!(envs.len(), 3);
        for (shard_id, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::VectorScatterRequest);
            assert_eq!(env.vshard_id, *shard_id);
            assert!(!env.payload.is_empty());
        }
    }

    #[test]
    fn scatter_with_filter() {
        let coord = VectorScatterGather::new(1, vec![0, 1]);
        let query = vec![1.0f32; 32];
        let filter = vec![0xFF_u8; 128];
        let envs = coord.build_scatter_envelopes("col", &query, 5, 50, Some(&filter));
        assert_eq!(envs.len(), 2);
        // Payload should be larger than without filter.
        let no_filter = coord.build_scatter_envelopes("col", &query, 5, 50, None);
        assert!(envs[0].1.payload.len() > no_filter[0].1.payload.len());
    }

    #[test]
    fn collect_and_merge() {
        let mut coord = VectorScatterGather::new(1, vec![0, 1]);
        assert!(!coord.all_responded());

        coord.record_response(&ShardSearchResult {
            shard_id: 0,
            hits: vec![
                VectorHit {
                    vector_id: 1,
                    distance: 0.1,
                    shard_id: 0,
                    doc_id: None,
                },
                VectorHit {
                    vector_id: 2,
                    distance: 0.5,
                    shard_id: 0,
                    doc_id: None,
                },
            ],
            success: true,
            error: None,
        });
        assert!(!coord.all_responded());

        coord.record_response(&ShardSearchResult {
            shard_id: 1,
            hits: vec![
                VectorHit {
                    vector_id: 10,
                    distance: 0.05,
                    shard_id: 1,
                    doc_id: None,
                },
                VectorHit {
                    vector_id: 11,
                    distance: 0.3,
                    shard_id: 1,
                    doc_id: None,
                },
            ],
            success: true,
            error: None,
        });
        assert!(coord.all_responded());

        let top2 = coord.merge_top_k(2);
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].vector_id, 10); // distance 0.05
        assert_eq!(top2[1].vector_id, 1); // distance 0.1
    }
}
