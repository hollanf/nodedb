//! Spatial scatter-gather coordinator for cross-shard spatial queries.
//!
//! Same pattern as vector/timeseries distributed queries:
//! coordinator → VShardEnvelope per shard → collect responses → merge.

use super::merge::{ShardSpatialResult, SpatialResultMerger};
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Wire message for spatial scatter request payload (zerompk).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct SpatialScatterPayload {
    pub collection: String,
    pub field: String,
    pub predicate: String,
    /// Raw query geometry bytes (GeoJSON, passed through as-is).
    pub query_geometry: Vec<u8>,
    pub distance_meters: f64,
    pub limit: u32,
}

/// Scatter-gather coordinator for distributed spatial queries.
pub struct SpatialScatterGather {
    pub source_node: u64,
    pub shard_ids: Vec<u16>,
    merger: SpatialResultMerger,
}

impl SpatialScatterGather {
    pub fn new(source_node: u64, shard_ids: Vec<u16>) -> Self {
        let count = shard_ids.len();
        Self {
            source_node,
            shard_ids,
            merger: SpatialResultMerger::new(count),
        }
    }

    /// Build scatter envelopes for a spatial query.
    ///
    /// Each envelope contains the query geometry + predicate type + distance
    /// as JSON payload.
    pub fn build_scatter_envelopes(
        &self,
        collection: &str,
        field: &str,
        predicate: &str,
        query_geometry_json: &[u8],
        distance_meters: f64,
        limit: usize,
    ) -> Vec<(u16, VShardEnvelope)> {
        let msg = SpatialScatterPayload {
            collection: collection.to_string(),
            field: field.to_string(),
            predicate: predicate.to_string(),
            query_geometry: query_geometry_json.to_vec(),
            distance_meters,
            limit: limit as u32,
        };
        let payload_bytes =
            zerompk::to_msgpack_vec(&msg).expect("SpatialScatterPayload is always serializable");

        self.shard_ids
            .iter()
            .map(|&shard_id| {
                let env = VShardEnvelope::new(
                    VShardMessageType::SpatialScatterRequest,
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
    pub fn record_response(&mut self, result: &ShardSpatialResult) {
        self.merger.add_shard_result(result);
    }

    /// Whether all shards have responded.
    pub fn all_responded(&self) -> bool {
        self.merger.all_responded()
    }

    /// Merge results from all shards.
    pub fn merge_results(
        &mut self,
        limit: usize,
        sort_by_distance: bool,
    ) -> Vec<super::merge::SpatialHit> {
        self.merger.merge(limit, sort_by_distance)
    }

    pub fn response_count(&self) -> usize {
        self.merger.response_count()
    }
}

#[cfg(test)]
mod tests {
    use super::super::merge::{ShardSpatialResult, SpatialHit};
    use super::*;

    #[test]
    fn scatter_envelopes_built() {
        let coord = SpatialScatterGather::new(1, vec![0, 1, 2]);
        let query =
            serde_json::to_vec(&serde_json::json!({"type": "Point", "coordinates": [0.0, 0.0]}))
                .unwrap();
        let envs =
            coord.build_scatter_envelopes("buildings", "geom", "st_dwithin", &query, 1000.0, 100);
        assert_eq!(envs.len(), 3);
        for (shard_id, env) in &envs {
            assert_eq!(env.msg_type, VShardMessageType::SpatialScatterRequest);
            assert_eq!(env.vshard_id, *shard_id);
        }
    }

    #[test]
    fn collect_and_merge() {
        let mut coord = SpatialScatterGather::new(1, vec![0, 1]);
        coord.record_response(&ShardSpatialResult {
            shard_id: 0,
            hits: vec![SpatialHit {
                doc_id: "a".into(),
                shard_id: 0,
                distance_meters: 200.0,
            }],
            success: true,
            error: None,
        });
        coord.record_response(&ShardSpatialResult {
            shard_id: 1,
            hits: vec![SpatialHit {
                doc_id: "b".into(),
                shard_id: 1,
                distance_meters: 50.0,
            }],
            success: true,
            error: None,
        });
        assert!(coord.all_responded());

        let results = coord.merge_results(10, true);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, "b"); // nearer
    }
}
