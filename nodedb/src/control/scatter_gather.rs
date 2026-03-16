//! Scatter-gather coordinator for cross-shard graph traversals.
//!
//! Cross-shard hops are NOT handled by forwarding the traversal to
//! the remote shard. Instead, the Data Plane returns partial results (the set
//! of cross-shard edge targets) to the Control Plane, which batches and
//! dispatches them to the appropriate target cores.
//!
//! This keeps the Data Plane stateless per-request and avoids distributed
//! deadlocks from recursive cross-shard calls.
//!
//! ## Vectorized Scatter Envelopes
//!
//! The Data Plane MUST NOT emit one SPSC message per unresolved cross-shard edge.
//! Instead, for each hop level, cross-shard destinations are accumulated into a
//! single vectorized envelope grouped by target shard:
//! `{ shard_id -> [node_id, ...] }`.

use std::collections::{HashMap, HashSet};

use crate::engine::graph::traversal_options::{GraphResponseMeta, GraphTraversalOptions};
use crate::types::VShardId;

/// A batch of node IDs targeted at a specific shard.
///
/// Produced by the scatter phase when graph traversal discovers nodes
/// that live on a different shard than the current core.
#[derive(Debug, Clone)]
pub struct ScatterBatch {
    /// Target shard for this batch of node IDs.
    pub target_shard: VShardId,
    /// Node IDs that need to be explored on the target shard.
    pub node_ids: Vec<String>,
}

/// Vectorized scatter envelope for one hop level.
///
/// Groups all cross-shard destinations by target shard, preventing
/// scatter amplification.
#[derive(Debug, Clone, Default)]
pub struct ScatterEnvelope {
    /// Batches grouped by target shard.
    batches: HashMap<VShardId, Vec<String>>,
}

impl ScatterEnvelope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a node ID destined for a specific shard.
    pub fn add(&mut self, shard: VShardId, node_id: String) {
        self.batches.entry(shard).or_default().push(node_id);
    }

    /// Number of distinct shards in this envelope.
    pub fn shard_count(&self) -> usize {
        self.batches.len()
    }

    /// Consume into scatter batches.
    pub fn into_batches(self) -> Vec<ScatterBatch> {
        self.batches
            .into_iter()
            .map(|(shard, node_ids)| ScatterBatch {
                target_shard: shard,
                node_ids,
            })
            .collect()
    }

    /// Total number of node IDs across all shards.
    pub fn total_nodes(&self) -> usize {
        self.batches.values().map(|v| v.len()).sum()
    }

    /// Check if the envelope is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Result of applying adaptive fan-out limits to a scatter envelope.
#[derive(Debug)]
pub enum FanOutDecision {
    /// All batches can proceed. No limits hit.
    Proceed {
        batches: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
    /// Soft limit exceeded but continuing. Response annotated with warning.
    ProceedWithWarning {
        batches: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
    /// Hard limit exceeded. If fan_out_partial, return partial results.
    /// Otherwise, return FAN_OUT_EXCEEDED error.
    Exceeded {
        /// Batches that were dispatched before limit was hit (for partial mode).
        dispatched: Vec<ScatterBatch>,
        /// Batches that were skipped.
        skipped: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
}

/// Apply adaptive fan-out limits to a scatter envelope.
/// - Soft limit (default 12): query continues, response annotated with warning
/// - Hard limit (default 16): query terminates with FAN_OUT_EXCEEDED unless
///   fan_out_partial is true, in which case partial results are returned
pub fn apply_fan_out_limits(
    envelope: ScatterEnvelope,
    options: &GraphTraversalOptions,
) -> FanOutDecision {
    let shard_count = envelope.shard_count() as u16;

    if shard_count <= options.fan_out_soft {
        // Under soft limit — all clear.
        FanOutDecision::Proceed {
            batches: envelope.into_batches(),
            meta: GraphResponseMeta {
                shards_reached: shard_count,
                ..Default::default()
            },
        }
    } else if shard_count <= options.fan_out_hard {
        // Between soft and hard limit — proceed with warning.
        let batches = envelope.into_batches();
        let meta = GraphResponseMeta::with_warning(shard_count, 0, options.fan_out_hard);
        FanOutDecision::ProceedWithWarning { batches, meta }
    } else {
        // Exceeded hard limit.
        let mut all_batches = envelope.into_batches();
        let hard = options.fan_out_hard as usize;
        let skipped = all_batches.split_off(hard);
        let skipped_count = skipped.len() as u16;
        let dispatched_count = all_batches.len() as u16;

        let meta = if options.fan_out_partial {
            GraphResponseMeta::with_truncation(dispatched_count, skipped_count)
        } else {
            GraphResponseMeta {
                shards_reached: dispatched_count,
                shards_skipped: skipped_count,
                truncated: true,
                fan_out_warning: None,
                approximate: true,
            }
        };

        FanOutDecision::Exceeded {
            dispatched: all_batches,
            skipped,
            meta,
        }
    }
}

/// Merge partial traversal results from multiple shards.
///
/// Deduplicates node IDs and accumulates all discovered nodes.
pub fn merge_traversal_results(
    local_nodes: Vec<String>,
    shard_results: &[Vec<String>],
) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut merged = Vec::new();

    for node in local_nodes {
        if seen.insert(node.clone()) {
            merged.push(node);
        }
    }

    for result in shard_results {
        for node in result {
            if seen.insert(node.clone()) {
                merged.push(node.clone());
            }
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_envelope_grouping() {
        let mut env = ScatterEnvelope::new();
        env.add(VShardId::new(0), "a".into());
        env.add(VShardId::new(0), "b".into());
        env.add(VShardId::new(1), "c".into());

        assert_eq!(env.shard_count(), 2);
        assert_eq!(env.total_nodes(), 3);

        let batches = env.into_batches();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn fan_out_under_soft_limit() {
        let mut env = ScatterEnvelope::new();
        for i in 0..5u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let decision = apply_fan_out_limits(env, &GraphTraversalOptions::default());
        match decision {
            FanOutDecision::Proceed { batches, meta } => {
                assert_eq!(batches.len(), 5);
                assert!(meta.is_clean());
                assert_eq!(meta.shards_reached, 5);
            }
            _ => panic!("expected Proceed"),
        }
    }

    #[test]
    fn fan_out_between_soft_and_hard() {
        let mut env = ScatterEnvelope::new();
        for i in 0..14u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let decision = apply_fan_out_limits(env, &GraphTraversalOptions::default());
        match decision {
            FanOutDecision::ProceedWithWarning { batches, meta } => {
                assert_eq!(batches.len(), 14);
                assert!(!meta.is_clean());
                assert!(meta.approximate);
                assert_eq!(meta.fan_out_warning, Some("14/16".to_string()));
            }
            _ => panic!("expected ProceedWithWarning"),
        }
    }

    #[test]
    fn fan_out_exceeded_no_partial() {
        let mut env = ScatterEnvelope::new();
        for i in 0..20u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_partial: false,
            ..Default::default()
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched,
                skipped,
                meta,
            } => {
                assert_eq!(dispatched.len(), 16);
                assert_eq!(skipped.len(), 4);
                assert!(meta.truncated);
                assert_eq!(meta.shards_reached, 16);
                assert_eq!(meta.shards_skipped, 4);
            }
            _ => panic!("expected Exceeded"),
        }
    }

    #[test]
    fn fan_out_exceeded_with_partial() {
        let mut env = ScatterEnvelope::new();
        for i in 0..20u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_partial: true,
            ..Default::default()
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched, meta, ..
            } => {
                assert_eq!(dispatched.len(), 16);
                assert!(meta.truncated);
            }
            _ => panic!("expected Exceeded"),
        }
    }

    #[test]
    fn merge_deduplicates() {
        let local = vec!["a".into(), "b".into(), "c".into()];
        let shard1 = vec!["b".into(), "d".into()];
        let shard2 = vec!["c".into(), "e".into()];

        let merged = merge_traversal_results(local, &[shard1, shard2]);
        assert_eq!(merged.len(), 5);
        assert!(merged.contains(&"a".to_string()));
        assert!(merged.contains(&"d".to_string()));
        assert!(merged.contains(&"e".to_string()));
    }

    #[test]
    fn empty_envelope() {
        let env = ScatterEnvelope::new();
        assert!(env.is_empty());
        assert_eq!(env.shard_count(), 0);
        assert_eq!(env.total_nodes(), 0);
    }

    #[test]
    fn custom_limits() {
        let mut env = ScatterEnvelope::new();
        for i in 0..10u16 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_soft: 4,
            fan_out_hard: 8,
            fan_out_partial: true,
            max_visited: 100_000,
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched,
                skipped,
                ..
            } => {
                assert_eq!(dispatched.len(), 8);
                assert_eq!(skipped.len(), 2);
            }
            _ => panic!("expected Exceeded"),
        }
    }
}
