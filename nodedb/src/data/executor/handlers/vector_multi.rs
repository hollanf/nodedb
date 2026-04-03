//! Multi-vector document handlers: insert N vectors per doc, delete all,
//! and aggregated scoring search (MaxSim / AvgSim / SumSim).

use std::collections::HashMap;

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Insert multiple vectors for a single document into the HNSW index.
    ///
    /// All vectors share the same `doc_id` in the `doc_id_map` and are tracked
    /// in `multi_doc_map` for bulk deletion.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_multi_vector_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field_name: &str,
        doc_id: &str,
        vectors_flat: &[f32],
        count: usize,
        dim: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection, %field_name, %doc_id, count, dim,
            "multi-vector insert"
        );

        if count == 0 || dim == 0 {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    constraint: "multi-vector count and dim must be > 0".into(),
                },
            );
        }
        if vectors_flat.len() != count * dim {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    constraint: format!(
                        "data length mismatch: expected {} ({}×{}), got {}",
                        count * dim,
                        count,
                        dim,
                        vectors_flat.len()
                    ),
                },
            );
        }

        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);

        // Validate dimension compatibility before taking mutable reference.
        if let Some(existing) = self.vector_collections.get(&index_key)
            && existing.dim() != dim
        {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    constraint: format!(
                        "dimension mismatch: index has {}, got {dim}",
                        existing.dim()
                    ),
                },
            );
        }

        // Get or create the vector collection.
        let core_id = self.core_id;
        let params = self
            .vector_params
            .get(&index_key)
            .cloned()
            .unwrap_or_default();
        let coll = self
            .vector_collections
            .entry(index_key.clone())
            .or_insert_with(|| {
                debug!(
                    core = core_id,
                    dim, "creating vector collection for multi-vector"
                );
                crate::engine::vector::collection::VectorCollection::new(dim, params)
            });

        // Build vector slices from flat data.
        let vector_slices: Vec<&[f32]> = (0..count)
            .map(|i| &vectors_flat[i * dim..(i + 1) * dim])
            .collect();

        // Delete old multi-vector entries for this doc if they exist (upsert).
        coll.delete_multi_vector(doc_id);

        // Insert all vectors with shared doc_id.
        let ids = coll.insert_multi_vector(&vector_slices, doc_id.to_string());

        // Auto-seal if needed.
        if coll.needs_seal()
            && let Some(req) = coll.seal(&index_key)
            && let Some(tx) = &self.build_tx
            && let Err(e) = tx.send(req)
        {
            warn!(core = self.core_id, error = %e, "failed to send HNSW build after multi-vector insert");
        }

        self.checkpoint_coordinator.mark_dirty("vector", ids.len());

        match super::super::response_codec::encode_count("inserted_vectors", ids.len()) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Delete all vectors for a multi-vector document.
    pub(in crate::data::executor) fn execute_multi_vector_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field_name: &str,
        doc_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field_name, %doc_id, "multi-vector delete");

        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);
        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        let deleted = coll.delete_multi_vector(doc_id);
        if deleted > 0 {
            self.checkpoint_coordinator.mark_dirty("vector", deleted);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    /// Search with multi-vector aggregated scoring.
    ///
    /// 1. Over-fetch from HNSW: top_k × over_fetch_factor candidates
    /// 2. Group candidates by doc_id
    /// 3. For each document, collect all its candidate distances
    /// 4. Aggregate per-document using the specified mode (MaxSim/AvgSim/SumSim)
    /// 5. Sort by aggregated score, dedup, return top-K documents
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_multi_vector_score_search(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field_name: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        mode_str: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection, %field_name, top_k, %mode_str,
            "multi-vector score search"
        );

        let mode = match nodedb_types::MultiVectorScoreMode::parse(mode_str) {
            Some(m) => m,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        constraint: format!(
                            "unknown score mode '{mode_str}'; supported: max_sim, avg_sim, sum_sim"
                        ),
                    },
                );
            }
        };

        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);
        let Some(coll) = self.vector_collections.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if coll.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }

        // Over-fetch: we need enough candidates so that after grouping by doc_id,
        // we still have top_k distinct documents. Factor of 10 is conservative
        // for typical multi-vector docs with 50-500 tokens.
        let over_fetch = (top_k * 10).clamp(100, 10_000);
        let ef = if ef_search > 0 {
            ef_search.max(over_fetch)
        } else {
            over_fetch.saturating_mul(2).max(64)
        };

        let candidates = coll.search(query_vector, over_fetch, ef);

        // Group by doc_id. For distance metrics where lower = better (L2, cosine),
        // we need to convert: similarity = 1.0 / (1.0 + distance) so higher = better.
        // For inner product, distance is already a similarity score (higher = better).
        let mut doc_scores: HashMap<String, Vec<f32>> = HashMap::new();

        for result in &candidates {
            let doc_id = match coll.get_doc_id(result.id) {
                Some(id) => id.to_string(),
                None => result.id.to_string(),
            };
            // Convert distance to similarity for aggregation.
            // Lower distance = more similar → higher similarity score.
            let similarity = 1.0 / (1.0 + result.distance);
            doc_scores.entry(doc_id).or_default().push(similarity);
        }

        // Aggregate per-document and collect results.
        let mut scored_docs: Vec<(String, f32)> = doc_scores
            .iter()
            .map(|(doc_id, scores)| (doc_id.clone(), mode.aggregate(scores)))
            .collect();

        // Sort by aggregated score descending.
        scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Truncate to top_k.
        scored_docs.truncate(top_k);

        // Build response hits.
        let hits: Vec<super::super::response_codec::VectorSearchHit> = scored_docs
            .iter()
            .map(|(doc_id, score)| {
                // Try to parse doc_id as u32 for the `id` field; if it's a string doc_id
                // we use 0 as the numeric id and populate doc_id.
                let numeric_id = doc_id.parse::<u32>().unwrap_or(0);
                super::super::response_codec::VectorSearchHit {
                    id: numeric_id,
                    distance: *score, // This is the aggregated similarity score.
                    doc_id: Some(doc_id.clone()),
                }
            })
            .collect();

        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "multi-vector search encode failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }
}
