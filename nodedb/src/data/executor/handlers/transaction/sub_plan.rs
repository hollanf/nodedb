//! Per-sub-plan execution within a transaction batch.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::bridge::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, MetaOp, TimeseriesOp, VectorOp,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::{
    append_only, hash_chain, materialized_sum, period_lock, retention, state_transition,
    transition_check,
};
use crate::data::executor::handlers::document::extract_indexable_text;
use crate::data::executor::strict_format;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TraceId};

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a single sub-plan within a transaction, recording undo info.
    ///
    /// CRDT deltas are NOT applied immediately — they are buffered in
    /// `crdt_deltas` and only applied after all sub-plans succeed.
    ///
    /// The match is exhaustive: every `PhysicalPlan` variant is explicitly
    /// handled. Write-producing variants push an `UndoEntry`; read-only and
    /// DDL variants pass through without undo tracking.
    #[allow(clippy::too_many_lines)]
    pub(super) fn execute_tx_sub_plan(
        &mut self,
        tid: u64,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64)>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        // Temporary task used for sub-plan response construction.
        let dummy_task = ExecutionTask::new(crate::bridge::envelope::Request {
            request_id: crate::types::RequestId::new(0),
            tenant_id: TenantId::new(tid),
            vshard_id: crate::types::VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: crate::types::RequestId::new(0),
            }),
            // no-determinism: sub-plan deadline is ephemeral, not written to WAL
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
            priority: crate::bridge::envelope::Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        });

        match plan {
            // ── Document writes ──────────────────────────────────────────────
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                ..
            }) => self.tx_point_put(
                &dummy_task,
                tid,
                collection,
                document_id,
                *surrogate,
                value,
                undo_log,
                user_roles,
            ),

            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
            }) => {
                let row_key = crate::engine::document::store::surrogate_to_doc_id(*surrogate);
                let exists = self
                    .sparse
                    .get(tid, collection, &row_key)
                    .ok()
                    .flatten()
                    .is_some();
                if exists {
                    if *if_absent {
                        return Ok(self.response_ok(&dummy_task));
                    }
                    return Err(ErrorCode::RejectedConstraint {
                        constraint: "unique".to_string(),
                        detail: format!(
                            "duplicate key value '{document_id}' violates primary-key \
                             uniqueness on '{collection}'"
                        ),
                    });
                }
                self.tx_point_put(
                    &dummy_task,
                    tid,
                    collection,
                    document_id,
                    *surrogate,
                    value,
                    undo_log,
                    user_roles,
                )
            }

            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                ..
            }) => self.tx_point_delete(
                &dummy_task,
                tid,
                collection,
                document_id,
                *surrogate,
                undo_log,
            ),

            // ── Vector writes ────────────────────────────────────────────────
            PhysicalPlan::Vector(VectorOp::Insert {
                collection,
                vector,
                dim,
                field_name,
                surrogate,
            }) => {
                let index_key = Self::vector_index_key(tid, collection, field_name);
                let params = self
                    .vector_params
                    .get(&index_key)
                    .cloned()
                    .unwrap_or_default();
                let index = self
                    .vector_collections
                    .entry(index_key.clone())
                    .or_insert_with(|| {
                        crate::engine::vector::collection::VectorCollection::new(*dim, params)
                    });

                if vector.len() != index.dim() {
                    return Err(ErrorCode::Internal {
                        detail: format!(
                            "dimension mismatch: expected {}, got {}",
                            index.dim(),
                            vector.len()
                        ),
                    });
                }

                let vector_id = index.len() as u32;
                index.insert_with_surrogate(vector.clone(), *surrogate);
                undo_log.push(UndoEntry::InsertVector {
                    index_key,
                    vector_id,
                });
                Ok(self.response_ok(&dummy_task))
            }

            PhysicalPlan::Vector(VectorOp::Delete {
                collection,
                vector_id,
            }) => {
                let index_key = Self::vector_index_key(tid, collection, "");
                if let Some(index) = self.vector_collections.get_mut(&index_key)
                    && index.delete(*vector_id)
                {
                    undo_log.push(UndoEntry::DeleteVector {
                        index_key,
                        vector_id: *vector_id,
                    });
                }
                Ok(self.response_ok(&dummy_task))
            }

            // ── Graph writes ─────────────────────────────────────────────────
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            }) => {
                let old_properties = self
                    .edge_store
                    .get_edge(
                        nodedb_types::TenantId::new(tid),
                        collection,
                        src_id,
                        label,
                        dst_id,
                    )
                    .ok()
                    .flatten();

                let resp = self.execute_edge_put(
                    &dummy_task,
                    tid,
                    collection,
                    src_id,
                    label,
                    dst_id,
                    properties,
                    *src_surrogate,
                    *dst_surrogate,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "edge put failed".into(),
                    }));
                }

                undo_log.push(UndoEntry::PutEdge {
                    collection: collection.clone(),
                    src_id: src_id.clone(),
                    label: label.clone(),
                    dst_id: dst_id.clone(),
                    old_properties,
                });
                Ok(resp)
            }

            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
            }) => {
                let old_properties = self
                    .edge_store
                    .get_edge(
                        nodedb_types::TenantId::new(tid),
                        collection,
                        src_id,
                        label,
                        dst_id,
                    )
                    .ok()
                    .flatten();

                let resp =
                    self.execute_edge_delete(&dummy_task, tid, collection, src_id, label, dst_id);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "edge delete failed".into(),
                    }));
                }

                if let Some(props) = old_properties {
                    undo_log.push(UndoEntry::DeleteEdge {
                        collection: collection.clone(),
                        src_id: src_id.clone(),
                        label: label.clone(),
                        dst_id: dst_id.clone(),
                        old_properties: props,
                    });
                }
                Ok(resp)
            }

            // ── CRDT (buffered until commit) ──────────────────────────────────
            PhysicalPlan::Crdt(CrdtOp::Apply { delta, peer_id, .. }) => {
                crdt_deltas.push((delta.clone(), *peer_id));
                Ok(self.response_ok(&dummy_task))
            }

            // ── KV writes (tracked) and reads (passthrough) ──────────────────
            PhysicalPlan::Kv(kv_op) => self.execute_tx_kv(&dummy_task, tid, kv_op, undo_log),

            // ── Columnar insert (tracked) ────────────────────────────────────
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
                surrogates,
            }) => self.execute_tx_columnar_insert(
                &dummy_task,
                collection,
                payload,
                format,
                *intent,
                on_conflict_updates,
                surrogates,
                undo_log,
            ),

            // ── Timeseries ingest (tracked) ──────────────────────────────────
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn,
                ..
            }) => self.execute_tx_timeseries_ingest(
                &dummy_task,
                TenantId::new(tid),
                collection,
                payload,
                format,
                *wal_lsn,
                undo_log,
            ),

            // ── Read-only / passthrough variants ─────────────────────────────
            // None of these mutate engine state, so no undo entry is needed.
            // They execute normally via the standard dispatch path.
            PhysicalPlan::Document(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_) => {
                let resp = self.execute(&ExecutionTask::new(crate::bridge::envelope::Request {
                    request_id: crate::types::RequestId::new(0),
                    tenant_id: TenantId::new(tid),
                    vshard_id: crate::types::VShardId::new(0),
                    plan: plan.clone(),
                    // no-determinism: sub-plan deadline is ephemeral, not written to WAL
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
                    priority: crate::bridge::envelope::Priority::Normal,
                    trace_id: TraceId::ZERO,
                    consistency: crate::types::ReadConsistency::Strong,
                    idempotency_key: None,
                    event_source: crate::event::EventSource::User,
                    user_roles: Vec::new(),
                }));
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "sub-plan execution failed".into(),
                    }));
                }
                Ok(resp)
            }
        }
    }

    // ── PointPut / PointDelete helpers (unchanged from prior impl) ───────────

    /// Execute a PointPut within a transaction.
    #[allow(clippy::too_many_arguments)]
    fn tx_point_put(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: nodedb_types::Surrogate,
        value: &[u8],
        undo_log: &mut Vec<UndoEntry>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let old_value = self.sparse.get(tid, collection, row_key).ok().flatten();

        let config_key = (TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key) {
            append_only::check_point_put(collection, &config.enforcement, &old_value)?;
            if let Some(ref pl) = config.enforcement.period_lock {
                period_lock::check_period_lock(&self.sparse, tid, collection, value, pl)?;
            }
            if old_value.is_some() {
                let old_json = old_value
                    .as_ref()
                    .and_then(|b| doc_format::decode_document(b));
                let new_json = doc_format::decode_document(value);
                if let (Some(old_doc), Some(new_doc)) = (&old_json, &new_json) {
                    if !config.enforcement.state_constraints.is_empty() {
                        state_transition::check_state_transitions(
                            collection,
                            &config.enforcement.state_constraints,
                            old_doc,
                            new_doc,
                            user_roles,
                        )?;
                    }
                    if !config.enforcement.transition_checks.is_empty() {
                        transition_check::check_transition_predicates(
                            collection,
                            &config.enforcement.transition_checks,
                            old_doc,
                            new_doc,
                        )?;
                    }
                }
            }
        }

        let encode_for_storage = |bytes: &[u8]| -> Result<Vec<u8>, ErrorCode> {
            if let Some(config) = self.doc_configs.get(&config_key)
                && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
            {
                strict_format::bytes_to_binary_tuple(bytes, schema).map_err(|e| {
                    ErrorCode::Internal {
                        detail: format!("strict encode: {e}"),
                    }
                })
            } else {
                Ok(doc_format::canonicalize_document_for_storage(bytes))
            }
        };

        let stored = if old_value.is_none() {
            let hash_chain_enabled = self
                .doc_configs
                .get(&config_key)
                .is_some_and(|c| c.enforcement.hash_chain);
            match hash_chain::apply_chain_on_insert(
                &mut self.chain_hashes,
                tid,
                collection,
                document_id,
                value,
                hash_chain_enabled,
            ) {
                Some(chained) => chained,
                None => encode_for_storage(value)?,
            }
        } else {
            encode_for_storage(value)?
        };
        match self.sparse.put(tid, collection, row_key, &stored) {
            Ok(_prior) => {
                if let Some(doc) = doc_format::decode_document(value) {
                    let text_content = extract_indexable_text(&doc);
                    if !text_content.is_empty() {
                        let _ = self.inverted.index_document(
                            TenantId::new(tid),
                            collection,
                            surrogate,
                            &text_content,
                        );
                    }
                }

                undo_log.push(UndoEntry::PutDocument {
                    collection: collection.to_string(),
                    document_id: row_key.to_string(),
                    surrogate,
                    old_value: old_value.clone(),
                });

                if old_value.is_none()
                    && let Some(config) = self.doc_configs.get(&config_key)
                    && !config.enforcement.materialized_sum_sources.is_empty()
                    && let Some(src_doc) = doc_format::decode_document(value)
                {
                    let target_writes = materialized_sum::apply_materialized_sums(
                        &self.sparse,
                        tid,
                        &config.enforcement.materialized_sum_sources,
                        &src_doc,
                    )?;
                    for tw in target_writes {
                        undo_log.push(UndoEntry::PutDocument {
                            collection: tw.collection,
                            document_id: tw.document_id,
                            surrogate: nodedb_types::Surrogate::ZERO,
                            old_value: tw.old_value,
                        });
                    }
                }

                Ok(self.response_ok(dummy_task))
            }
            Err(e) => Err(ErrorCode::Internal {
                detail: e.to_string(),
            }),
        }
    }

    /// Execute a PointDelete within a transaction.
    #[allow(clippy::too_many_arguments)]
    fn tx_point_delete(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: nodedb_types::Surrogate,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let _ = document_id;
        let config_key = (TenantId::new(tid), collection.to_string());
        let old_value = self.sparse.get(tid, collection, row_key).ok().flatten();
        if let Some(config) = self.doc_configs.get(&config_key) {
            append_only::check_point_delete(collection, &config.enforcement)?;
            if let Some(ref pl) = config.enforcement.period_lock
                && let Some(ref old_bytes) = old_value
            {
                period_lock::check_period_lock(&self.sparse, tid, collection, old_bytes, pl)?;
            }
            let created_at = old_value
                .as_ref()
                .and_then(|b| retention::extract_created_at_secs(b));
            retention::check_delete_allowed(collection, &config.enforcement, created_at)?;
        }
        match self.sparse.delete(tid, collection, row_key) {
            Ok(_) => {
                if let Some(s) = crate::engine::document::store::doc_id_to_surrogate(row_key) {
                    let _ = self
                        .inverted
                        .remove_document(TenantId::new(tid), collection, s);
                }
                let _ = self
                    .sparse
                    .delete_indexes_for_document(tid, collection, row_key);
                let edges_removed = self.csr_partition_mut(tid).remove_node_edges(row_key);
                if edges_removed > 0 {
                    let cascade_ord = self.hlc.next_ordinal();
                    let _ = self.edge_store.delete_edges_for_node(
                        nodedb_types::TenantId::new(tid),
                        row_key,
                        cascade_ord,
                    );
                }

                if let Some(old) = old_value {
                    undo_log.push(UndoEntry::DeleteDocument {
                        collection: collection.to_string(),
                        document_id: row_key.to_string(),
                        old_value: old,
                    });
                }
                Ok(self.response_ok(dummy_task))
            }
            Err(e) => Err(ErrorCode::Internal {
                detail: e.to_string(),
            }),
        }
    }
}
