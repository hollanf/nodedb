//! KV, Columnar, and Timeseries write tracking for transaction batches.
//!
//! These handlers capture prior state before each write so the undo log
//! can reverse the operation on batch failure.

use nodedb_columnar::pk_index::RowLocation;

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::bridge::physical_plan::document::UpdateValue;
use crate::bridge::physical_plan::{ColumnarInsertIntent, KvOp};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TenantId;

use super::undo::UndoEntry;

/// Captured undo state for a pending columnar insert: the list of new PK bytes
/// to insert, paired with the prior `RowLocation` of any displaced memtable rows.
type ColumnarUndoState = (Vec<Vec<u8>>, Vec<(Vec<u8>, RowLocation)>);

impl CoreLoop {
    // ── KV ───────────────────────────────────────────────────────────────────

    /// Execute a KV operation in a transaction context.
    ///
    /// Write operations capture prior state before executing and push an
    /// `UndoEntry`. Read-only operations execute without undo tracking.
    /// DDL/TTL operations are rejected — they do not belong in a multi-engine
    /// `TransactionBatch`.
    pub(super) fn execute_tx_kv(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &KvOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            // ── Read-only KV ops — no undo needed ───────────────────────────
            KvOp::Get { .. }
            | KvOp::Scan { .. }
            | KvOp::BatchGet { .. }
            | KvOp::GetTtl { .. }
            | KvOp::FieldGet { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. } => {
                let resp = self.execute_kv(task, tid, op);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv read failed".into(),
                    }));
                }
                Ok(resp)
            }

            // ── DDL / TTL ops — reject inside TransactionBatch ───────────────
            KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::Truncate { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. } => Err(ErrorCode::Internal {
                detail: "KV DDL / TTL operations are not permitted inside a TransactionBatch"
                    .into(),
            }),

            // ── Write ops — capture prior value, execute, push undo ──────────
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(tid, collection, key, now_ms);
                let resp =
                    self.execute_kv_put(task, tid, collection, key, value, *ttl_ms, *surrogate);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv put failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Insert {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
            } => {
                let resp =
                    self.execute_kv_insert(task, tid, collection, key, value, *ttl_ms, *surrogate);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv insert failed".into(),
                    }));
                }
                // Insert only succeeds when key was absent; prior_value is None.
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: None,
                });
                Ok(resp)
            }

            KvOp::InsertIfAbsent {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
            } => {
                let now_ms = current_ms();
                let was_absent = self.kv_engine.get(tid, collection, key, now_ms).is_none();
                let resp = self.execute_kv_insert_if_absent(
                    task, tid, collection, key, value, *ttl_ms, *surrogate,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv insert-if-absent failed".into(),
                    }));
                }
                // Only push undo if the key was actually written (was absent).
                if was_absent {
                    undo_log.push(UndoEntry::KvPut {
                        collection: collection.clone(),
                        key: key.clone(),
                        prior_value: None,
                    });
                }
                Ok(resp)
            }

            KvOp::InsertOnConflictUpdate {
                collection, key, ..
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(tid, collection, key, now_ms);
                let resp = self.execute_kv(task, tid, op);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv insert-on-conflict-update failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Delete { collection, keys } => {
                let now_ms = current_ms();
                // Capture prior values for all keys that exist before deleting.
                let priors: Vec<(Vec<u8>, Vec<u8>)> = keys
                    .iter()
                    .filter_map(|k| {
                        let v = self.kv_engine.get(tid, collection, k, now_ms)?;
                        Some((k.clone(), v))
                    })
                    .collect();
                let resp = self.execute_kv_delete(task, tid, collection, keys);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv delete failed".into(),
                    }));
                }
                for (key, prior_value) in priors {
                    undo_log.push(UndoEntry::KvDelete {
                        collection: collection.clone(),
                        key,
                        prior_value,
                    });
                }
                Ok(resp)
            }

            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
            } => {
                let now_ms = current_ms();
                let prior_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries
                    .iter()
                    .map(|(k, _v)| {
                        let prior = self.kv_engine.get(tid, collection, k, now_ms);
                        (k.clone(), prior)
                    })
                    .collect();
                let resp = self.execute_kv_batch_put(task, tid, collection, entries, *ttl_ms);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv batch put failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvBatchPut {
                    collection: collection.clone(),
                    entries: prior_entries,
                });
                Ok(resp)
            }

            KvOp::FieldSet {
                collection,
                key,
                updates,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(tid, collection, key, now_ms);
                let resp = self.execute_kv_field_set(task, tid, collection, key, updates);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv field set failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(tid, collection, key, now_ms);
                let resp = self.execute_kv_incr(task, tid, collection, key, *delta, *ttl_ms);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv incr failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::IncrFloat {
                collection,
                key,
                delta,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(tid, collection, key, now_ms);
                let resp = self.execute_kv_incr_float(task, tid, collection, key, *delta);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv incr float failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(tid, collection, key, now_ms);
                let resp = self.execute_kv_cas(task, tid, collection, key, expected, new_value);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv cas failed".into(),
                    }));
                }
                // CAS only mutates on success (which we verified above).
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::GetSet {
                collection,
                key,
                new_value,
            } => {
                let now_ms = current_ms();
                let prior = self.kv_engine.get(tid, collection, key, now_ms);
                let resp = self.execute_kv_getset(task, tid, collection, key, new_value);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv get-set failed".into(),
                    }));
                }
                undo_log.push(UndoEntry::KvPut {
                    collection: collection.clone(),
                    key: key.clone(),
                    prior_value: prior,
                });
                Ok(resp)
            }

            KvOp::Transfer {
                collection,
                source_key,
                dest_key,
                ..
            } => {
                let now_ms = current_ms();
                let source_prior = self.kv_engine.get(tid, collection, source_key, now_ms);
                let dest_prior = self.kv_engine.get(tid, collection, dest_key, now_ms);
                let resp = self.execute_kv(task, tid, op);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv transfer failed".into(),
                    }));
                }
                let Some(source_bytes) = source_prior else {
                    // Transfer requires source to exist; it would have failed above.
                    return Err(ErrorCode::Internal {
                        detail: "kv transfer: source prior missing after success".into(),
                    });
                };
                undo_log.push(UndoEntry::KvTransfer {
                    collection: collection.clone(),
                    source_key: source_key.clone(),
                    source_prior: source_bytes,
                    dest_key: dest_key.clone(),
                    dest_prior,
                });
                Ok(resp)
            }

            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
            } => {
                let now_ms = current_ms();
                let source_prior = self.kv_engine.get(tid, source_collection, item_key, now_ms);
                let dest_prior = self.kv_engine.get(tid, dest_collection, dest_key, now_ms);
                let resp = self.execute_kv_transfer_item(
                    task,
                    tid,
                    source_collection,
                    dest_collection,
                    item_key,
                    dest_key,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "kv transfer-item failed".into(),
                    }));
                }
                let Some(source_bytes) = source_prior else {
                    return Err(ErrorCode::Internal {
                        detail: "kv transfer-item: source prior missing after success".into(),
                    });
                };
                undo_log.push(UndoEntry::KvTransferItem {
                    source_collection: source_collection.clone(),
                    dest_collection: dest_collection.clone(),
                    item_key: item_key.clone(),
                    dest_key: dest_key.clone(),
                    source_prior: source_bytes,
                    dest_prior,
                });
                Ok(resp)
            }
        }
    }

    // ── Columnar insert ──────────────────────────────────────────────────────

    /// Execute a columnar insert in a transaction context.
    ///
    /// Captures `row_count_before`, inserted PK bytes, and displaced prior-row
    /// locations before the insert so the undo log can reverse the operation.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_tx_columnar_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        payload: &[u8],
        format: &str,
        intent: ColumnarInsertIntent,
        on_conflict_updates: &[(String, UpdateValue)],
        surrogates: &[nodedb_types::Surrogate],
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let collection_key = (task.request.tenant_id, collection.to_string());

        let row_count_before = self
            .columnar_engines
            .get(&collection_key)
            .map(|e| e.memtable().row_count())
            .unwrap_or(0);

        let (inserted_pks, displaced) =
            self.capture_columnar_insert_undo_state(&collection_key, payload, intent);

        let resp = self.execute_columnar_insert(
            task,
            collection,
            payload,
            format,
            intent,
            on_conflict_updates,
            surrogates,
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                detail: "columnar insert failed".into(),
            }));
        }

        undo_log.push(UndoEntry::ColumnarInsert {
            collection_key,
            row_count_before,
            inserted_pks,
            displaced,
        });
        Ok(resp)
    }

    /// Capture the PK bytes and displaced prior-row locations for a pending
    /// columnar insert, without executing the insert.
    fn capture_columnar_insert_undo_state(
        &self,
        collection_key: &(TenantId, String),
        payload: &[u8],
        intent: ColumnarInsertIntent,
    ) -> ColumnarUndoState {
        let mut inserted_pks: Vec<Vec<u8>> = Vec::new();
        let mut displaced: Vec<(Vec<u8>, RowLocation)> = Vec::new();

        let Some(engine) = self.columnar_engines.get(collection_key) else {
            // Engine doesn't exist yet; execute_columnar_insert will create it.
            // row_count_before will be 0, so truncate_to(0) handles rollback.
            return (inserted_pks, displaced);
        };

        let ndb_rows: Vec<nodedb_types::Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(arr)) => arr,
            Ok(v @ nodedb_types::Value::Object(_)) => vec![v],
            _ => return (inserted_pks, displaced),
        };

        let schema = engine.schema().clone();
        for row in &ndb_rows {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };

            let values: Vec<nodedb_types::Value> = schema
                .columns
                .iter()
                .map(|col| {
                    obj.get(&col.name)
                        .cloned()
                        .unwrap_or(nodedb_types::Value::Null)
                })
                .collect();

            let Ok(pk_bytes) = engine.encode_pk_from_row(&values) else {
                continue;
            };

            match intent {
                ColumnarInsertIntent::InsertIfAbsent => {
                    if !engine.pk_index().contains(&pk_bytes) {
                        inserted_pks.push(pk_bytes);
                    }
                }
                ColumnarInsertIntent::Insert | ColumnarInsertIntent::Put => {
                    if let Some(prior_loc) = engine.pk_index().get(&pk_bytes).copied()
                        && prior_loc.segment_id == engine.memtable_segment_id()
                    {
                        displaced.push((pk_bytes.clone(), prior_loc));
                    }
                    inserted_pks.push(pk_bytes);
                }
            }
        }

        (inserted_pks, displaced)
    }

    // ── Timeseries ingest ────────────────────────────────────────────────────

    /// Execute a timeseries ingest in a transaction context.
    ///
    /// Captures the memtable row count before ingest so the undo log can
    /// truncate back to that point on batch failure.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_tx_timeseries_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: TenantId,
        collection: &str,
        payload: &[u8],
        format: &str,
        wal_lsn: Option<u64>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let collection_key = (tid, collection.to_string());

        let row_count_before = self
            .columnar_memtables
            .get(&collection_key)
            .map(|mt| mt.row_count())
            .unwrap_or(0);

        let resp = self.execute_timeseries_ingest(task, tid, collection, payload, format, wal_lsn);
        if resp.status == Status::Error {
            return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                detail: "timeseries ingest failed".into(),
            }));
        }

        undo_log.push(UndoEntry::TimeseriesIngest {
            collection_key,
            row_count_before,
        });
        Ok(resp)
    }
}
