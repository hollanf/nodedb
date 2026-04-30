//! Aggregate handler: GROUP BY, HAVING, and aggregate function execution.
//!
//! The generic (non-columnar) path uses **streaming accumulators** — see
//! `accum.rs`.  Raw document bytes are never stored; only the extracted
//! scalar / approximate values needed by each aggregate function are kept.
//! Memory is O(num_groups × num_aggregates) instead of
//! O(total_matching_docs × avg_doc_size).

use std::collections::HashMap;

use sonic_rs;
use tracing::debug;

use super::accum::GroupState;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::AggregateSpec;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::agg_key::canonical_agg_key;
use nodedb_query::msgpack_scan;

// ── Cache key ──────────────────────────────────────────────────────────────

fn aggregate_cache_key(
    tid: u64,
    collection: &str,
    group_by: &[String],
    aggregates: &[AggregateSpec],
    sub_group_by: &[String],
    sub_aggregates: &[AggregateSpec],
) -> (crate::types::TenantId, String) {
    use std::fmt::Write;
    let mut rest = format!(
        "{collection}\0{}\0{}",
        group_by.join(","),
        aggregates
            .iter()
            .map(|agg| {
                if agg.expr.is_some() {
                    format!("{}(expr)->{}", agg.function, agg.alias)
                } else {
                    format!("{}({})->{}", agg.function, agg.field, agg.alias)
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    );
    if !sub_group_by.is_empty() || !sub_aggregates.is_empty() {
        let _ = write!(
            rest,
            "\0sub:{}\0{}",
            sub_group_by.join(","),
            sub_aggregates
                .iter()
                .map(|agg| {
                    if agg.expr.is_some() {
                        format!("{}(expr)->{}", agg.function, agg.alias)
                    } else {
                        format!("{}({})->{}", agg.function, agg.field, agg.alias)
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    (crate::types::TenantId::new(tid), rest)
}

fn legacy_aggregate_pairs(aggregates: &[AggregateSpec]) -> Option<Vec<(String, String)>> {
    aggregates
        .iter()
        .map(|agg| {
            if agg.expr.is_some() {
                None
            } else {
                Some((agg.function.clone(), agg.field.clone()))
            }
        })
        .collect()
}

fn apply_user_aliases_to_rows(rows: &mut [serde_json::Value], aggregates: &[AggregateSpec]) {
    let renames: Vec<(&str, &str)> = aggregates
        .iter()
        .filter_map(|agg| {
            agg.user_alias
                .as_deref()
                .filter(|alias| *alias != agg.alias)
                .map(|alias| (agg.alias.as_str(), alias))
        })
        .collect();

    if renames.is_empty() {
        return;
    }

    for row in rows {
        if let Some(obj) = row.as_object_mut() {
            for (from, to) in &renames {
                if let Some(value) = obj.remove(*from) {
                    obj.insert((*to).to_string(), value);
                }
            }
        }
    }
}

// ── CoreLoop impl ──────────────────────────────────────────────────────────

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_aggregate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        group_by: &[String],
        aggregates: &[AggregateSpec],
        filters: &[u8],
        having: &[u8],
        limit: usize,
        sub_group_by: &[String],
        sub_aggregates: &[AggregateSpec],
    ) -> Response {
        debug!(core = self.core_id, %collection, group_fields = group_by.len(), aggs = aggregates.len(), "aggregate");

        // Fast path: incremental aggregate cache.
        if filters.is_empty() && having.is_empty() {
            let cache_key = aggregate_cache_key(
                tid,
                collection,
                group_by,
                aggregates,
                sub_group_by,
                sub_aggregates,
            );
            if let Some(cached) = self.aggregate_cache.get(&cache_key) {
                debug!(core = self.core_id, %collection, "aggregate cache hit");
                return self.response_with_payload(task, cached.clone());
            }
        }

        // Fast path: index-backed COUNT/GROUP BY.
        if group_by.len() == 1
            && filters.is_empty()
            && having.is_empty()
            && aggregates.len() == 1
            && aggregates[0].expr.is_none()
            && aggregates[0].function == "count"
        {
            let field = &group_by[0];
            if let Ok(groups) = self.sparse.scan_index_groups(tid, collection, field)
                && !groups.is_empty()
            {
                let mut payload_buf = Vec::with_capacity(groups.len() * 64);
                let row_count = groups.len().min(limit);
                let count_key = aggregates[0]
                    .user_alias
                    .clone()
                    .unwrap_or_else(|| canonical_agg_key("count", "*"));
                msgpack_scan::write_array_header(&mut payload_buf, row_count);
                for (value, count) in groups.into_iter().take(limit) {
                    msgpack_scan::write_map_header(&mut payload_buf, 2);
                    msgpack_scan::write_kv_str(&mut payload_buf, field, &value);
                    msgpack_scan::write_kv_i64(&mut payload_buf, &count_key, count as i64);
                }
                return match Ok::<Vec<u8>, crate::Error>(payload_buf) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        }

        let scan_limit = self.query_tuning.aggregate_scan_cap;

        let mt_key = (crate::types::TenantId::new(tid), collection.to_string());
        let columnar_mt = self
            .columnar_memtables
            .get(&mt_key)
            .filter(|mt| !mt.is_empty());

        // Fast path: native columnar aggregation.
        if let Some(mt) =
            columnar_mt.filter(|_| sub_group_by.is_empty() && sub_aggregates.is_empty())
        {
            let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                Vec::new()
            } else {
                match zerompk::from_msgpack(filters) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!(core = self.core_id, error = %e, "filter predicate deserialization failed");
                        Vec::new()
                    }
                }
            };

            let legacy_aggs = legacy_aggregate_pairs(aggregates);
            if let Some(mut agg_result) = legacy_aggs.and_then(|pairs| {
                super::columnar_agg::try_columnar_aggregate(
                    mt,
                    group_by,
                    &pairs,
                    &filter_predicates,
                    limit,
                    scan_limit,
                )
            }) {
                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> = match zerompk::from_msgpack(having) {
                        Ok(h) => h,
                        Err(e) => {
                            tracing::warn!(core = self.core_id, error = %e, "having predicate deserialization failed");
                            Vec::new()
                        }
                    };
                    if !having_predicates.is_empty() {
                        agg_result.rows.retain(|row| {
                            let mp = nodedb_types::json_to_msgpack_or_empty(row);
                            having_predicates.iter().all(|f| f.matches_binary(&mp))
                        });
                    }
                }

                apply_user_aliases_to_rows(&mut agg_result.rows, aggregates);
                agg_result.rows.truncate(limit);

                return match super::super::response_codec::encode_json_vec(&agg_result.rows) {
                    Ok(payload) => {
                        if filters.is_empty() && having.is_empty() {
                            let cache_key = aggregate_cache_key(
                                tid,
                                collection,
                                group_by,
                                aggregates,
                                sub_group_by,
                                sub_aggregates,
                            );
                            if self.aggregate_cache.len() < 256 {
                                self.aggregate_cache.insert(cache_key, payload.clone());
                            }
                        }
                        self.response_with_payload(task, payload)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        }

        // ── Streaming aggregation ──────────────────────────────────────────
        // Documents are processed one at a time.  Per-group accumulators hold
        // only the derived scalar / approximate state needed for the final
        // result — no raw document bytes are retained.
        // Memory: O(num_groups × num_aggregates) instead of O(all_docs).

        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "filter predicate deserialization failed");
                    Vec::new()
                }
            }
        };

        let use_field_index = filter_predicates.len() + group_by.len() >= 2;
        let need_sub = !sub_group_by.is_empty() && !sub_aggregates.is_empty();

        // outer_group_key → GroupState
        let mut groups: HashMap<String, GroupState> = HashMap::new();
        // outer_group_key → sub_group_key → GroupState
        let mut sub_groups: HashMap<String, HashMap<String, GroupState>> = HashMap::new();

        let chunk_size = 10_000;

        let scan_result = self
            .scan_collection(tid, collection, scan_limit)
            .map(|docs| {
                for chunk in docs.chunks(chunk_size) {
                    for (_, value) in chunk {
                        let outer_key = if use_field_index {
                            let idx = msgpack_scan::FieldIndex::build(value, 0)
                                .unwrap_or_else(msgpack_scan::FieldIndex::empty);
                            if !filter_predicates
                                .iter()
                                .all(|f| f.matches_binary_indexed(value, &idx))
                            {
                                continue;
                            }
                            msgpack_scan::group_key::build_group_key_indexed(value, group_by, &idx)
                        } else {
                            if !filter_predicates.iter().all(|f| f.matches_binary(value)) {
                                continue;
                            }
                            msgpack_scan::build_group_key(value, group_by)
                        };

                        groups
                            .entry(outer_key.clone())
                            .or_insert_with(|| GroupState::new(aggregates))
                            .feed(aggregates, value);

                        if need_sub {
                            let sub_key = msgpack_scan::build_group_key(value, sub_group_by);
                            sub_groups
                                .entry(outer_key)
                                .or_default()
                                .entry(sub_key)
                                .or_insert_with(|| GroupState::new(sub_aggregates))
                                .feed(sub_aggregates, value);
                        }
                    }
                }
            });

        match scan_result {
            Ok(()) => {
                let mut results: Vec<serde_json::Value> = Vec::new();

                for (group_key, state) in groups {
                    let mut row = serde_json::Map::new();

                    if !group_by.is_empty()
                        && let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(&group_key)
                    {
                        for (i, field) in group_by.iter().enumerate() {
                            let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                            row.insert(field.clone(), val);
                        }
                    }

                    for (alias, val) in state.finalize(aggregates) {
                        let json_val: serde_json::Value = val.into();
                        row.insert(alias, json_val);
                    }

                    if need_sub {
                        let sub_map = sub_groups.remove(&group_key).unwrap_or_default();
                        let mut sub_results: Vec<serde_json::Value> = Vec::new();
                        for (sub_key, sub_state) in sub_map {
                            let mut sub_row = serde_json::Map::new();
                            if let Ok(parts) =
                                sonic_rs::from_str::<Vec<serde_json::Value>>(&sub_key)
                            {
                                for (i, field) in sub_group_by.iter().enumerate() {
                                    let val =
                                        parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                                    sub_row.insert(field.clone(), val);
                                }
                            }
                            for (alias, val) in sub_state.finalize(sub_aggregates) {
                                let json_val: serde_json::Value = val.into();
                                sub_row.insert(alias, json_val);
                            }
                            let mut sub_value = serde_json::Value::Object(sub_row);
                            apply_user_aliases_to_rows(
                                std::slice::from_mut(&mut sub_value),
                                sub_aggregates,
                            );
                            sub_results.push(sub_value);
                        }
                        row.insert(
                            "sub_groups".to_string(),
                            serde_json::Value::Array(sub_results),
                        );
                    }

                    results.push(serde_json::Value::Object(row));
                }

                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> = match zerompk::from_msgpack(having) {
                        Ok(f) => f,
                        Err(e) => {
                            tracing::warn!(
                                core = self.core_id,
                                error = %e,
                                "HAVING predicate deserialization failed (schemaless)"
                            );
                            Vec::new()
                        }
                    };
                    if !having_predicates.is_empty() {
                        results.retain(|row| {
                            let mp = nodedb_types::json_to_msgpack_or_empty(row);
                            having_predicates.iter().all(|f| f.matches_binary(&mp))
                        });
                    }
                }

                apply_user_aliases_to_rows(&mut results, aggregates);
                results.truncate(limit);

                match super::super::response_codec::encode_json_vec(&results) {
                    Ok(payload) => {
                        if filters.is_empty() && having.is_empty() {
                            let cache_key = aggregate_cache_key(
                                tid,
                                collection,
                                group_by,
                                aggregates,
                                sub_group_by,
                                sub_aggregates,
                            );
                            if self.aggregate_cache.len() < 256 {
                                self.aggregate_cache.insert(cache_key, payload.clone());
                            }
                        }
                        self.response_with_payload(task, payload)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
