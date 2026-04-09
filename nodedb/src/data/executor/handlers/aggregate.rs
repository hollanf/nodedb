//! Aggregate handler: GROUP BY, HAVING, and aggregate function execution.

use sonic_rs;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::AggregateSpec;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::msgpack_scan;

/// Build a cache key for an aggregate query.
///
/// Format: `"{tid}:{collection}\0{group_fields}\0{agg_ops}"`.
/// Null bytes separate sections to avoid ambiguity with field names.
fn aggregate_cache_key(
    tid: u32,
    collection: &str,
    group_by: &[String],
    aggregates: &[AggregateSpec],
    sub_group_by: &[String],
    sub_aggregates: &[AggregateSpec],
) -> String {
    use std::fmt::Write;
    let mut key = format!(
        "{tid}:{collection}\0{}\0{}",
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
            key,
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
    key
}

/// Group a single document into the binary_groups map.
///
/// Applies filter predicates, computes group key, and stores the raw
/// document bytes for later aggregation.
fn group_doc(
    value: &[u8],
    group_by: &[String],
    filter_predicates: &[ScanFilter],
    use_field_index: bool,
    binary_groups: &mut std::collections::HashMap<String, Vec<Vec<u8>>>,
) {
    if use_field_index {
        let idx = msgpack_scan::FieldIndex::build(value, 0)
            .unwrap_or_else(msgpack_scan::FieldIndex::empty);
        if !filter_predicates
            .iter()
            .all(|f| f.matches_binary_indexed(value, &idx))
        {
            return;
        }
        let key = msgpack_scan::group_key::build_group_key_indexed(value, group_by, &idx);
        binary_groups.entry(key).or_default().push(value.to_vec());
    } else {
        if !filter_predicates.iter().all(|f| f.matches_binary(value)) {
            return;
        }
        let key = msgpack_scan::build_group_key(value, group_by);
        binary_groups.entry(key).or_default().push(value.to_vec());
    }
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

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_aggregate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
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
        // If we've cached the result for this exact (collection, group_by, aggregates)
        // combination and there are no filters/having, return cached result directly.
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
        // When GROUP BY has a single field, no filters, no HAVING, and the
        // only aggregate is COUNT(*), scan the INDEXES table directly.
        // No document table access — O(index_entries) instead of O(documents).
        if group_by.len() == 1
            && filters.is_empty()
            && having.is_empty()
            && aggregates.len() == 1
            && aggregates[0].expr.is_none()
            && aggregates[0].function == "count"
        {
            let field = &group_by[0];
            // Empty index — fall through to full scan (documents may exist
            // without index entries if no secondary indexes are declared).
            if let Ok(groups) = self.sparse.scan_index_groups(tid, collection, field)
                && !groups.is_empty()
            {
                // Build result rows as raw msgpack — no serde_json::Value.
                let mut payload_buf = Vec::with_capacity(groups.len() * 64);
                let row_count = groups.len().min(limit);
                msgpack_scan::write_array_header(&mut payload_buf, row_count);
                for (value, count) in groups.into_iter().take(limit) {
                    msgpack_scan::write_map_header(&mut payload_buf, 2);
                    msgpack_scan::write_kv_str(&mut payload_buf, field, &value);
                    msgpack_scan::write_kv_i64(&mut payload_buf, "count_all", count as i64);
                }
                let results_payload = payload_buf;
                return match Ok::<Vec<u8>, crate::Error>(results_payload) {
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

        // Aggregates must scan all matching documents for correct results.
        // Cap at aggregate_scan_cap to prevent OOM on unbounded collections.
        let scan_limit = self.query_tuning.aggregate_scan_cap;

        // If collection has columnar memtable data, read from there.
        // Works for all columnar profiles: plain, timeseries, spatial.
        // Spatial inserts write to both sparse (R-tree) and columnar (scans/aggregates).
        let columnar_mt = self
            .columnar_memtables
            .get(collection)
            .filter(|mt| !mt.is_empty());

        // Fast path: native columnar aggregation.
        // Groups directly on symbol IDs (u32) instead of JSON-serialized strings.
        // Accumulates in-place without document construction.
        // Falls back to generic path for complex filters (OR, string comparisons).
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
                // Apply HAVING filters.
                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> = match zerompk::from_msgpack(having) {
                        Ok(h) => h,
                        Err(e) => {
                            tracing::warn!(core = self.core_id, error = %e, "having predicate deserialization failed");
                            Vec::new()
                        }
                    };
                    if !having_predicates.is_empty() {
                        agg_result
                            .rows
                            .retain(|row| having_predicates.iter().all(|f| f.matches(row)));
                    }
                }

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
            // Native path returned None — fall through to generic path.
        }

        // ── Streaming aggregation: process documents in chunks ──
        // Instead of loading all documents into memory, scan in chunks of
        // 10K docs, group + aggregate each chunk, then merge partial results.
        // Memory: O(chunk_size + num_groups) instead of O(all_docs).

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

        // Accumulate per-group doc bytes across all chunks.
        // Key: group_key string, Value: collected raw doc bytes for final aggregation.
        let mut binary_groups: std::collections::HashMap<String, Vec<Vec<u8>>> =
            std::collections::HashMap::new();

        let chunk_size = 10_000;

        // Universal scan: routes to the correct engine (KV, columnar, sparse/strict)
        // and normalizes all results to standard msgpack maps.
        let scan_result = self
            .scan_collection(tid, collection, scan_limit)
            .map(|docs| {
                for chunk in docs.chunks(chunk_size) {
                    for (_, value) in chunk {
                        group_doc(
                            value,
                            group_by,
                            &filter_predicates,
                            use_field_index,
                            &mut binary_groups,
                        );
                    }
                }
            });

        match scan_result {
            Ok(()) => {
                let mut results: Vec<serde_json::Value> = Vec::new();
                for (group_key, group_docs) in &binary_groups {
                    let mut row = serde_json::Map::new();

                    // Insert group-by field values into the result row.
                    if !group_by.is_empty()
                        && let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(group_key)
                    {
                        for (i, field) in group_by.iter().enumerate() {
                            let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                            row.insert(field.clone(), val);
                        }
                    }

                    let doc_slices: Vec<&[u8]> = group_docs.iter().map(|d| d.as_slice()).collect();

                    for agg in aggregates {
                        let val = msgpack_scan::compute_aggregate_binary(
                            &agg.function,
                            &agg.field,
                            agg.expr.as_ref(),
                            &doc_slices,
                        );
                        let json_val: serde_json::Value = val.into();
                        row.insert(agg.alias.clone(), json_val);
                    }

                    // Nested sub-aggregation on raw bytes.
                    if !sub_group_by.is_empty() && !sub_aggregates.is_empty() {
                        let mut sub_groups: std::collections::HashMap<String, Vec<&[u8]>> =
                            std::collections::HashMap::new();
                        for doc_bytes in &doc_slices {
                            let sub_key = msgpack_scan::build_group_key(doc_bytes, sub_group_by);
                            sub_groups.entry(sub_key).or_default().push(doc_bytes);
                        }

                        let mut sub_results = Vec::new();
                        for (sub_key, sub_docs) in &sub_groups {
                            let mut sub_row = serde_json::Map::new();
                            if let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(sub_key)
                            {
                                for (i, field) in sub_group_by.iter().enumerate() {
                                    let val =
                                        parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                                    sub_row.insert(field.clone(), val);
                                }
                            }
                            for agg in sub_aggregates {
                                let val = msgpack_scan::compute_aggregate_binary(
                                    &agg.function,
                                    &agg.field,
                                    agg.expr.as_ref(),
                                    sub_docs,
                                );
                                let json_val: serde_json::Value = val.into();
                                sub_row.insert(agg.alias.clone(), json_val);
                            }
                            sub_results.push(serde_json::Value::Object(sub_row));
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
                            tracing::warn!(core = self.core_id, error = %e, "HAVING predicate deserialization failed (schemaless)");
                            Vec::new()
                        }
                    };
                    if !having_predicates.is_empty() {
                        results.retain(|row| having_predicates.iter().all(|f| f.matches(row)));
                    }
                }

                results.truncate(limit);

                match super::super::response_codec::encode_json_vec(&results) {
                    Ok(payload) => {
                        // Cache the result for future identical queries.
                        if filters.is_empty() && having.is_empty() {
                            let cache_key = aggregate_cache_key(
                                tid,
                                collection,
                                group_by,
                                aggregates,
                                sub_group_by,
                                sub_aggregates,
                            );
                            // Bounded cache: max 256 entries per core.
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
