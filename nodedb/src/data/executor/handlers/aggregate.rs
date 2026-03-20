//! Aggregate handler: GROUP BY, HAVING, and aggregate function execution.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{ScanFilter, compute_aggregate};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_aggregate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        group_by: &[String],
        aggregates: &[(String, String)],
        filters: &[u8],
        having: &[u8],
        limit: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, group_fields = group_by.len(), aggs = aggregates.len(), "aggregate");

        // Aggregates must scan all matching documents for correct results.
        // Cap at 10M to prevent OOM on unbounded collections.
        const AGGREGATE_SCAN_CAP: usize = 10_000_000;
        let scan_limit = AGGREGATE_SCAN_CAP;
        match self.sparse.scan_documents(tid, collection, scan_limit) {
            Ok(docs) => {
                let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                    Vec::new()
                } else {
                    rmp_serde::from_slice(filters).unwrap_or_default()
                };

                let mut groups: std::collections::HashMap<String, Vec<serde_json::Value>> =
                    std::collections::HashMap::new();

                // Determine which fields we actually need to extract.
                // This avoids full document deserialization when possible.
                let mut needed_fields: Vec<&str> = Vec::new();
                for f in group_by {
                    if !needed_fields.contains(&f.as_str()) {
                        needed_fields.push(f.as_str());
                    }
                }
                for (_, field) in aggregates {
                    if field != "*" && !needed_fields.contains(&field.as_str()) {
                        needed_fields.push(field.as_str());
                    }
                }

                // If filters are present, we need full deserialization for
                // filter evaluation (filters can reference any field).
                let needs_full_deser = !filter_predicates.is_empty();

                for (_, value) in &docs {
                    if needs_full_deser {
                        // Full deserialization path (filters need arbitrary field access).
                        let Some(doc) = super::super::doc_format::decode_document(value) else {
                            continue;
                        };
                        if !filter_predicates.iter().all(|f| f.matches(&doc)) {
                            continue;
                        }
                        let key = if group_by.is_empty() {
                            "__all__".to_string()
                        } else {
                            let key_parts: Vec<serde_json::Value> = group_by
                                .iter()
                                .map(|field| {
                                    doc.get(field.as_str())
                                        .cloned()
                                        .unwrap_or(serde_json::Value::Null)
                                })
                                .collect();
                            serde_json::to_string(&key_parts).unwrap_or_else(|_| "[]".into())
                        };
                        groups.entry(key).or_default().push(doc);
                    } else {
                        // Targeted extraction path: only extract needed fields.
                        let extracted =
                            super::super::doc_format::extract_fields(value, &needed_fields);

                        // Build group key from extracted fields.
                        let key = if group_by.is_empty() {
                            "__all__".to_string()
                        } else {
                            let key_parts: Vec<serde_json::Value> = group_by
                                .iter()
                                .map(|field| {
                                    let idx =
                                        needed_fields.iter().position(|&n| n == field.as_str());
                                    idx.and_then(|i| extracted[i].clone())
                                        .unwrap_or(serde_json::Value::Null)
                                })
                                .collect();
                            serde_json::to_string(&key_parts).unwrap_or_else(|_| "[]".into())
                        };

                        // Build a partial document with only the needed fields.
                        let mut doc_map = serde_json::Map::new();
                        for (i, &field_name) in needed_fields.iter().enumerate() {
                            if let Some(val) = &extracted[i] {
                                doc_map.insert(field_name.to_string(), val.clone());
                            }
                        }
                        groups
                            .entry(key)
                            .or_default()
                            .push(serde_json::Value::Object(doc_map));
                    }
                }

                let mut results: Vec<serde_json::Value> = Vec::new();
                for (group_key, group_docs) in &groups {
                    let mut row = serde_json::Map::new();

                    if !group_by.is_empty() {
                        if let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(group_key)
                        {
                            for (i, field) in group_by.iter().enumerate() {
                                let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                                row.insert(field.clone(), val);
                            }
                        }
                    }

                    for (op, field) in aggregates {
                        let agg_key = format!("{op}_{field}").replace('*', "all");
                        let val = compute_aggregate(op, field, group_docs);
                        row.insert(agg_key, val);
                    }

                    results.push(serde_json::Value::Object(row));
                }

                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> =
                        rmp_serde::from_slice(having).unwrap_or_default();
                    if !having_predicates.is_empty() {
                        results.retain(|row| having_predicates.iter().all(|f| f.matches(row)));
                    }
                }

                results.truncate(limit);

                match super::super::response_codec::encode(&results) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::Internal { detail: e }),
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
