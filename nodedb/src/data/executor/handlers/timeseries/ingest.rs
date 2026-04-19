//! Timeseries ILP ingest handler.

use std::collections::HashMap;

use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use super::msgpack_decode::{self, MsgpackValue};
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig,
};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

impl CoreLoop {
    /// Execute a timeseries ingest.
    ///
    /// `wal_lsn` is set by the WAL catch-up task to enable deduplication:
    /// if the record has already been ingested (LSN <= max ingested) or
    /// flushed to disk (LSN <= max flushed), the ingest is skipped.
    pub(in crate::data::executor) fn execute_timeseries_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        format: &str,
        wal_lsn: Option<u64>,
    ) -> Response {
        let key = (tid, collection.to_string());
        // LSN-based deduplication: only skip records that are provably
        // already flushed to sealed disk partitions.
        if let Some(lsn) = wal_lsn
            && let Some(registry) = self.ts_registries.get(&key)
        {
            let max_flushed = registry
                .iter()
                .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                .max()
                .unwrap_or(0);
            if max_flushed > 0 && lsn <= max_flushed {
                let result = serde_json::json!({
                    "accepted": 0,
                    "rejected": 0,
                    "collection": collection,
                    "dedup_skipped": true,
                });
                let json = match response_codec::encode_json(&result) {
                    Ok(b) => b,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                };
                return Response {
                    request_id: task.request.request_id,
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_vec(json),
                    watermark_lsn: self.watermark,
                    error_code: None,
                };
            }
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match format {
            "ilp" => self.execute_ilp_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "json" => self.execute_json_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "msgpack" => {
                self.execute_msgpack_ingest(task, tid, collection, payload, wal_lsn, now_ms)
            }
            _ => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("unknown ingest format: {format}"),
                },
            ),
        }
    }

    fn execute_ilp_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        wal_lsn: Option<u64>,
        now_ms: i64,
    ) -> Response {
        let key = (tid, collection.to_string());
        let input = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid UTF-8 in ILP: {e}"),
                    },
                );
            }
        };

        let lines: Vec<_> = ilp::parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        if lines.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid ILP lines in payload".into(),
                },
            );
        }

        // Ensure memtable exists (auto-create on first write).
        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        if is_new_memtable {
            let schema = ilp_ingest::infer_schema(&lines);
            let config = ColumnarMemtableConfig {
                max_memory_bytes: 64 * 1024 * 1024,
                hard_memory_limit: 80 * 1024 * 1024,
                max_tag_cardinality: 100_000,
            };
            let mt = ColumnarMemtable::new(schema, config);
            self.columnar_memtables.insert(key.clone(), mt);
        }

        // Schema evolution: detect new fields and expand memtable schema.
        let cols_before = if !is_new_memtable {
            self.columnar_memtables
                .get(&key)
                .map(|mt| mt.schema().columns.len())
                .unwrap_or(0)
        } else {
            0
        };
        if !is_new_memtable && let Some(mt) = self.columnar_memtables.get_mut(&key) {
            ilp_ingest::evolve_schema(mt, &lines);
        }
        let schema_changed = !is_new_memtable
            && self
                .columnar_memtables
                .get(&key)
                .is_some_and(|mt| mt.schema().columns.len() != cols_before);

        // Pre-flush: flush BEFORE ingesting if memtable is at the soft limit
        // OR if the timeseries engine budget is exhausted (governor pressure).
        let governor_pressure = self
            .governor
            .as_ref()
            .is_some_and(|g| g.try_reserve(nodedb_mem::EngineId::Timeseries, 0).is_err());
        if let Some(mt) = self.columnar_memtables.get(&key)
            && (mt.memory_bytes() >= 64 * 1024 * 1024 || governor_pressure)
        {
            self.flush_ts_collection(tid, collection, now_ms);
        }

        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after init: {collection}"),
                },
            );
        };
        // Reserve memory budget for this batch (~24 bytes per ILP line estimate).
        let batch_estimate = lines.len() * 24;
        if let Some(ref gov) = self.governor {
            let _ = gov.try_reserve(nodedb_mem::EngineId::Timeseries, batch_estimate);
        }

        let lvc = self.ts_last_value_caches.get_mut(&key);
        let mut series_keys = HashMap::new();
        let (mut accepted, rejected) =
            ilp_ingest::ingest_batch_with_lvc(mt, &lines, &mut series_keys, now_ms, lvc);

        // If rows were rejected (memtable hit hard limit), flush and re-ingest.
        if rejected > 0 {
            tracing::warn!(
                collection,
                accepted,
                rejected,
                "ILP batch rows rejected by hard limit, flushing and retrying"
            );
            self.flush_ts_collection(tid, collection, now_ms);
            if let Some(mt) = self.columnar_memtables.get_mut(&key) {
                let mut retry_keys = HashMap::new();
                let retry_lines = &lines[accepted..];
                let retry_lvc = self.ts_last_value_caches.get_mut(&key);
                let (retry_accepted, _) = ilp_ingest::ingest_batch_with_lvc(
                    mt,
                    retry_lines,
                    &mut retry_keys,
                    now_ms,
                    retry_lvc,
                );
                accepted += retry_accepted;
            }
        }

        // Post-flush: standard 64MB threshold check.
        let Some(mt) = self.columnar_memtables.get(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after ingest: {collection}"),
                },
            );
        };
        if mt.memory_bytes() >= 64 * 1024 * 1024 {
            self.flush_ts_collection(tid, collection, now_ms);
        }

        // Track WAL LSN and last ingest time for dedup + idle flush.
        if accepted > 0 {
            if let Some(lsn) = wal_lsn {
                let entry = self.ts_max_ingested_lsn.entry(key.clone()).or_insert(0);
                *entry = (*entry).max(lsn);
            }
            self.last_ts_ingest = Some(std::time::Instant::now());
        }

        self.checkpoint_coordinator
            .mark_dirty("timeseries", accepted);

        // Include schema_columns when schema is new OR evolved.
        let include_schema = is_new_memtable || schema_changed;
        let result = if include_schema && let Some(mt) = self.columnar_memtables.get(&key) {
            let schema_columns: Vec<serde_json::Value> = mt
                .schema()
                .columns
                .iter()
                .map(|(name, col_type)| {
                    let type_str = match col_type {
                        ColumnType::Timestamp => "TIMESTAMP",
                        ColumnType::Float64 => "FLOAT",
                        ColumnType::Int64 => "BIGINT",
                        ColumnType::Symbol => "VARCHAR",
                    };
                    serde_json::json!([name, type_str])
                })
                .collect();
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
                "schema_columns": schema_columns,
            })
        } else {
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
            })
        };
        let json = match response_codec::encode_json(&result) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(json),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }

    /// Ingest JSON row objects from SQL INSERT path.
    ///
    /// Payload is a msgpack array of maps (same schema as JSON ingest but in msgpack).
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    fn execute_msgpack_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        wal_lsn: Option<u64>,
        now_ms: i64,
    ) -> Response {
        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-] allowed"
                    ),
                },
            );
        }

        let rows = match msgpack_decode::decode_msgpack_rows(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("msgpack decode error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty msgpack rows array".into(),
                },
            );
        }

        let mut ilp_buf = String::new();
        for row in &rows {
            let mut fields = Vec::new();
            let mut timestamp_ns: Option<i64> = None;

            for (key, val) in row {
                let lower = key.to_lowercase();
                if lower == "ts" || lower == "timestamp" || lower == "time" {
                    match val {
                        MsgpackValue::Str(s) => {
                            timestamp_ns = parse_ts_string_to_nanos(s);
                        }
                        MsgpackValue::Int(n) => {
                            timestamp_ns = Some(*n * 1_000_000);
                        }
                        MsgpackValue::Float(f) => {
                            timestamp_ns = Some(*f as i64 * 1_000_000);
                        }
                        _ => {}
                    }
                    continue;
                }

                match val {
                    MsgpackValue::Float(f) => fields.push(format!("{key}={f}")),
                    MsgpackValue::Int(n) => fields.push(format!("{key}={n}i")),
                    MsgpackValue::Str(s) => {
                        fields.push(format!("{key}=\"{}\"", s.replace('\"', "\\\"")));
                    }
                    MsgpackValue::Bool(b) => fields.push(format!("{key}={b}")),
                    _ => {}
                }
            }

            if fields.is_empty() {
                continue;
            }

            ilp_buf.push_str(measurement);
            ilp_buf.push(' ');
            ilp_buf.push_str(&fields.join(","));
            if let Some(ts) = timestamp_ns {
                ilp_buf.push(' ');
                ilp_buf.push_str(&ts.to_string());
            }
            ilp_buf.push('\n');
        }

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in msgpack payload".into(),
                },
            );
        }

        self.execute_ilp_ingest(task, tid, collection, ilp_buf.as_bytes(), wal_lsn, now_ms)
    }

    /// Payload is a JSON array like: `[{"id":"e1","ts":"2024-01-01T00:00:00Z","value":42.0}]`.
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    fn execute_json_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        wal_lsn: Option<u64>,
        now_ms: i64,
    ) -> Response {
        let rows: sonic_rs::Array = match sonic_rs::from_slice(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("JSON parse error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty JSON rows array".into(),
                },
            );
        }

        // Convert JSON rows to ILP text.
        // The collection name serves as the ILP measurement.
        // Strip tenant scope prefix if present (e.g., "1:events" → "events").
        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        // Validate measurement name: only [a-zA-Z0-9_-] allowed.
        // ILP special characters (space, comma, =, newline) in measurement names corrupt the line.
        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-] allowed"
                    ),
                },
            );
        }

        let mut ilp_buf = String::new();
        for row_val in rows.iter() {
            let obj = match row_val.as_object() {
                Some(o) => o,
                None => continue,
            };

            let mut fields = Vec::new();
            let mut timestamp_ns: Option<i64> = None;

            for (key, val) in obj.iter() {
                let lower = key.to_lowercase();
                if lower == "ts" || lower == "timestamp" || lower == "time" {
                    if let Some(s) = val.as_str() {
                        timestamp_ns = parse_ts_string_to_nanos(s);
                    } else if let Some(n) = val.as_i64() {
                        // Treat integer timestamps as milliseconds → convert to ns.
                        timestamp_ns = Some(n * 1_000_000);
                    } else if let Some(f) = val.as_f64() {
                        timestamp_ns = Some(f as i64 * 1_000_000);
                    }
                    continue;
                }

                if let Some(f) = val.as_f64() {
                    fields.push(format!("{key}={f}"));
                } else if let Some(n) = val.as_i64() {
                    fields.push(format!("{key}={n}i"));
                } else if let Some(s) = val.as_str() {
                    fields.push(format!("{key}=\"{}\"", s.replace('\"', "\\\"")));
                } else if let Some(b) = val.as_bool() {
                    fields.push(format!("{key}={b}"));
                }
            }

            if fields.is_empty() {
                continue;
            }

            ilp_buf.push_str(measurement);
            ilp_buf.push(' ');
            ilp_buf.push_str(&fields.join(","));
            if let Some(ts) = timestamp_ns {
                ilp_buf.push(' ');
                ilp_buf.push_str(&ts.to_string());
            }
            ilp_buf.push('\n');
        }

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in JSON payload".into(),
                },
            );
        }

        // Delegate to the ILP ingest path.
        self.execute_ilp_ingest(task, tid, collection, ilp_buf.as_bytes(), wal_lsn, now_ms)
    }
}

/// Parse a datetime string to nanoseconds since Unix epoch.
///
/// Accepts RFC3339 / ISO8601 with timezone (e.g., "2024-01-01T00:00:00Z"),
/// and common datetime formats without timezone (treated as UTC).
/// Returns nanoseconds since Unix epoch, or `None` if the string cannot be parsed.
fn parse_ts_string_to_nanos(s: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

    // Try RFC3339 first (includes timezone info).
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_nanos_opt();
    }

    // Try common space-separated format "YYYY-MM-DD HH:MM:SS[.frac]".
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for fmt in &formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Utc.from_utc_datetime(&ndt).timestamp_nanos_opt();
        }
    }

    None
}
