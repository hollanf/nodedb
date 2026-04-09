//! KV Scan handler and filter extraction.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_kv_scan(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        cursor: &[u8],
        count: usize,
        match_pattern: Option<&str>,
        filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, count, "kv scan");
        let now_ms = current_ms();

        // Try to extract a single equality filter for index pushdown.
        let (filter_field, filter_value) = extract_eq_filter(filters);
        let (entries, next_cursor) = self.kv_engine.scan(
            tid,
            collection,
            cursor,
            count,
            now_ms,
            match_pattern,
            filter_field.as_deref(),
            filter_value.as_deref(),
        );

        // Parse filter predicates for post-scan evaluation.
        // Index pushdown handles eq filters on indexed fields, but general
        // predicates (gt, lt, in, etc.) need post-scan evaluation.
        let filter_predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if !filters.is_empty()
        {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Build results as raw msgpack — no serde_json::Value intermediary.
        let mut result_entries: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        for (k, v) in &entries {
            let key_str = String::from_utf8_lossy(k);
            // Inject "key" field into msgpack map without decode/re-encode.
            let entry_mp = nodedb_query::msgpack_scan::inject_str_field(v, "key", &key_str);

            // Apply filter predicates post-scan (already works on raw msgpack).
            if !filter_predicates.is_empty()
                && !filter_predicates
                    .iter()
                    .all(|f| f.matches_binary(&entry_mp))
            {
                continue;
            }

            result_entries.push(entry_mp);
        }

        // Build response as raw msgpack map: {"entries": [...], "next_cursor"?: "..."}
        let field_count = if next_cursor.is_empty() { 1 } else { 2 };
        let mut payload =
            Vec::with_capacity(result_entries.iter().map(|e| e.len()).sum::<usize>() + 64);
        nodedb_query::msgpack_scan::write_map_header(&mut payload, field_count);
        // "entries" array
        nodedb_query::msgpack_scan::write_str(&mut payload, "entries");
        nodedb_query::msgpack_scan::write_array_header(&mut payload, result_entries.len());
        for entry in &result_entries {
            payload.extend_from_slice(entry);
        }
        // Optional "next_cursor"
        if !next_cursor.is_empty() {
            use base64::Engine;
            let cursor_b64 = base64::engine::general_purpose::STANDARD.encode(&next_cursor);
            nodedb_query::msgpack_scan::write_kv_str(&mut payload, "next_cursor", &cursor_b64);
        }
        if let Some(ref m) = self.metrics {
            m.record_kv_scan();
        }
        self.response_with_payload(task, payload)
    }
}

/// Extract a single equality filter from serialized ScanFilter bytes.
///
/// Looks for the first `{"field": "x", "op": "eq", "value": "y"}` filter.
/// Returns `(Some(field), Some(value_bytes))` if found, `(None, None)` otherwise.
pub(in crate::data::executor) fn extract_eq_filter(
    filters: &[u8],
) -> (Option<String>, Option<Vec<u8>>) {
    if filters.is_empty() {
        return (None, None);
    }

    // Filters are MessagePack-encoded Vec<ScanFilter>.
    let Ok(parsed) = zerompk::from_msgpack::<Vec<nodedb_types::json_msgpack::JsonValue>>(filters)
        .map(|v| {
            v.into_iter()
                .map(|jv| jv.0)
                .collect::<Vec<serde_json::Value>>()
        })
    else {
        tracing::trace!(
            len = filters.len(),
            "filter deserialization failed, falling back to full scan"
        );
        return (None, None);
    };

    for filter in &parsed {
        let Some(field) = filter.get("field").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(op) = filter.get("op").and_then(|v| v.as_str()) else {
            continue;
        };
        if op != "eq" {
            continue;
        }
        let Some(value) = filter.get("value") else {
            continue;
        };

        let value_bytes = match value {
            serde_json::Value::String(s) => s.as_bytes().to_vec(),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    let sortable = (i as u64) ^ (1u64 << 63);
                    sortable.to_be_bytes().to_vec()
                } else {
                    n.to_string().into_bytes()
                }
            }
            other => other.to_string().into_bytes(),
        };

        return (Some(field.to_string()), Some(value_bytes));
    }

    (None, None)
}
