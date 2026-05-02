//! Helper for projecting post-write documents into a `RowsPayload`.
//!
//! Called by PointUpdate, BulkUpdate, PointDelete, BulkDelete handlers
//! when the plan carries a `ReturningSpec`. Produces a `RowsPayload` msgpack
//! blob that the Control Plane decodes into multi-column pgwire rows.

use crate::bridge::physical_plan::{ReturningColumns, ReturningSpec};
use crate::data::executor::response_codec::RowsPayload;

/// Project a slice of documents per `spec` and encode as a `RowsPayload` msgpack blob.
///
/// For `ReturningColumns::Star`, all fields in each document are emitted in
/// insertion order (JSON object key order). For `ReturningColumns::Named`,
/// only the named fields are emitted in spec order. Missing fields and JSON
/// nulls are encoded as `None` so the Control Plane can emit a real SQL NULL.
pub(super) fn build_rows_payload(
    spec: &ReturningSpec,
    docs: &[serde_json::Value],
) -> crate::Result<Vec<u8>> {
    let (columns, source_names) = match &spec.columns {
        ReturningColumns::Star => {
            if docs.is_empty() {
                return encode_empty(Vec::new());
            }
            // Derive column names from the first doc's keys; both output and
            // source names are identical for `RETURNING *`.
            let cols: Vec<String> = docs
                .first()
                .and_then(|d| d.as_object())
                .map(|obj| obj.keys().cloned().collect())
                .unwrap_or_default();
            (cols.clone(), cols)
        }
        ReturningColumns::Named(items) => {
            let output_names: Vec<String> = items
                .iter()
                .map(|item| item.alias.clone().unwrap_or_else(|| item.name.clone()))
                .collect();
            let source_names: Vec<String> = items.iter().map(|item| item.name.clone()).collect();
            (output_names, source_names)
        }
    };

    let rows: Vec<Vec<Option<String>>> = docs
        .iter()
        .map(|doc| project_row(doc, &source_names))
        .collect();

    let payload = RowsPayload { columns, rows };
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Codec {
        detail: format!("RowsPayload encode: {e}"),
    })
}

fn encode_empty(columns: Vec<String>) -> crate::Result<Vec<u8>> {
    let payload = RowsPayload {
        columns,
        rows: Vec::new(),
    };
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Codec {
        detail: format!("RowsPayload encode empty: {e}"),
    })
}

/// Project a single document into one cell per source name.
///
/// Returns `None` for missing fields or JSON null, `Some(text)` otherwise.
fn project_row(doc: &serde_json::Value, source_names: &[String]) -> Vec<Option<String>> {
    let obj = doc.as_object();
    source_names
        .iter()
        .map(|name| obj.and_then(|o| o.get(name)).and_then(value_to_text))
        .collect()
}

/// Convert a JSON value to its TEXT representation for pgwire.
///
/// Returns `None` for JSON null so the Control Plane emits a real SQL NULL
/// rather than the string "null". Strings are returned as-is (no extra
/// quotes); numbers, booleans, arrays, and objects use JSON text.
fn value_to_text(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}
