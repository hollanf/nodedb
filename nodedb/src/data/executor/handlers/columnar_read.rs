//! Columnar base scan handler.
//!
//! Reads rows from the `MutationEngine` memtable, applies projection and
//! limit. Used by plain columnar and spatial collections.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a base columnar scan: read from MutationEngine memtable.
    pub(in crate::data::executor) fn execute_columnar_scan(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        projection: &[String],
        limit: usize,
        _filters: &[u8],
        _rls_filters: &[u8],
    ) -> Response {
        let limit = if limit == 0 { 1000 } else { limit };

        let engine = match self.columnar_engines.get(collection) {
            Some(e) => e,
            None => {
                // Empty result for missing collection.
                return match response_codec::encode_json_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        let schema = engine.schema();
        let mut results = Vec::new();

        for row in engine.scan_memtable_rows().take(limit) {
            let mut obj = serde_json::Map::new();
            for (i, col_def) in schema.columns.iter().enumerate() {
                if !projection.is_empty() && !projection.iter().any(|p| p == &col_def.name) {
                    continue;
                }
                if i < row.len() {
                    obj.insert(col_def.name.clone(), value_to_json(&row[i]));
                }
            }
            results.push(serde_json::Value::Object(obj));
        }

        match response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

/// Convert a `nodedb_types::Value` to `serde_json::Value` for response encoding.
fn value_to_json(val: &nodedb_types::value::Value) -> serde_json::Value {
    use nodedb_types::value::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Uuid(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        _ => serde_json::Value::Null,
    }
}

/// Convert a timeseries columnar memtable cell to a JSON value.
///
/// Used by timeseries raw_scan and aggregate handlers that still use the
/// internal `ColumnarMemtable` (timeseries-specific, not yet migrated).
pub(super) fn emit_column_value(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    col_type: &crate::engine::timeseries::columnar_memtable::ColumnType,
    col_data: &crate::engine::timeseries::columnar_memtable::ColumnData,
    row_idx: usize,
) -> serde_json::Value {
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData as TsColumnData, ColumnType as TsColumnType,
    };
    match col_type {
        TsColumnType::Timestamp => {
            serde_json::Value::Number(serde_json::Number::from(col_data.as_timestamps()[row_idx]))
        }
        TsColumnType::Float64 => {
            let v = col_data.as_f64()[row_idx];
            serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        TsColumnType::Symbol => {
            if let TsColumnData::Symbol(ids) = col_data {
                let sym_id = ids[row_idx];
                mt.symbol_dict(col_idx)
                    .and_then(|dict| dict.get(sym_id))
                    .map(|s| serde_json::Value::String(s.to_string()))
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::Null
            }
        }
        TsColumnType::Int64 => {
            if let TsColumnData::Int64(vals) = col_data {
                serde_json::Value::Number(serde_json::Number::from(vals[row_idx]))
            } else {
                serde_json::Value::Null
            }
        }
    }
}
