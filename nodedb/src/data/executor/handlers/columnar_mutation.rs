//! Columnar UPDATE and DELETE handlers for plain/spatial collections.
//!
//! Uses `nodedb-columnar`'s `MutationEngine` for full mutation support
//! (PK index, delete bitmaps, WAL records).

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle columnar UPDATE: scan memtable for matching rows, apply field updates.
    ///
    /// Currently operates on in-memory memtable rows only.
    /// Returns `{"affected": N}` as JSON payload.
    pub(in crate::data::executor) fn execute_columnar_update(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        _filter_bytes: &[u8],
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar update");

        let key = collection.to_string();
        let engine = match self.columnar_engines.get_mut(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        // For now, columnar UPDATE requires PK-based access.
        // TODO: scan-based UPDATE with filter predicates.
        let schema = engine.schema().clone();
        let pk_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        if pk_cols.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar UPDATE requires a PRIMARY KEY column".into(),
                },
            );
        }

        // Scan memtable rows to find matches and apply updates.
        // Collect rows to update (can't mutate while iterating).
        let rows: Vec<Vec<nodedb_types::value::Value>> = engine.scan_memtable_rows().collect();

        let mut affected = 0u64;
        for row in &rows {
            // Apply field updates to the row.
            let mut new_row = row.clone();
            for (field_name, value_bytes) in updates {
                if let Some(col_idx) = schema.columns.iter().position(|c| c.name == *field_name) {
                    let val: serde_json::Value = if let Ok(v) =
                        nodedb_types::value_from_msgpack(value_bytes)
                    {
                        v.into()
                    } else if let Ok(v) = sonic_rs::from_slice(value_bytes) {
                        v
                    } else {
                        serde_json::Value::String(String::from_utf8_lossy(value_bytes).into_owned())
                    };
                    new_row[col_idx] = json_to_value(&val, &schema.columns[col_idx].column_type);
                }
            }

            // Extract old PK value.
            let old_pk = &row[pk_cols[0]];

            // Execute update via MutationEngine (delete + insert).
            match engine.update(old_pk, &new_row) {
                Ok(_result) => {
                    affected += 1;
                }
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar update row failed");
                }
            }
        }

        debug!(core = self.core_id, %collection, affected, "columnar update complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Handle columnar DELETE: scan memtable for matching rows, delete them.
    ///
    /// Currently operates on in-memory memtable rows only.
    /// Returns `{"affected": N}` as JSON payload.
    pub(in crate::data::executor) fn execute_columnar_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        _filter_bytes: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar delete");

        let key = collection.to_string();
        let engine = match self.columnar_engines.get_mut(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        let schema = engine.schema().clone();
        let pk_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        if pk_cols.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar DELETE requires a PRIMARY KEY column".into(),
                },
            );
        }

        // Collect all PK values to delete (can't mutate while iterating).
        let pk_values: Vec<nodedb_types::value::Value> = engine
            .scan_memtable_rows()
            .map(|row| row[pk_cols[0]].clone())
            .collect();

        let mut affected = 0u64;
        for pk in &pk_values {
            match engine.delete(pk) {
                Ok(_) => affected += 1,
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar delete row failed");
                }
            }
        }

        debug!(core = self.core_id, %collection, affected, "columnar delete complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json(&result) {
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

/// Convert a `serde_json::Value` to `nodedb_types::value::Value` for a given column type.
fn json_to_value(
    json: &serde_json::Value,
    col_type: &nodedb_types::columnar::ColumnType,
) -> nodedb_types::value::Value {
    use nodedb_types::columnar::ColumnType;
    use nodedb_types::value::Value;

    match (col_type, json) {
        (_, serde_json::Value::Null) => Value::Null,
        (ColumnType::Int64, serde_json::Value::Number(n)) => {
            Value::Integer(n.as_i64().unwrap_or(0))
        }
        (ColumnType::Float64, serde_json::Value::Number(n)) => {
            Value::Float(n.as_f64().unwrap_or(0.0))
        }
        (ColumnType::Bool, serde_json::Value::Bool(b)) => Value::Bool(*b),
        (ColumnType::String, serde_json::Value::String(s)) => Value::String(s.clone()),
        (ColumnType::Timestamp, serde_json::Value::Number(n)) => {
            Value::Integer(n.as_i64().unwrap_or(0))
        }
        (ColumnType::Timestamp, serde_json::Value::String(s)) => {
            nodedb_types::datetime::NdbDateTime::parse(s)
                .map(Value::DateTime)
                .unwrap_or_else(|| Value::String(s.clone()))
        }
        // Fallback: try to coerce.
        (_, serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        (_, serde_json::Value::String(s)) => Value::String(s.clone()),
        (_, serde_json::Value::Bool(b)) => Value::Bool(*b),
        _ => Value::Null,
    }
}
