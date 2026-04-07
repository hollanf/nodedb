//! Columnar base insert handler.
//!
//! Writes rows to `nodedb-columnar`'s `MutationEngine`. Accepts JSON payload
//! (array of objects). Creates the engine on first insert with schema inferred
//! from the first row.

use nodedb_columnar::MutationEngine;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a columnar insert: write rows from JSON payload to MutationEngine.
    pub(in crate::data::executor) fn execute_columnar_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        payload: &[u8],
        _format: &str,
    ) -> Response {
        // Parse payload: zerompk array/object of nodedb_types::Value.
        let ndb_rows: Vec<nodedb_types::Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(arr)) => arr,
            Ok(v @ nodedb_types::Value::Object(_)) => vec![v],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "payload must be an array or object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid payload: {e}"),
                    },
                );
            }
        };

        if ndb_rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        // Ensure MutationEngine exists (auto-create on first write).
        if !self.columnar_engines.contains_key(collection) {
            let schema = infer_schema_from_value(&ndb_rows[0]);
            let engine = MutationEngine::new(collection.to_string(), schema);
            self.columnar_engines.insert(collection.to_string(), engine);
        }

        let Some(engine) = self.columnar_engines.get_mut(collection) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar engine missing after create".into(),
                },
            );
        };

        let schema = engine.schema().clone();
        let mut accepted = 0u64;

        for row in &ndb_rows {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };

            // Build Value slice in schema order.
            let values: Vec<Value> = schema
                .columns
                .iter()
                .map(|col| ndb_field_to_value(obj.get(&col.name), &col.column_type))
                .collect();

            match engine.insert(&values) {
                Ok(_) => accepted += 1,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        %collection,
                        error = %e,
                        "columnar insert row failed"
                    );
                }
            }
        }

        self.checkpoint_coordinator
            .mark_dirty("columnar", accepted as usize);

        let result = serde_json::json!({
            "accepted": accepted,
            "collection": collection,
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
}

impl CoreLoop {
    /// Ingest a single JSON document into the columnar engine for a collection.
    ///
    /// Creates the engine on first call. Used by the spatial insert path.
    pub(in crate::data::executor) fn ingest_doc_to_columnar(
        &mut self,
        collection: &str,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) {
        if !self.columnar_engines.contains_key(collection) {
            let schema = infer_schema_from_json(&serde_json::Value::Object(obj.clone()));
            let engine = MutationEngine::new(collection.to_string(), schema);
            self.columnar_engines.insert(collection.to_string(), engine);
        }

        let Some(engine) = self.columnar_engines.get_mut(collection) else {
            return;
        };
        let schema = engine.schema().clone();

        let ndb_obj: std::collections::HashMap<String, Value> = obj
            .iter()
            .map(|(k, v)| (k.clone(), Value::from(v.clone())))
            .collect();
        let values: Vec<Value> = schema
            .columns
            .iter()
            .map(|col| ndb_field_to_value(ndb_obj.get(&col.name), &col.column_type))
            .collect();

        let _ = engine.insert(&values);
    }
}

/// Coerce a `nodedb_types::Value` field to match the column type.
fn ndb_field_to_value(val: Option<&Value>, col_type: &ColumnType) -> Value {
    let Some(val) = val else { return Value::Null };
    match (col_type, val) {
        (_, Value::Null) => Value::Null,
        (ColumnType::Int64, Value::Integer(_)) => val.clone(),
        (ColumnType::Int64, Value::Float(f)) => Value::Integer(*f as i64),
        (ColumnType::Int64, Value::String(s)) => {
            s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null)
        }
        (ColumnType::Float64, Value::Float(_)) => val.clone(),
        (ColumnType::Float64, Value::Integer(n)) => Value::Float(*n as f64),
        (ColumnType::Float64, Value::String(s)) => {
            s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
        }
        (ColumnType::Bool, Value::Bool(_)) => val.clone(),
        (ColumnType::String, Value::String(_)) => val.clone(),
        (ColumnType::Timestamp, Value::Integer(n)) => {
            Value::DateTime(nodedb_types::NdbDateTime::from_millis(*n))
        }
        (ColumnType::Timestamp, Value::Float(f)) => {
            Value::DateTime(nodedb_types::NdbDateTime::from_millis(*f as i64))
        }
        (ColumnType::Timestamp, Value::String(s)) => nodedb_types::datetime::NdbDateTime::parse(s)
            .map(Value::DateTime)
            .unwrap_or_else(|| Value::String(s.clone())),
        (ColumnType::Uuid, Value::String(_)) => val.clone(),
        // Fallback: integers as floats, strings as strings.
        (ColumnType::Float64, _) => Value::Null,
        (ColumnType::Int64, _) => Value::Null,
        _ => val.clone(),
    }
}

/// Infer a columnar schema from a `nodedb_types::Value::Object` (first row).
fn infer_schema_from_value(row: &Value) -> ColumnarSchema {
    let obj = match row {
        Value::Object(m) => m,
        _ => {
            return ColumnarSchema::new(vec![ColumnDef::required("value", ColumnType::Float64)])
                .expect("single-column schema");
        }
    };

    let mut columns = Vec::new();
    for (key, val) in obj {
        let lower = key.to_lowercase();
        let col_type = if lower == "timestamp" || lower == "ts" || lower == "time" {
            ColumnType::Timestamp
        } else {
            match val {
                Value::Float(_) => ColumnType::Float64,
                Value::Integer(_) => ColumnType::Int64,
                Value::Bool(_) => ColumnType::Bool,
                _ => ColumnType::String,
            }
        };
        if lower == "id" {
            columns.push(ColumnDef::required(key.clone(), col_type).with_primary_key());
        } else {
            columns.push(ColumnDef::nullable(key.clone(), col_type));
        }
    }

    if columns.is_empty() {
        columns.push(ColumnDef::required("value", ColumnType::Float64));
    }

    ColumnarSchema::new(columns).expect("inferred schema must be valid")
}

/// Infer a columnar schema from a JSON object — used by the spatial insert path.
pub(super) fn infer_schema_from_json(row: &serde_json::Value) -> ColumnarSchema {
    let ndb: Value = row.clone().into();
    infer_schema_from_value(&ndb)
}
