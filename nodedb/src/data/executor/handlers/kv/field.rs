//! KV field-level operation handlers: FieldGet, FieldSet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_field_get(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        fields: &[String],
    ) -> Response {
        debug!(core = self.core_id, %collection, field_count = fields.len(), "kv field get");
        let now_ms = current_ms();

        // Get the full value.
        let Some(value) = self.kv_engine.get(tid, collection, key, now_ms) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Deserialize as nodedb_types::Value.
        let doc = match nodedb_types::value_from_msgpack(&value) {
            Ok(nodedb_types::Value::Object(map)) => map,
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "value is not a msgpack-encoded object".into(),
                    },
                );
            }
        };

        // Extract requested fields.
        let result: std::collections::HashMap<String, nodedb_types::Value> = fields
            .iter()
            .map(|f| {
                let v = doc.get(f).cloned().unwrap_or(nodedb_types::Value::Null);
                (f.clone(), v)
            })
            .collect();

        let result_value = nodedb_types::Value::Object(result);
        match response_codec::encode(&result_value) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_field_set(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, field_count = updates.len(), "kv field set");
        let now_ms = current_ms();

        // Read current value.
        let current = self.kv_engine.get(tid, collection, key, now_ms);

        let mut doc: std::collections::HashMap<String, nodedb_types::Value> = current
            .as_ref()
            .and_then(|v| nodedb_types::value_from_msgpack(v).ok())
            .and_then(|v| {
                if let nodedb_types::Value::Object(m) = v {
                    Some(m)
                } else {
                    None
                }
            })
            .unwrap_or_default();

        // Merge field updates, tracking how many fields are new (not previously existing).
        let mut fields_added = 0u64;
        for (field, value_bytes) in updates {
            let new_value =
                nodedb_types::value_from_msgpack(value_bytes).unwrap_or(nodedb_types::Value::Null);
            if !doc.contains_key(field) {
                fields_added += 1;
            }
            doc.insert(field.clone(), new_value);
        }

        // Serialize back to MessagePack and write.
        let new_value = match nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(doc)) {
            Ok(v) => v,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("field set serialization: {e}"),
                    },
                );
            }
        };

        self.kv_engine
            .put(tid, collection, key, &new_value, 0, now_ms);
        match response_codec::encode_json(&serde_json::json!({ "fields_added": fields_added })) {
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
