//! Conversion utilities for CRDT value types.

/// Convert a `serde_json::Value` to a `loro::LoroValue`.
pub(super) fn json_to_loro_value(val: &serde_json::Value) -> loro::LoroValue {
    match val {
        serde_json::Value::Null => loro::LoroValue::Null,
        serde_json::Value::Bool(b) => loro::LoroValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                loro::LoroValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                loro::LoroValue::Double(f)
            } else {
                loro::LoroValue::Null
            }
        }
        serde_json::Value::String(s) => loro::LoroValue::String(s.clone().into()),
        serde_json::Value::Array(arr) => {
            let items: Vec<loro::LoroValue> = arr.iter().map(json_to_loro_value).collect();
            loro::LoroValue::List(items.into())
        }
        serde_json::Value::Object(map) => {
            let entries: std::collections::HashMap<String, loro::LoroValue> = map
                .iter()
                .map(|(k, v)| (k.clone(), json_to_loro_value(v)))
                .collect();
            loro::LoroValue::Map(entries.into())
        }
    }
}
