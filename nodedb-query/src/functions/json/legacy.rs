//! Existing JSON manipulation scalar functions (json_extract, json_set, etc.)

use std::collections::HashMap;

use crate::value_ops::value_to_display_string;
use nodedb_types::Value;

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "json_extract" | "json_get" => {
            let obj = args.first().unwrap_or(&Value::Null);
            let path = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let mut current = obj;
            for key in path.split('.') {
                current = match current {
                    Value::Object(map) => map.get(key).unwrap_or(&Value::Null),
                    Value::Array(arr) => key
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| arr.get(i))
                        .unwrap_or(&Value::Null),
                    _ => &Value::Null,
                };
            }
            current.clone()
        }
        "json_set" => {
            let mut obj = args
                .first()
                .cloned()
                .unwrap_or(Value::Object(HashMap::new()));
            let key = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let val = args.get(2).cloned().unwrap_or(Value::Null);
            if let Value::Object(ref mut map) = obj {
                map.insert(key.to_string(), val);
            }
            obj
        }
        "json_remove" => {
            let mut obj = args.first().cloned().unwrap_or(Value::Null);
            let key = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            if let Value::Object(ref mut map) = obj {
                map.remove(key);
            }
            obj
        }
        "json_keys" => match args.first() {
            Some(Value::Object(map)) => {
                Value::Array(map.keys().map(|k| Value::String(k.clone())).collect())
            }
            _ => Value::Null,
        },
        "json_values" => match args.first() {
            Some(Value::Object(map)) => Value::Array(map.values().cloned().collect()),
            _ => Value::Null,
        },
        "json_length" | "json_len" => match args.first() {
            Some(Value::Object(map)) => Value::Integer(map.len() as i64),
            Some(Value::Array(arr)) => Value::Integer(arr.len() as i64),
            Some(Value::String(s)) => Value::Integer(s.len() as i64),
            _ => Value::Null,
        },
        "json_type" => {
            let type_name = match args.first() {
                Some(Value::Null) | None => "null",
                Some(Value::Bool(_)) => "boolean",
                Some(Value::Integer(_) | Value::Float(_) | Value::Decimal(_)) => "number",
                Some(Value::String(_)) => "string",
                Some(Value::Array(_)) => "array",
                Some(Value::Object(_)) => "object",
                Some(_) => "string", // Uuid, DateTime, etc. appear as strings
            };
            Value::String(type_name.into())
        }
        "json_array" => Value::Array(args.to_vec()),
        "json_object" => {
            let mut map = HashMap::new();
            let mut i = 0;
            while i + 1 < args.len() {
                let key = value_to_display_string(&args[i]);
                let val = args[i + 1].clone();
                map.insert(key, val);
                i += 2;
            }
            Value::Object(map)
        }
        "json_contains" => {
            let container = args.first().unwrap_or(&Value::Null);
            let needle = args.get(1).unwrap_or(&Value::Null);
            let result = match container {
                Value::Array(arr) => arr.contains(needle),
                Value::Object(map) => {
                    if let Some(key) = needle.as_str() {
                        map.contains_key(key)
                    } else {
                        false
                    }
                }
                _ => false,
            };
            Value::Bool(result)
        }
        "json_merge" | "json_patch" => {
            let mut base = match args.first() {
                Some(Value::Object(m)) => m.clone(),
                _ => HashMap::new(),
            };
            if let Some(Value::Object(overlay)) = args.get(1) {
                for (k, v) in overlay {
                    base.insert(k.clone(), v.clone());
                }
            }
            Value::Object(base)
        }
        _ => return None,
    };
    Some(v)
}
