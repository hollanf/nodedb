//! Strict document encoding/decoding: Value ↔ Binary Tuple.
//!
//! All internal encoding uses `nodedb_types::Value` directly — no JSON intermediary.
//! JSON is only produced at the read boundary (`binary_tuple_to_json`) for pgwire clients.

use nodedb_types::columnar::{ColumnType, StrictSchema};
use nodedb_types::value::Value;

/// Encode a `nodedb_types::Value::Object` as a Binary Tuple according to the schema.
///
/// Accepts zerompk bytes (from planner) and decodes them internally.
/// Missing nullable columns become NULL; missing non-nullable columns error.
pub(super) fn bytes_to_binary_tuple(
    bytes: &[u8],
    schema: &StrictSchema,
) -> Result<Vec<u8>, String> {
    let value =
        nodedb_types::value_from_msgpack(bytes).map_err(|e| format!("zerompk decode: {e}"))?;
    value_to_binary_tuple(&value, schema)
}

/// Encode a `nodedb_types::Value` as a Binary Tuple according to the schema.
pub(super) fn value_to_binary_tuple(
    value: &Value,
    schema: &StrictSchema,
) -> Result<Vec<u8>, String> {
    let map = match value {
        Value::Object(m) => m,
        _ => return Err("strict value must be an Object".to_string()),
    };

    let encoder = nodedb_strict::TupleEncoder::new(schema);
    let mut values = Vec::with_capacity(schema.columns.len());

    for col in &schema.columns {
        let field_val = map.get(&col.name);
        let typed = match field_val {
            None | Some(Value::Null) => {
                if !col.nullable {
                    return Err(format!(
                        "column '{}' is NOT NULL but no value provided",
                        col.name
                    ));
                }
                Value::Null
            }
            Some(v) => coerce_value(v, &col.column_type, &col.name)?,
        };
        values.push(typed);
    }

    encoder
        .encode(&values)
        .map_err(|e| format!("Binary Tuple encode: {e}"))
}

/// Decode a Binary Tuple to a JSON object using the schema (for pgwire output).
pub(super) fn binary_tuple_to_json(
    tuple_bytes: &[u8],
    schema: &StrictSchema,
) -> Option<serde_json::Value> {
    let decoder = nodedb_strict::TupleDecoder::new(schema);
    let values = decoder.extract_all(tuple_bytes).ok()?;

    let mut obj = serde_json::Map::with_capacity(schema.columns.len());
    for (i, col) in schema.columns.iter().enumerate() {
        obj.insert(col.name.clone(), value_to_json(&values[i]));
    }
    Some(serde_json::Value::Object(obj))
}

/// Coerce a `nodedb_types::Value` to match a column's declared type.
fn coerce_value(val: &Value, col_type: &ColumnType, col_name: &str) -> Result<Value, String> {
    match col_type {
        ColumnType::Bool => match val {
            Value::Bool(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Bool(*n != 0)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(format!("column '{col_name}': cannot coerce '{s}' to BOOL")),
            },
            _ => Err(format!("column '{col_name}': expected BOOL, got {val:?}")),
        },
        ColumnType::Int64 => match val {
            Value::Integer(_) => Ok(val.clone()),
            Value::Float(f) => Ok(Value::Integer(*f as i64)),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| format!("column '{col_name}': cannot parse '{s}' as INT")),
            _ => Err(format!("column '{col_name}': expected INT, got {val:?}")),
        },
        ColumnType::Float64 => match val {
            Value::Float(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Float(*n as f64)),
            Value::String(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| format!("column '{col_name}': cannot parse '{s}' as FLOAT")),
            _ => Err(format!("column '{col_name}': expected FLOAT, got {val:?}")),
        },
        ColumnType::String | ColumnType::Uuid => match val {
            Value::String(_) | Value::Uuid(_) | Value::Ulid(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::String(n.to_string())),
            Value::Float(f) => Ok(Value::String(f.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            other => Ok(Value::String(format!("{other:?}"))),
        },
        ColumnType::Bytes => match val {
            Value::Bytes(_) => Ok(val.clone()),
            Value::String(s) => {
                let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_else(|_| s.as_bytes().to_vec());
                Ok(Value::Bytes(bytes))
            }
            _ => Err(format!("column '{col_name}': expected BYTES, got {val:?}")),
        },
        ColumnType::Timestamp => match val {
            Value::DateTime(_) => Ok(val.clone()),
            Value::Integer(ms) => Ok(Value::DateTime(nodedb_types::NdbDateTime::from_millis(*ms))),
            Value::Float(f) => Ok(Value::DateTime(nodedb_types::NdbDateTime::from_millis(
                *f as i64,
            ))),
            Value::String(s) => {
                if let Ok(ms) = s.parse::<i64>() {
                    Ok(Value::DateTime(nodedb_types::NdbDateTime::from_millis(ms)))
                } else {
                    Ok(Value::String(s.clone()))
                }
            }
            _ => Err(format!(
                "column '{col_name}': expected TIMESTAMP, got {val:?}"
            )),
        },
        ColumnType::Decimal => match val {
            Value::Decimal(_) => Ok(val.clone()),
            Value::Float(f) => rust_decimal::Decimal::try_from(*f)
                .map(Value::Decimal)
                .map_err(|_| format!("column '{col_name}': cannot convert {f} to DECIMAL")),
            Value::Integer(n) => Ok(Value::Decimal(rust_decimal::Decimal::from(*n))),
            Value::String(s) => s
                .parse::<rust_decimal::Decimal>()
                .map(Value::Decimal)
                .map_err(|_| format!("column '{col_name}': cannot parse '{s}' as DECIMAL")),
            _ => Err(format!(
                "column '{col_name}': expected DECIMAL, got {val:?}"
            )),
        },
        ColumnType::Vector(dim) => match val {
            Value::Bytes(b) if b.len() == *dim as usize * 4 => Ok(val.clone()),
            Value::Array(arr) => {
                let floats: Vec<f32> = arr
                    .iter()
                    .filter_map(|v| match v {
                        Value::Float(f) => Some(*f as f32),
                        Value::Integer(n) => Some(*n as f32),
                        _ => None,
                    })
                    .collect();
                if floats.len() != *dim as usize {
                    return Err(format!(
                        "column '{col_name}': expected VECTOR({dim}), got {} elements",
                        floats.len()
                    ));
                }
                let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();
                Ok(Value::Bytes(bytes))
            }
            _ => Err(format!(
                "column '{col_name}': expected VECTOR array, got {val:?}"
            )),
        },
        ColumnType::Geometry => Ok(Value::String(format!("{val:?}"))),
    }
}

/// Convert a typed `Value` to JSON (for pgwire output only).
fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        Value::DateTime(dt) => serde_json::json!(dt.micros / 1000),
        Value::Duration(d) => serde_json::json!(d.as_millis()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Object(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        Value::Geometry(_)
        | Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. } => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::ColumnDef;

    fn test_schema() -> StrictSchema {
        StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("age", ColumnType::Int64),
            ],
            version: 1,
        }
    }

    #[test]
    fn roundtrip_via_value() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Alice".into()));
        map.insert("age".into(), Value::Integer(30));

        let tuple_bytes = value_to_binary_tuple(&Value::Object(map), &schema).unwrap();
        let decoded = binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u1");
        assert_eq!(decoded["name"], "Alice");
        assert_eq!(decoded["age"], 30);
    }

    #[test]
    fn nullable_field_omitted() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u2".into()));
        map.insert("name".into(), Value::String("Bob".into()));

        let tuple_bytes = value_to_binary_tuple(&Value::Object(map), &schema).unwrap();
        let decoded = binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u2");
        assert!(decoded["age"].is_null());
    }

    #[test]
    fn non_nullable_missing_errors() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u3".into()));

        let result = value_to_binary_tuple(&Value::Object(map), &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("NOT NULL"));
    }
}
