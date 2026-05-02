//! Value conversion and SQL formatting helpers for the remote client.
//!
//! Extracts pgwire row/column conversion, JSON-to-Value mapping, and
//! SQL identifier quoting into a focused module.

use nodedb_types::Value;
use nodedb_types::error::{NodeDbError, NodeDbResult};

/// Convert a `tokio_postgres` column value to `nodedb_types::Value`.
pub(crate) fn pg_value_to_value(
    row: &tokio_postgres::Row,
    idx: usize,
    ty: &tokio_postgres::types::Type,
) -> Value {
    use tokio_postgres::types::Type;

    match *ty {
        Type::BOOL => row
            .try_get::<_, bool>(idx)
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        Type::INT2 => row
            .try_get::<_, i16>(idx)
            .map(|v| Value::Integer(v as i64))
            .unwrap_or(Value::Null),
        Type::INT4 => row
            .try_get::<_, i32>(idx)
            .map(|v| Value::Integer(v as i64))
            .unwrap_or(Value::Null),
        Type::INT8 => row
            .try_get::<_, i64>(idx)
            .map(Value::Integer)
            .unwrap_or(Value::Null),
        Type::FLOAT4 => row
            .try_get::<_, f32>(idx)
            .map(|v| Value::Float(v as f64))
            .unwrap_or(Value::Null),
        Type::FLOAT8 => row
            .try_get::<_, f64>(idx)
            .map(Value::Float)
            .unwrap_or(Value::Null),
        Type::TEXT | Type::VARCHAR | Type::NAME => row
            .try_get::<_, String>(idx)
            .map(Value::String)
            .unwrap_or(Value::Null),
        Type::BYTEA => row
            .try_get::<_, Vec<u8>>(idx)
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        Type::JSON | Type::JSONB => row
            .try_get::<_, serde_json::Value>(idx)
            .map(|v| json_to_value(&v))
            .unwrap_or(Value::Null),
        _ => {
            // Fallback: try as string.
            row.try_get::<_, String>(idx)
                .map(Value::String)
                .unwrap_or(Value::Null)
        }
    }
}

/// Convert `serde_json::Value` to `nodedb_types::Value`.
pub(crate) fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(a) => Value::Array(a.iter().map(json_to_value).collect()),
        serde_json::Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

/// Quote a SQL identifier to prevent injection. Doubles any internal
/// double-quotes and wraps in double-quotes: `foo` → `"foo"`.
pub(crate) fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Decode a pgwire-decoded integer column as `u64`.
pub(crate) fn value_as_u64(v: &Value) -> NodeDbResult<u64> {
    match v {
        Value::Integer(i) => Ok(*i as u64),
        other => Err(NodeDbError::storage(format!(
            "expected integer for u64 column, got {other:?}"
        ))),
    }
}

/// Decode a pgwire-decoded string column. `Null` projects to the
/// empty string so downstream callers don't have to special-case
/// nullable text columns for owner lookups.
pub(crate) fn value_as_string(v: &Value) -> NodeDbResult<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Null => Ok(String::new()),
        other => Err(NodeDbError::storage(format!(
            "expected string column, got {other:?}"
        ))),
    }
}

/// Format an f32 slice as a SQL ARRAY literal: `ARRAY[0.1,0.2,0.3]`.
pub(crate) fn format_vector_array(v: &[f32]) -> String {
    let inner: Vec<String> = v.iter().map(|f| format!("{f}")).collect();
    format!("ARRAY[{}]", inner.join(","))
}
