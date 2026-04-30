use sqlparser::ast::Value;

use crate::error::{Result, SqlError};
use crate::types::*;

/// Convert a sqlparser `Value` to our `SqlValue`.
///
/// Number literal routing:
/// - Pure integers → `SqlValue::Int`.
/// - Numbers with `.`, `e`, or `E` → `SqlValue::Decimal` (exact arithmetic).
/// - If decimal parse fails → fallback to `SqlValue::Float`, then `SqlValue::String`.
pub fn convert_value(val: &Value) -> Result<SqlValue> {
    match val {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(SqlValue::Int(i))
            } else if n.contains('.') || n.contains('e') || n.contains('E') {
                // Fractional or scientific notation: prefer exact Decimal.
                if let Ok(d) = rust_decimal::Decimal::from_str_exact(n) {
                    Ok(SqlValue::Decimal(d))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(SqlValue::Float(f))
                } else {
                    Ok(SqlValue::String(n.clone()))
                }
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(SqlValue::Float(f))
            } else {
                Ok(SqlValue::String(n.clone()))
            }
        }
        Value::SingleQuotedString(s) => Ok(SqlValue::String(s.clone())),
        Value::Boolean(b) => Ok(SqlValue::Bool(*b)),
        Value::Null => Ok(SqlValue::Null),
        _ => Err(SqlError::Unsupported {
            detail: format!("value literal: {val}"),
        }),
    }
}

/// Parse an interval string to microseconds.
///
/// Delegates to `nodedb_types::kv_parsing::parse_interval_to_ms` (ms → μs)
/// and `NdbDuration::parse` for compound shorthand forms.
pub(super) fn parse_interval_to_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Try NdbDuration::parse first (handles compound "1h30m", "500ms", "2d").
    if let Some(dur) = nodedb_types::NdbDuration::parse(s) {
        return Some(dur.micros);
    }

    // Delegate to shared interval parser (handles all forms including compound).
    if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(s) {
        return Some(ms as i64 * 1000); // ms → μs
    }

    None
}
