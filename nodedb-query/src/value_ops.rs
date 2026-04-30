//! Core value operations on `nodedb_types::Value`: comparison, coercion, truthiness.
//!
//! Equivalent to `json_ops.rs` but operates on the internal `Value` type directly,
//! avoiding the `serde_json::Value` intermediate. Used by expression evaluation,
//! computed projections, and aggregate expression paths.

use std::cmp::Ordering;

use nodedb_types::Value;

/// Coerce a Value to f64.
///
/// - Integer/Float: direct conversion
/// - String: parse as f64
/// - Bool: `true` → `1.0`, `false` → `0.0` (when `coerce_bool` is true)
/// - Other types: `None`
pub fn value_to_f64(v: &Value, coerce_bool: bool) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Bool(b) if coerce_bool => Some(if *b { 1.0 } else { 0.0 }),
        Value::Decimal(d) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f64()
        }
        _ => None,
    }
}

/// Compare two Values with type coercion.
///
/// Tries numeric comparison first (with bool coercion), then falls
/// back to string comparison.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    if let (Some(na), Some(nb)) = (value_to_f64(a, true), value_to_f64(b, true)) {
        return na.partial_cmp(&nb).unwrap_or(Ordering::Equal);
    }
    let sa = value_to_display_string(a);
    let sb = value_to_display_string(b);
    sa.cmp(&sb)
}

/// Check equality with type coercion.
///
/// Handles `"5" == 5` by coercing both sides to f64 when one is a
/// number and the other is a numeric string.
pub fn coerced_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    if let (Some(af), Some(bf)) = (value_to_f64(a, true), value_to_f64(b, true)) {
        return (af - bf).abs() < f64::EPSILON;
    }
    false
}

/// Check if a Value is truthy (for boolean contexts).
///
/// - `true` → true, `false` → false
/// - `Null` → false
/// - Numbers: non-zero → true
/// - Strings: non-empty → true
/// - Arrays/Objects: always true
pub fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Integer(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// Convert a Value to a display string.
///
/// - Strings: returned as-is (no quotes)
/// - Null: empty string
/// - Numbers/Bools: `.to_string()`
/// - Objects/Arrays: JSON serialization
pub fn value_to_display_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => s.clone(),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => dt.to_iso8601(),
        Value::Duration(d) => d.to_human(),
        Value::Decimal(d) => d.to_string(),
        other => {
            // Fallback: convert to JSON string representation.
            let json = serde_json::Value::from(other.clone());
            json.to_string()
        }
    }
}

/// Convert an f64 to a Value, preferring integer representation.
///
/// Returns `Null` for NaN/Infinity.
pub fn to_value_number(n: f64) -> Value {
    if n.is_nan() || n.is_infinite() {
        Value::Null
    } else if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        Value::Integer(n as i64)
    } else {
        Value::Float(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerced_eq_mixed_types() {
        assert!(coerced_eq(&Value::Integer(5), &Value::String("5".into())));
        assert!(!coerced_eq(&Value::Integer(5), &Value::String("6".into())));
    }

    #[test]
    fn coerced_eq_bool_numeric() {
        assert!(coerced_eq(&Value::Bool(true), &Value::Integer(1)));
        assert!(coerced_eq(&Value::Bool(false), &Value::Integer(0)));
        assert!(!coerced_eq(&Value::Bool(true), &Value::Integer(0)));
    }

    #[test]
    fn compare_numeric_coercion() {
        assert_eq!(
            compare_values(&Value::Integer(5), &Value::String("4".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn truthiness() {
        assert!(is_truthy(&Value::Bool(true)));
        assert!(!is_truthy(&Value::Bool(false)));
        assert!(!is_truthy(&Value::Null));
        assert!(is_truthy(&Value::Integer(1)));
        assert!(!is_truthy(&Value::Integer(0)));
        assert!(is_truthy(&Value::String("hello".into())));
        assert!(!is_truthy(&Value::String(String::new())));
    }

    #[test]
    fn to_value_number_nan() {
        assert_eq!(to_value_number(f64::NAN), Value::Null);
    }

    #[test]
    fn to_value_number_integer() {
        assert_eq!(to_value_number(42.0), Value::Integer(42));
    }

    #[test]
    fn to_value_number_float() {
        assert_eq!(to_value_number(3.15), Value::Float(3.15));
    }
}
