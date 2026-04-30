//! Shared argument-extraction helpers used by all function sub-modules.

use nodedb_types::Value;

/// Extract a string argument at `idx`, returning `None` for null/missing.
pub fn str_arg(args: &[Value], idx: usize) -> Option<String> {
    match args.get(idx)? {
        Value::String(s) => Some(s.clone()),
        Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => Some(s.clone()),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => Some(dt.to_iso8601()),
        Value::Duration(d) => Some(d.to_human()),
        _ => None,
    }
}

/// Extract a numeric argument with bool coercion.
pub fn num_arg(args: &[Value], idx: usize) -> Option<f64> {
    args.get(idx)
        .and_then(|v| crate::value_ops::value_to_f64(v, true))
}

/// Check if the first arg is a string matching a predicate.
pub fn bool_id_check(args: &[Value], check: impl Fn(&str) -> bool) -> Value {
    args.first()
        .and_then(|v| v.as_str())
        .map_or(Value::Bool(false), |s| Value::Bool(check(s)))
}
