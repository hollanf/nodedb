//! SQL/JSON standard functions: JSON_VALUE, JSON_QUERY, JSON_EXISTS.

use nodedb_types::Value;

use super::path::{parse_jsonpath, walk_path};
use super::pg_ops::value_to_text;

/// `JSON_VALUE(json, path)` — extract a scalar value at JSONPath. Returns text or Null.
pub(super) fn json_value(target: &Value, path: &Value) -> Value {
    let path_str = match path.as_str() {
        Some(s) => s,
        None => return Value::Null,
    };
    let steps = match parse_jsonpath(path_str) {
        Ok(s) => s,
        Err(_) => return Value::Null,
    };
    match walk_path(target, &steps) {
        Some(v) => value_to_text(v.clone()),
        None => Value::Null,
    }
}

/// `JSON_QUERY(json, path)` — extract a subtree at JSONPath. Returns Json or Null.
pub(super) fn json_query(target: &Value, path: &Value) -> Value {
    let path_str = match path.as_str() {
        Some(s) => s,
        None => return Value::Null,
    };
    let steps = match parse_jsonpath(path_str) {
        Ok(s) => s,
        Err(_) => return Value::Null,
    };
    walk_path(target, &steps).cloned().unwrap_or(Value::Null)
}

/// `JSON_EXISTS(json, path)` — returns true if path resolves to a non-null value.
pub(super) fn json_exists(target: &Value, path: &Value) -> Value {
    let path_str = match path.as_str() {
        Some(s) => s,
        None => return Value::Bool(false),
    };
    let steps = match parse_jsonpath(path_str) {
        Ok(s) => s,
        Err(_) => return Value::Bool(false),
    };
    match walk_path(target, &steps) {
        Some(Value::Null) | None => Value::Bool(false),
        Some(_) => Value::Bool(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn obj(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn arr(items: Vec<Value>) -> Value {
        Value::Array(items)
    }

    // ── json_value ─────────────────────────────────────────────────────────

    #[test]
    fn json_value_key() {
        let o = obj(&[("name", Value::String("alice".into()))]);
        assert_eq!(
            json_value(&o, &Value::String("$.name".into())),
            Value::String("alice".into())
        );
    }

    #[test]
    fn json_value_nested() {
        let inner = obj(&[("z", Value::Integer(42))]);
        let o = obj(&[("a", obj(&[("b", inner)]))]);
        assert_eq!(
            json_value(&o, &Value::String("$.a.b.z".into())),
            Value::String("42".into())
        );
    }

    #[test]
    fn json_value_array_index() {
        let o = obj(&[("arr", arr(vec![Value::Integer(10), Value::Integer(20)]))]);
        assert_eq!(
            json_value(&o, &Value::String("$.arr[1]".into())),
            Value::String("20".into())
        );
    }

    #[test]
    fn json_value_missing_returns_null() {
        let o = obj(&[]);
        assert_eq!(
            json_value(&o, &Value::String("$.missing".into())),
            Value::Null
        );
    }

    // ── json_query ─────────────────────────────────────────────────────────

    #[test]
    fn json_query_returns_subtree() {
        let inner = obj(&[("x", Value::Integer(1))]);
        let o = obj(&[("sub", inner.clone())]);
        assert_eq!(json_query(&o, &Value::String("$.sub".into())), inner);
    }

    #[test]
    fn json_query_missing_returns_null() {
        let o = obj(&[]);
        assert_eq!(
            json_query(&o, &Value::String("$.missing".into())),
            Value::Null
        );
    }

    #[test]
    fn json_query_array_element() {
        let a = arr(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let o = obj(&[("a", a)]);
        assert_eq!(
            json_query(&o, &Value::String("$.a[2]".into())),
            Value::Integer(3)
        );
    }

    // ── json_exists ────────────────────────────────────────────────────────

    #[test]
    fn json_exists_present() {
        let o = obj(&[("k", Value::Integer(1))]);
        assert_eq!(
            json_exists(&o, &Value::String("$.k".into())),
            Value::Bool(true)
        );
    }

    #[test]
    fn json_exists_missing() {
        let o = obj(&[]);
        assert_eq!(
            json_exists(&o, &Value::String("$.missing".into())),
            Value::Bool(false)
        );
    }

    #[test]
    fn json_exists_null_value_is_false() {
        let o = obj(&[("k", Value::Null)]);
        assert_eq!(
            json_exists(&o, &Value::String("$.k".into())),
            Value::Bool(false)
        );
    }

    // ── JSONPath error paths ───────────────────────────────────────────────

    #[test]
    fn json_value_malformed_path_returns_null() {
        let o = obj(&[]);
        assert_eq!(json_value(&o, &Value::String("$.[bad".into())), Value::Null);
    }

    #[test]
    fn json_query_recursive_descent_returns_null() {
        let o = obj(&[]);
        assert_eq!(json_query(&o, &Value::String("$..key".into())), Value::Null);
    }

    #[test]
    fn json_exists_filter_expr_returns_false() {
        let o = obj(&[]);
        assert_eq!(
            json_exists(&o, &Value::String("$.arr[?(@>5)]".into())),
            Value::Bool(false)
        );
    }
}
