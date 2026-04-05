//! Central dispatcher for scalar function evaluation.

/// Evaluate a scalar function call.
///
/// Dispatches to category-specific sub-modules. Returns `Null` for unknown functions.
pub(in crate::bridge::expr_eval) fn eval_function(
    name: &str,
    args: &[serde_json::Value],
) -> serde_json::Value {
    if let Some(v) = super::string::try_eval(name, args) {
        return v;
    }
    if let Some(v) = super::math::try_eval(name, args) {
        return v;
    }
    if let Some(v) = super::conditional::try_eval(name, args) {
        return v;
    }
    if let Some(v) = super::id::try_eval(name, args) {
        return v;
    }
    if let Some(v) = super::datetime::try_eval(name, args) {
        return v;
    }
    if let Some(v) = super::json::try_eval(name, args) {
        return v;
    }
    serde_json::Value::Null
}
