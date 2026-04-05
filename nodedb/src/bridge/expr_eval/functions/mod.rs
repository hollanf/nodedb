//! Scalar function evaluation for SqlExpr.
//!
//! Each function category is in its own sub-module with a `try_eval(name, args)`
//! entry point that returns `Some(value)` if it handles the function name.

mod conditional;
mod datetime;
mod dispatcher;
mod id;
mod json;
mod math;
pub(super) mod shared;
mod string;

pub(super) use dispatcher::eval_function;

#[cfg(test)]
mod tests {
    use super::super::eval::SqlExpr;
    use serde_json::json;

    fn eval_fn(name: &str, args: Vec<serde_json::Value>) -> serde_json::Value {
        super::eval_function(name, &args)
    }

    #[test]
    fn upper() {
        assert_eq!(eval_fn("upper", vec![json!("hello")]), json!("HELLO"));
    }

    #[test]
    fn upper_null_propagation() {
        assert_eq!(eval_fn("upper", vec![json!(null)]), json!(null));
    }

    #[test]
    fn substr_null_propagation() {
        assert_eq!(eval_fn("substr", vec![json!(null), json!(1)]), json!(null));
    }

    #[test]
    fn replace_null_propagation() {
        assert_eq!(
            eval_fn("replace", vec![json!(null), json!("a"), json!("b")]),
            json!(null)
        );
    }

    #[test]
    fn substring() {
        assert_eq!(
            eval_fn("substr", vec![json!("hello"), json!(2), json!(3)]),
            json!("ell")
        );
    }

    #[test]
    fn round_with_decimals() {
        assert_eq!(
            eval_fn("round", vec![json!(3.15159), json!(2)]),
            json!(3.15)
        );
    }

    #[test]
    fn typeof_int() {
        assert_eq!(eval_fn("typeof", vec![json!(42)]), json!("int"));
    }

    #[test]
    fn typeof_float() {
        assert_eq!(eval_fn("typeof", vec![json!(3.15)]), json!("float"));
    }

    #[test]
    fn typeof_null() {
        assert_eq!(eval_fn("typeof", vec![json!(null)]), json!("null"));
    }

    #[test]
    fn function_via_expr() {
        let expr = SqlExpr::Function {
            name: "upper".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        let doc = json!({"name": "alice"});
        assert_eq!(expr.eval(&doc), json!("ALICE"));
    }
}
