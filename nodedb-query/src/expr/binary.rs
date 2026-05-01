//! Binary-operator evaluation on `Value` operands.

use rust_decimal::Decimal;

use nodedb_types::Value;

use crate::value_ops::{
    coerced_eq, compare_values, is_truthy, to_value_number, value_to_display_string, value_to_f64,
};

use super::types::BinaryOp;

/// Coerce a `Value` to `Decimal` for precise arithmetic.
///
/// - `Decimal`: identity
/// - `Integer`: exact conversion via `Decimal::from`
/// - `Float`: best-effort via `Decimal::try_from` (preserves fractional digits
///   rather than compounding f64 ULP error through further f64 arithmetic)
/// - Other types: `None`
fn value_to_decimal(v: &Value) -> Option<Decimal> {
    match v {
        Value::Decimal(d) => Some(*d),
        Value::Integer(i) => Some(Decimal::from(*i)),
        Value::Float(f) => Decimal::try_from(*f).ok(),
        _ => None,
    }
}

/// Apply a Decimal arithmetic operation, returning `Value::Null` on overflow/div-zero.
fn decimal_arith(a: Decimal, op: BinaryOp, b: Decimal) -> Value {
    let result = match op {
        BinaryOp::Add => a.checked_add(b),
        BinaryOp::Sub => a.checked_sub(b),
        BinaryOp::Mul => a.checked_mul(b),
        BinaryOp::Div => a.checked_div(b),
        BinaryOp::Mod => a.checked_rem(b),
        _ => return Value::Null,
    };
    result.map(Value::Decimal).unwrap_or(Value::Null)
}

pub(super) fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Value {
    match op {
        // Arithmetic: prefer Decimal when either operand is Decimal to avoid f64 drift.
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            let left_is_decimal = matches!(left, Value::Decimal(_));
            let right_is_decimal = matches!(right, Value::Decimal(_));
            if left_is_decimal || right_is_decimal {
                match (value_to_decimal(left), value_to_decimal(right)) {
                    (Some(a), Some(b)) => return decimal_arith(a, op, b),
                    _ => return Value::Null,
                }
            }
            // Both operands are non-Decimal: use f64 path.
            match op {
                BinaryOp::Add => match (value_to_f64(left, true), value_to_f64(right, true)) {
                    (Some(a), Some(b)) => to_value_number(a + b),
                    _ => Value::Null,
                },
                BinaryOp::Sub => match (value_to_f64(left, true), value_to_f64(right, true)) {
                    (Some(a), Some(b)) => to_value_number(a - b),
                    _ => Value::Null,
                },
                BinaryOp::Mul => match (value_to_f64(left, true), value_to_f64(right, true)) {
                    (Some(a), Some(b)) => to_value_number(a * b),
                    _ => Value::Null,
                },
                BinaryOp::Div => match (value_to_f64(left, true), value_to_f64(right, true)) {
                    (Some(a), Some(b)) => {
                        if b == 0.0 {
                            Value::Null
                        } else {
                            to_value_number(a / b)
                        }
                    }
                    _ => Value::Null,
                },
                BinaryOp::Mod => match (value_to_f64(left, true), value_to_f64(right, true)) {
                    (Some(a), Some(b)) => {
                        if b == 0.0 {
                            Value::Null
                        } else {
                            to_value_number(a % b)
                        }
                    }
                    _ => Value::Null,
                },
                _ => Value::Null,
            }
        }
        BinaryOp::Concat => {
            let ls = value_to_display_string(left);
            let rs = value_to_display_string(right);
            Value::String(format!("{ls}{rs}"))
        }
        BinaryOp::Eq => Value::Bool(coerced_eq(left, right)),
        BinaryOp::NotEq => Value::Bool(!coerced_eq(left, right)),
        BinaryOp::Gt => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Greater),
        BinaryOp::GtEq => {
            let c = compare_values(left, right);
            Value::Bool(c == std::cmp::Ordering::Greater || c == std::cmp::Ordering::Equal)
        }
        BinaryOp::Lt => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Less),
        BinaryOp::LtEq => {
            let c = compare_values(left, right);
            Value::Bool(c == std::cmp::Ordering::Less || c == std::cmp::Ordering::Equal)
        }
        BinaryOp::And => Value::Bool(is_truthy(left) && is_truthy(right)),
        BinaryOp::Or => Value::Bool(is_truthy(left) || is_truthy(right)),
    }
}
