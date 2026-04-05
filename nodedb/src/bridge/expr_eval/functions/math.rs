//! Math scalar functions.

use crate::bridge::json_ops::to_json_number;

use super::shared::num_arg;

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "abs" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.abs())),
        "round" => {
            let Some(n) = num_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let decimals = num_arg(args, 1).unwrap_or(0.0) as i32;
            let factor = 10.0_f64.powi(decimals);
            to_json_number((n * factor).round() / factor)
        }
        "ceil" | "ceiling" => {
            num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.ceil()))
        }
        "floor" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.floor())),
        "power" | "pow" => {
            let Some(base) = num_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let exp = num_arg(args, 1).unwrap_or(1.0);
            to_json_number(base.powf(exp))
        }
        "sqrt" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.sqrt())),
        "mod" => {
            let Some(a) = num_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let b = num_arg(args, 1).unwrap_or(1.0);
            if b == 0.0 {
                serde_json::Value::Null
            } else {
                to_json_number(a % b)
            }
        }
        "sign" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.signum())),
        "log" | "ln" => {
            num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.ln()))
        }
        "log10" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.log10())),
        "log2" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.log2())),
        "exp" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.exp())),
        _ => return None,
    };
    Some(v)
}
