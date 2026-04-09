//! DateTime and duration scalar functions.

use crate::value_ops::{to_value_number, value_to_display_string};
use nodedb_types::Value;

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "now" | "current_timestamp" => {
            let dt = nodedb_types::NdbDateTime::now();
            Value::String(dt.to_iso8601())
        }
        "datetime" | "to_datetime" => args
            .first()
            .and_then(|v| match v {
                Value::String(s) => {
                    nodedb_types::NdbDateTime::parse(s).map(|dt| Value::String(dt.to_iso8601()))
                }
                Value::Integer(micros) => Some(Value::String(
                    nodedb_types::NdbDateTime::from_micros(*micros).to_iso8601(),
                )),
                Value::Float(f) => Some(Value::String(
                    nodedb_types::NdbDateTime::from_micros(*f as i64).to_iso8601(),
                )),
                Value::DateTime(dt) => Some(Value::String(dt.to_iso8601())),
                _ => None,
            })
            .unwrap_or(Value::Null),
        "unix_secs" | "epoch_secs" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDateTime::parse)
            .map_or(Value::Null, |dt| Value::Integer(dt.unix_secs())),
        "unix_millis" | "epoch_millis" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDateTime::parse)
            .map_or(Value::Null, |dt| Value::Integer(dt.unix_millis())),
        "extract" | "date_part" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match dt {
                Some(dt) => {
                    let c = dt.components();
                    let val: i64 = match part.to_lowercase().as_str() {
                        "year" | "y" => c.year as i64,
                        "month" | "mon" => c.month as i64,
                        "day" | "d" => c.day as i64,
                        "hour" | "h" => c.hour as i64,
                        "minute" | "min" | "m" => c.minute as i64,
                        "second" | "sec" | "s" => c.second as i64,
                        "microsecond" | "us" => c.microsecond as i64,
                        "epoch" => dt.unix_secs(),
                        "dow" | "dayofweek" => {
                            let days = dt.micros / 86_400_000_000;
                            (days + 4) % 7
                        }
                        _ => return Some(Value::Null),
                    };
                    Value::Integer(val)
                }
                None => Value::Null,
            }
        }
        "date_trunc" | "datetrunc" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match dt {
                Some(dt) => {
                    let c = dt.components();
                    let truncated = match part.to_lowercase().as_str() {
                        "year" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-01-01T00:00:00Z",
                            c.year
                        )),
                        "month" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-01T00:00:00Z",
                            c.year, c.month
                        )),
                        "day" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T00:00:00Z",
                            c.year, c.month, c.day
                        )),
                        "hour" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:00:00Z",
                            c.year, c.month, c.day, c.hour
                        )),
                        "minute" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:00Z",
                            c.year, c.month, c.day, c.hour, c.minute
                        )),
                        "second" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                            c.year, c.month, c.day, c.hour, c.minute, c.second
                        )),
                        _ => None,
                    };
                    truncated.map_or(Value::Null, |t| Value::String(t.to_iso8601()))
                }
                None => Value::Null,
            }
        }
        "date_add" | "datetime_add" => {
            let dt = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some(dt), Some(dur)) => Value::String(dt.add_duration(dur).to_iso8601()),
                _ => Value::Null,
            }
        }
        "date_sub" | "datetime_sub" => {
            let dt = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some(dt), Some(dur)) => Value::String(dt.sub_duration(dur).to_iso8601()),
                _ => Value::Null,
            }
        }
        "date_diff" | "datediff" => {
            let dt1 = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dt2 = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match (dt1, dt2) {
                (Some(a), Some(b)) => to_value_number(a.duration_since(&b).as_secs_f64()),
                _ => Value::Null,
            }
        }
        "duration" | "to_duration" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDuration::parse)
            .map_or(Value::Null, |d| Value::String(d.to_human())),
        "decimal" | "to_decimal" => args.first().map_or(Value::Null, |v| {
            let s = value_to_display_string(v);
            match s.parse::<rust_decimal::Decimal>() {
                Ok(d) => Value::String(d.to_string()),
                Err(_) => Value::Null,
            }
        }),
        "time_bucket" => eval_time_bucket(args),
        _ => return None,
    };
    Some(v)
}

/// `time_bucket(interval, timestamp)` — truncate a millisecond timestamp
/// to the start of the given interval bucket.
///
/// Accepts two argument orders (both common in SQL):
/// - `time_bucket('1 hour', timestamp_col)` — interval first
/// - `time_bucket(timestamp_col, '1 hour')` — timestamp first
///
/// The interval is a string like `'1h'`, `'5m'`, `'1 hour'`, `'30 seconds'`.
/// The timestamp is an integer (epoch milliseconds).
fn eval_time_bucket(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }

    // Detect which arg is the interval string and which is the timestamp.
    let (interval_ms, timestamp_ms) = match (&args[0], &args[1]) {
        // time_bucket('1 hour', timestamp)
        (Value::String(s), ts_val) => {
            let interval = parse_interval_to_ms(s);
            let ts = value_to_timestamp_ms(ts_val);
            (interval, ts)
        }
        // time_bucket(timestamp, '1 hour')
        (ts_val, Value::String(s)) => {
            let interval = parse_interval_to_ms(s);
            let ts = value_to_timestamp_ms(ts_val);
            (interval, ts)
        }
        // time_bucket(3600, timestamp) — interval as integer seconds
        (Value::Integer(interval_secs), ts_val) => {
            let ts = value_to_timestamp_ms(ts_val);
            (Some((*interval_secs) * 1000), ts)
        }
        _ => return Value::Null,
    };

    match (interval_ms, timestamp_ms) {
        (Some(i), Some(ts)) if i > 0 => Value::Integer((ts / i) * i),
        _ => Value::Null,
    }
}

fn value_to_timestamp_ms(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(n) => Some(*n),
        Value::Float(f) => Some(*f as i64),
        _ => None,
    }
}

/// Parse an interval string like "1h", "5m", "1 hour", "30 seconds" to ms.
///
/// Delegates to the canonical `nodedb_types::kv_parsing::parse_interval_to_ms`.
fn parse_interval_to_ms(s: &str) -> Option<i64> {
    nodedb_types::kv_parsing::parse_interval_to_ms(s)
        .ok()
        .map(|ms| ms as i64)
        .filter(|&ms| ms > 0)
}
