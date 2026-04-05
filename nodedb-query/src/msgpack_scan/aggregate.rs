//! Zero-deserialization aggregate computation on raw MessagePack documents.
//!
//! Replaces `compute_aggregate(op, field, docs: &[serde_json::Value])` with
//! direct binary field extraction. Each document is `&[u8]` (MessagePack map).

use std::cmp::Ordering;
use std::collections::HashSet;

use crate::msgpack_scan::compare::compare_field_bytes;
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::reader::{read_f64, read_null, read_str};

/// Compute an aggregate function over raw MessagePack documents.
///
/// Each entry in `docs` is a complete MessagePack map (the raw bytes from storage).
/// Returns the result as `nodedb_types::Value` — conversion to JSON happens at the
/// response boundary only.
pub fn compute_aggregate_binary(op: &str, field: &str, docs: &[&[u8]]) -> nodedb_types::Value {
    match op {
        "count" => nodedb_types::Value::Integer(docs.len() as i64),

        "sum" => {
            let total: f64 = docs.iter().filter_map(|d| extract_f64(d, field)).sum();
            nodedb_types::Value::Float(total)
        }

        "avg" => {
            let (sum, count) = docs
                .iter()
                .filter_map(|d| extract_f64(d, field))
                .fold((0.0f64, 0u64), |(s, c), v| (s + v, c + 1));
            if count == 0 {
                nodedb_types::Value::Null
            } else {
                nodedb_types::Value::Float(sum / count as f64)
            }
        }

        "min" => find_minmax(docs, field, false),
        "max" => find_minmax(docs, field, true),

        "count_distinct" => {
            let mut seen = HashSet::new();
            for doc in docs {
                if let Some((start, end)) = extract_field(doc, 0, field)
                    && !read_null(doc, start)
                {
                    seen.insert(&doc[start..end]);
                }
            }
            nodedb_types::Value::Integer(seen.len() as i64)
        }

        "stddev" | "stddev_pop" => {
            stat_aggregate(docs, field, |variance, _n| variance.sqrt(), true)
        }

        "stddev_samp" => stat_aggregate(docs, field, |variance, _n| variance.sqrt(), false),

        "variance" | "var_pop" => stat_aggregate(docs, field, |variance, _n| variance, true),

        "var_samp" => stat_aggregate(docs, field, |variance, _n| variance, false),

        "array_agg" => {
            let values: Vec<nodedb_types::Value> = docs
                .iter()
                .filter_map(|d| extract_as_value(d, field))
                .filter(|v| !v.is_null())
                .collect();
            nodedb_types::Value::Array(values)
        }

        "array_agg_distinct" => {
            let mut seen_bytes = HashSet::new();
            let mut values = Vec::new();
            for doc in docs {
                if let Some((start, end)) = extract_field(doc, 0, field)
                    && !read_null(doc, start)
                {
                    let bytes = &doc[start..end];
                    if seen_bytes.insert(bytes)
                        && let Some(v) = extract_as_value(doc, field)
                    {
                        values.push(v);
                    }
                }
            }
            nodedb_types::Value::Array(values)
        }

        "string_agg" | "group_concat" => {
            let values: Vec<&str> = docs.iter().filter_map(|d| extract_str(d, field)).collect();
            nodedb_types::Value::String(values.join(","))
        }

        "percentile_cont" => {
            let (pct, actual_field) = if let Some(idx) = field.find(':') {
                let p: f64 = field[..idx].parse().unwrap_or(0.5);
                (p, &field[idx + 1..])
            } else {
                (0.5, field)
            };
            let mut values: Vec<f64> = docs
                .iter()
                .filter_map(|d| extract_f64(d, actual_field))
                .collect();
            if values.is_empty() {
                return nodedb_types::Value::Null;
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
            let lower = idx.floor() as usize;
            let upper = idx.ceil() as usize;
            let frac = idx - lower as f64;
            let result = values[lower] * (1.0 - frac) + values[upper] * frac;
            nodedb_types::Value::Float(result)
        }

        _ => nodedb_types::Value::Null,
    }
}

// ── Internal helpers ───────────────────────────────────────────────────

/// Extract a field from a msgpack doc and read as f64.
#[inline]
fn extract_f64(doc: &[u8], field: &str) -> Option<f64> {
    let (start, _end) = extract_field(doc, 0, field)?;
    read_f64(doc, start)
}

/// Extract a field as a zero-copy string slice.
#[inline]
fn extract_str<'a>(doc: &'a [u8], field: &str) -> Option<&'a str> {
    let (start, _end) = extract_field(doc, 0, field)?;
    read_str(doc, start)
}

/// Extract a field as `nodedb_types::Value`. Uses direct msgpack→Value
/// for scalars; falls back to json_from_msgpack only for complex types.
fn extract_as_value(doc: &[u8], field: &str) -> Option<nodedb_types::Value> {
    let (start, end) = extract_field(doc, 0, field)?;
    // Fast path: scalar types (null, bool, int, float, string).
    if let Some(v) = crate::msgpack_scan::reader::read_value(doc, start) {
        return Some(v);
    }
    // Slow path: complex types (array, map, bin) — go through JSON.
    let field_bytes = &doc[start..end];
    let json = nodedb_types::json_msgpack::json_from_msgpack(field_bytes).ok()?;
    Some(nodedb_types::Value::from(json))
}

/// Find min or max across docs by comparing raw field bytes.
fn find_minmax(docs: &[&[u8]], field: &str, want_max: bool) -> nodedb_types::Value {
    let mut best_doc: Option<&[u8]> = None;
    let mut best_range: Option<(usize, usize)> = None;

    for doc in docs {
        if let Some(range) = extract_field(doc, 0, field) {
            if read_null(doc, range.0) {
                continue;
            }
            match best_range {
                None => {
                    best_doc = Some(doc);
                    best_range = Some(range);
                }
                Some(br) => {
                    let cmp = compare_field_bytes(doc, range, best_doc.unwrap(), br);
                    let replace = if want_max {
                        cmp == Ordering::Greater
                    } else {
                        cmp == Ordering::Less
                    };
                    if replace {
                        best_doc = Some(doc);
                        best_range = Some(range);
                    }
                }
            }
        }
    }

    match (best_doc, best_range) {
        (Some(doc), Some((start, end))) => {
            // Fast path: scalars directly.
            if let Some(v) = crate::msgpack_scan::reader::read_value(doc, start) {
                return v;
            }
            // Slow path: complex types through JSON.
            let bytes = &doc[start..end];
            nodedb_types::json_msgpack::json_from_msgpack(bytes)
                .ok()
                .map(nodedb_types::Value::from)
                .unwrap_or(nodedb_types::Value::Null)
        }
        _ => nodedb_types::Value::Null,
    }
}

/// Compute stddev or variance. `population` = true for population variant.
/// `finalize` transforms the variance into the final result.
fn stat_aggregate(
    docs: &[&[u8]],
    field: &str,
    finalize: fn(f64, usize) -> f64,
    population: bool,
) -> nodedb_types::Value {
    let values: Vec<f64> = docs.iter().filter_map(|d| extract_f64(d, field)).collect();
    if values.len() < 2 {
        return nodedb_types::Value::Null;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let divisor = if population {
        values.len() as f64
    } else {
        (values.len() - 1) as f64
    };
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / divisor;
    nodedb_types::Value::Float(finalize(variance, values.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn count() {
        let d1 = encode(&json!({"x": 1}));
        let d2 = encode(&json!({"x": 2}));
        let d3 = encode(&json!({"x": 3}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count", "x", &docs),
            nodedb_types::Value::Integer(3)
        );
    }

    #[test]
    fn sum() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"v": 20}));
        let d3 = encode(&json!({"v": 30}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("sum", "v", &docs),
            nodedb_types::Value::Float(60.0)
        );
    }

    #[test]
    fn avg() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"v": 20}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        assert_eq!(
            compute_aggregate_binary("avg", "v", &docs),
            nodedb_types::Value::Float(15.0)
        );
    }

    #[test]
    fn avg_empty() {
        let d1 = encode(&json!({"other": 1}));
        let docs: Vec<&[u8]> = vec![&d1];
        assert_eq!(
            compute_aggregate_binary("avg", "v", &docs),
            nodedb_types::Value::Null
        );
    }

    #[test]
    fn min_max() {
        let d1 = encode(&json!({"v": 5}));
        let d2 = encode(&json!({"v": 1}));
        let d3 = encode(&json!({"v": 9}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];

        let min = compute_aggregate_binary("min", "v", &docs);
        let max = compute_aggregate_binary("max", "v", &docs);
        assert_eq!(min, nodedb_types::Value::Integer(1));
        assert_eq!(max, nodedb_types::Value::Integer(9));
    }

    #[test]
    fn count_distinct() {
        let d1 = encode(&json!({"v": "a"}));
        let d2 = encode(&json!({"v": "b"}));
        let d3 = encode(&json!({"v": "a"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count_distinct", "v", &docs),
            nodedb_types::Value::Integer(2)
        );
    }

    #[test]
    fn string_agg() {
        let d1 = encode(&json!({"n": "alice"}));
        let d2 = encode(&json!({"n": "bob"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        assert_eq!(
            compute_aggregate_binary("string_agg", "n", &docs),
            nodedb_types::Value::String("alice,bob".into())
        );
    }

    #[test]
    fn array_agg() {
        let d1 = encode(&json!({"v": 1}));
        let d2 = encode(&json!({"v": 2}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        let result = compute_aggregate_binary("array_agg", "v", &docs);
        assert_eq!(
            result,
            nodedb_types::Value::Array(vec![
                nodedb_types::Value::Integer(1),
                nodedb_types::Value::Integer(2),
            ])
        );
    }

    #[test]
    fn stddev_pop() {
        let d1 = encode(&json!({"v": 2.0}));
        let d2 = encode(&json!({"v": 4.0}));
        let d3 = encode(&json!({"v": 4.0}));
        let d4 = encode(&json!({"v": 4.0}));
        let d5 = encode(&json!({"v": 5.0}));
        let d6 = encode(&json!({"v": 5.0}));
        let d7 = encode(&json!({"v": 7.0}));
        let d8 = encode(&json!({"v": 9.0}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3, &d4, &d5, &d6, &d7, &d8];
        let result = compute_aggregate_binary("stddev_pop", "v", &docs);
        if let nodedb_types::Value::Float(v) = result {
            assert!((v - 2.0).abs() < 0.01);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn percentile_cont_median() {
        let d1 = encode(&json!({"v": 1.0}));
        let d2 = encode(&json!({"v": 2.0}));
        let d3 = encode(&json!({"v": 3.0}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("percentile_cont", "v", &docs),
            nodedb_types::Value::Float(2.0)
        );
    }

    #[test]
    fn missing_field_skipped() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"other": 99}));
        let d3 = encode(&json!({"v": 30}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("sum", "v", &docs),
            nodedb_types::Value::Float(40.0)
        );
    }

    #[test]
    fn null_field_skipped_in_count_distinct() {
        let d1 = encode(&json!({"v": "a"}));
        let d2 = encode(&json!({"v": null}));
        let d3 = encode(&json!({"v": "a"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count_distinct", "v", &docs),
            nodedb_types::Value::Integer(1)
        );
    }

    #[test]
    fn array_agg_distinct() {
        let d1 = encode(&json!({"v": 1}));
        let d2 = encode(&json!({"v": 2}));
        let d3 = encode(&json!({"v": 1}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        let result = compute_aggregate_binary("array_agg_distinct", "v", &docs);
        assert_eq!(
            result,
            nodedb_types::Value::Array(vec![
                nodedb_types::Value::Integer(1),
                nodedb_types::Value::Integer(2),
            ])
        );
    }
}
