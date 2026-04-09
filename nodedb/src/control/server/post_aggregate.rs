//! Post-join aggregation in the Control Plane.
//!
//! When a query has `GROUP BY` over a `JOIN` result, the Data Plane cores
//! return raw join rows. This module aggregates them in the Control Plane.

use std::collections::HashMap;

use sonic_rs;

use crate::bridge::envelope::{Payload, Response};

/// Apply GROUP BY + aggregate functions on a join response payload.
///
/// The input response payload is a JSON array of objects (merged from all cores).
/// Returns a new response with aggregated results.
pub fn apply_post_aggregation(
    resp: Response,
    group_by: &[String],
    aggregates: &[(String, String)],
) -> crate::Result<Response> {
    let payload_bytes = resp.payload.as_bytes();
    let json_text = std::str::from_utf8(payload_bytes).map_err(|e| crate::Error::PlanError {
        detail: format!("post-aggregation: invalid UTF-8: {e}"),
    })?;

    let rows: Vec<serde_json::Value> =
        sonic_rs::from_str(json_text).map_err(|e| crate::Error::PlanError {
            detail: format!("post-aggregation: JSON parse error: {e}"),
        })?;

    // Group rows by the GROUP BY columns.
    // Join results use "collection.field" keys, but GROUP BY may use bare field names.
    let mut groups: HashMap<Vec<String>, Vec<&serde_json::Value>> = HashMap::new();
    for row in &rows {
        let key: Vec<String> = group_by
            .iter()
            .map(|col| {
                resolve_field(row, col)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();
        groups.entry(key).or_default().push(row);
    }

    // Compute aggregates per group.
    let mut result = Vec::with_capacity(groups.len());
    for (key, group_rows) in &groups {
        let mut obj = serde_json::Map::new();

        // Add GROUP BY columns.
        for (i, col) in group_by.iter().enumerate() {
            obj.insert(col.clone(), serde_json::Value::String(key[i].clone()));
        }

        // Compute each aggregate.
        for (op, field) in aggregates {
            let agg_key = format!("{op}({field})");
            let value = compute_aggregate(op, field, group_rows);
            obj.insert(agg_key, value);
        }

        result.push(serde_json::Value::Object(obj));
    }

    let output = sonic_rs::to_vec(&result).map_err(|e| crate::Error::PlanError {
        detail: format!("post-aggregation: serialize error: {e}"),
    })?;

    Ok(Response {
        payload: Payload::from_vec(output),
        ..resp
    })
}

/// Compute a single aggregate over a group of rows.
fn compute_aggregate(op: &str, field: &str, rows: &[&serde_json::Value]) -> serde_json::Value {
    match op {
        "count" => serde_json::Value::Number(serde_json::Number::from(rows.len() as u64)),
        "sum" => {
            let sum: f64 = rows.iter().filter_map(|r| extract_number(r, field)).sum();
            serde_json::json!(sum)
        }
        "avg" => {
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|r| extract_number(r, field))
                .collect();
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::json!(values.iter().sum::<f64>() / values.len() as f64)
            }
        }
        "min" => rows
            .iter()
            .filter_map(|r| extract_number(r, field))
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        "max" => rows
            .iter()
            .filter_map(|r| extract_number(r, field))
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}

/// Extract a numeric value from a JSON object field.
fn extract_number(row: &serde_json::Value, field: &str) -> Option<f64> {
    if field == "*" {
        return Some(1.0);
    }
    resolve_field(row, field).and_then(|v| match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    })
}

/// Resolve a field name in a JSON row, handling "collection.field" prefixed keys.
///
/// Join results use keys like "users.name" but SQL refers to fields as just "name"
/// or with alias "u.name". This tries exact match first, then suffix match.
fn resolve_field<'a>(row: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
    // Exact match first.
    if let Some(v) = row.get(field) {
        return Some(v);
    }
    // Suffix match: look for any key ending with ".{field}".
    let suffix = format!(".{field}");
    if let serde_json::Value::Object(map) = row {
        for (k, v) in map {
            if k.ends_with(&suffix) {
                return Some(v);
            }
        }
    }
    None
}
