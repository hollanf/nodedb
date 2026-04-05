//! Post-scan filter evaluation.
//!
//! `ScanFilter` represents a single filter predicate. `compare_json_values`
//! provides total ordering for JSON values used in sort and range comparisons.
//!
//! Shared between Origin (Control Plane + Data Plane) and Lite.

use crate::json_ops::{coerced_eq, compare_json_optional as compare_json_values};

/// A single filter predicate for document scan evaluation.
///
/// Supports simple comparison operators (eq, ne, gt, gte, lt, lte, contains,
/// is_null, is_not_null) and disjunctive groups via the `"or"` operator.
///
/// OR representation: `{"op": "or", "clauses": [[filter1, filter2], [filter3]]}`
/// means `(filter1 AND filter2) OR filter3`. Each clause is an AND-group;
/// the document matches if ANY clause group fully matches.
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ScanFilter {
    #[serde(default)]
    pub field: String,
    pub op: String,
    #[serde(default)]
    pub value: serde_json::Value,
    /// Disjunctive clause groups for OR predicates.
    /// Each inner Vec is an AND-group. The document matches if ANY group matches.
    #[serde(default)]
    pub clauses: Vec<Vec<ScanFilter>>,
}

impl ScanFilter {
    /// Evaluate this filter against a JSON document.
    pub fn matches(&self, doc: &serde_json::Value) -> bool {
        if self.op == "match_all" {
            return true;
        }

        if self.op == "exists" || self.op == "not_exists" {
            return true;
        }

        if self.op == "or" {
            return self
                .clauses
                .iter()
                .any(|clause| clause.iter().all(|f| f.matches(doc)));
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return self.op == "is_null",
        };

        match self.op.as_str() {
            "eq" => coerced_eq(field_val, &self.value),
            "ne" | "neq" => !coerced_eq(field_val, &self.value),
            "gt" => {
                compare_json_values(Some(field_val), Some(&self.value))
                    == std::cmp::Ordering::Greater
            }
            "gte" | "ge" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            "lt" => {
                compare_json_values(Some(field_val), Some(&self.value)) == std::cmp::Ordering::Less
            }
            "lte" | "le" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            "contains" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            "like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "not_like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "not_ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "in" => {
                if let Some(arr) = self.value.as_array() {
                    arr.iter().any(|v| field_val == v)
                } else {
                    false
                }
            }
            "not_in" => {
                if let Some(arr) = self.value.as_array() {
                    !arr.iter().any(|v| field_val == v)
                } else {
                    true
                }
            }
            "is_null" => field_val.is_null(),
            "is_not_null" => !field_val.is_null(),

            // ── Array operators ──
            // field is an array, value is a scalar: true if array contains the value.
            "array_contains" => {
                if let Some(arr) = field_val.as_array() {
                    arr.iter().any(|v| coerced_eq(v, &self.value))
                } else {
                    false
                }
            }
            // field is an array, value is an array: true if field contains ALL values.
            "array_contains_all" => {
                if let (Some(field_arr), Some(needle_arr)) =
                    (field_val.as_array(), self.value.as_array())
                {
                    needle_arr
                        .iter()
                        .all(|needle| field_arr.iter().any(|v| coerced_eq(v, needle)))
                } else {
                    false
                }
            }
            // field is an array, value is an array: true if any element is shared.
            "array_overlap" => {
                if let (Some(field_arr), Some(needle_arr)) =
                    (field_val.as_array(), self.value.as_array())
                {
                    needle_arr
                        .iter()
                        .any(|needle| field_arr.iter().any(|v| coerced_eq(v, needle)))
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

/// SQL LIKE pattern matching.
///
/// Supports `%` (zero or more characters) and `_` (exactly one character).
/// When `case_insensitive` is true, both input and pattern are lowercased (ILIKE).
pub fn sql_like_match(input: &str, pattern: &str, case_insensitive: bool) -> bool {
    let (input, pattern) = if case_insensitive {
        (input.to_lowercase(), pattern.to_lowercase())
    } else {
        (input.to_string(), pattern.to_string())
    };

    let input = input.as_bytes();
    let pattern = pattern.as_bytes();

    let (mut i, mut j) = (0usize, 0usize);
    let (mut star_j, mut star_i) = (usize::MAX, 0usize);

    while i < input.len() {
        if j < pattern.len() && (pattern[j] == b'_' || pattern[j] == input[i]) {
            i += 1;
            j += 1;
        } else if j < pattern.len() && pattern[j] == b'%' {
            star_j = j;
            star_i = i;
            j += 1;
        } else if star_j != usize::MAX {
            star_i += 1;
            i = star_i;
            j = star_j + 1;
        } else {
            return false;
        }
    }

    while j < pattern.len() && pattern[j] == b'%' {
        j += 1;
    }

    j == pattern.len()
}

/// Compute an aggregate function over a group of JSON documents.
///
/// Supported operations: count, sum, avg, min, max, count_distinct,
/// stddev, variance, array_agg, string_agg, percentile_cont.
pub fn compute_aggregate(op: &str, field: &str, docs: &[serde_json::Value]) -> serde_json::Value {
    match op {
        "count" => serde_json::json!(docs.len()),

        "sum" => {
            let total: f64 = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .sum();
            serde_json::json!(total)
        }

        "avg" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                serde_json::json!(avg)
            }
        }

        "min" => {
            let min = docs
                .iter()
                .filter_map(|d| d.get(field))
                .min_by(|a, b| compare_json_values(Some(a), Some(b)));
            match min {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        "max" => {
            let max = docs
                .iter()
                .filter_map(|d| d.get(field))
                .max_by(|a, b| compare_json_values(Some(a), Some(b)));
            match max {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        "count_distinct" => {
            let mut seen = std::collections::HashSet::new();
            for d in docs {
                if let Some(v) = d.get(field) {
                    seen.insert(v.to_string());
                }
            }
            serde_json::json!(seen.len())
        }

        "stddev" | "stddev_pop" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            serde_json::json!(variance.sqrt())
        }

        "stddev_samp" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            serde_json::json!(variance.sqrt())
        }

        "variance" | "var_pop" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            serde_json::json!(variance)
        }

        "var_samp" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            serde_json::json!(variance)
        }

        "array_agg" => {
            let values: Vec<serde_json::Value> = docs
                .iter()
                .filter_map(|d| d.get(field).cloned())
                .filter(|v| !v.is_null())
                .collect();
            serde_json::Value::Array(values)
        }

        "string_agg" | "group_concat" => {
            let values: Vec<String> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_str()).map(String::from))
                .collect();
            serde_json::Value::String(values.join(","))
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
                .filter_map(|d| d.get(actual_field).and_then(|v| v.as_f64()))
                .collect();
            if values.is_empty() {
                return serde_json::Value::Null;
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
            let lower = idx.floor() as usize;
            let upper = idx.ceil() as usize;
            let frac = idx - lower as f64;
            let result = values[lower] * (1.0 - frac) + values[upper] * frac;
            serde_json::json!(result)
        }

        // Collect distinct field values into a JSON array (deduplicated).
        "array_agg_distinct" => {
            let mut seen = Vec::new();
            for d in docs {
                if let Some(v) = d.get(field)
                    && !v.is_null()
                    && !seen.contains(v)
                {
                    seen.push(v.clone());
                }
            }
            serde_json::Value::Array(seen)
        }

        _ => serde_json::Value::Null,
    }
}

/// Parse simple SQL predicates into `ScanFilter` values.
///
/// Handles basic `field op value` predicates joined by AND.
/// Supports: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `LIKE`, `ILIKE`.
/// Values: single-quoted strings, numbers, `TRUE`/`FALSE`, `NULL`.
///
/// For complex predicates (OR, subqueries, functions), returns empty vec
/// (match all — facet counts will be unfiltered).
pub fn parse_simple_predicates(text: &str) -> Vec<ScanFilter> {
    let mut filters = Vec::new();
    for clause in text.split(" AND ").flat_map(|s| s.split(" and ")) {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }
        if let Some(f) = parse_single_predicate(clause) {
            filters.push(f);
        }
    }
    filters
}

fn parse_single_predicate(clause: &str) -> Option<ScanFilter> {
    let ops = &[">=", "<=", "!=", "<>", "=", ">", "<"];
    for op_str in ops {
        if let Some(pos) = clause.find(op_str) {
            let field = clause[..pos].trim().to_string();
            let raw_value = clause[pos + op_str.len()..].trim();
            let op = match *op_str {
                "=" => "eq",
                "!=" | "<>" => "ne",
                ">" => "gt",
                ">=" => "gte",
                "<" => "lt",
                "<=" => "lte",
                _ => return None,
            };
            return Some(ScanFilter {
                field,
                op: op.to_string(),
                value: parse_predicate_value(raw_value),
                clauses: Vec::new(),
            });
        }
    }

    let upper = clause.to_uppercase();
    if let Some(pos) = upper.find(" LIKE ") {
        let field = clause[..pos].trim().to_string();
        let raw_value = clause[pos + 6..].trim();
        return Some(ScanFilter {
            field,
            op: "like".to_string(),
            value: parse_predicate_value(raw_value),
            clauses: Vec::new(),
        });
    }
    if let Some(pos) = upper.find(" ILIKE ") {
        let field = clause[..pos].trim().to_string();
        let raw_value = clause[pos + 7..].trim();
        return Some(ScanFilter {
            field,
            op: "ilike".to_string(),
            value: parse_predicate_value(raw_value),
            clauses: Vec::new(),
        });
    }

    None
}

fn parse_predicate_value(raw: &str) -> serde_json::Value {
    let raw = raw.trim();
    if raw.starts_with('\'') && raw.ends_with('\'') && raw.len() >= 2 {
        let inner = &raw[1..raw.len() - 1];
        return serde_json::Value::String(inner.replace("''", "'"));
    }
    let upper = raw.to_uppercase();
    if upper == "TRUE" {
        return serde_json::Value::Bool(true);
    }
    if upper == "FALSE" {
        return serde_json::Value::Bool(false);
    }
    if upper == "NULL" {
        return serde_json::Value::Null;
    }
    if let Ok(i) = raw.parse::<i64>() {
        return serde_json::json!(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return serde_json::json!(f);
    }
    serde_json::Value::String(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filter_eq_coercion() {
        let doc = json!({"age": 25});
        let filter = ScanFilter {
            field: "age".into(),
            op: "eq".into(),
            value: json!("25"),
            clauses: vec![],
        };
        assert!(filter.matches(&doc));
    }

    #[test]
    fn filter_gt_coercion() {
        let doc = json!({"score": "90"});
        let filter = ScanFilter {
            field: "score".into(),
            op: "gt".into(),
            value: json!(80),
            clauses: vec![],
        };
        assert!(filter.matches(&doc));
    }

    #[test]
    fn like_basic() {
        assert!(sql_like_match("hello world", "%world", false));
        assert!(sql_like_match("hello world", "hello%", false));
        assert!(!sql_like_match("hello world", "xyz%", false));
    }

    #[test]
    fn ilike_case_insensitive() {
        assert!(sql_like_match("Hello", "hello", true));
        assert!(sql_like_match("WORLD", "%world%", true));
    }

    #[test]
    fn aggregate_count() {
        let docs = vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})];
        assert_eq!(compute_aggregate("count", "x", &docs), json!(3));
    }

    #[test]
    fn aggregate_sum() {
        let docs = vec![json!({"v": 10}), json!({"v": 20}), json!({"v": 30})];
        assert_eq!(compute_aggregate("sum", "v", &docs), json!(60.0));
    }

    #[test]
    fn aggregate_min_max() {
        let docs = vec![json!({"v": 5}), json!({"v": 1}), json!({"v": 9})];
        assert_eq!(compute_aggregate("min", "v", &docs), json!(1));
        assert_eq!(compute_aggregate("max", "v", &docs), json!(9));
    }
}
