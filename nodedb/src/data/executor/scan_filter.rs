//! Post-scan filter evaluation for DocumentScan.
//!
//! `ScanFilter` represents a single filter predicate deserialized from the
//! `filters` bytes in a `PhysicalPlan::DocumentScan`. `compare_json_values`
//! provides total ordering for JSON values used in sort and range comparisons.

/// A single filter predicate for DocumentScan post-scan evaluation.
///
/// Supports simple comparison operators (eq, ne, gt, gte, lt, lte, contains,
/// is_null, is_not_null) and disjunctive groups via the `"or"` operator.
///
/// OR representation: `{"op": "or", "clauses": [[filter1, filter2], [filter3]]}`
/// means `(filter1 AND filter2) OR filter3`. Each clause is an AND-group;
/// the document matches if ANY clause group fully matches.
#[derive(serde::Deserialize, Default)]
pub(super) struct ScanFilter {
    #[serde(default)]
    field: String,
    op: String,
    #[serde(default)]
    value: serde_json::Value,
    /// Disjunctive clause groups for OR predicates.
    /// Each inner Vec is an AND-group. The document matches if ANY group matches.
    #[serde(default)]
    clauses: Vec<Vec<ScanFilter>>,
}

impl ScanFilter {
    /// Evaluate this filter against a JSON document.
    pub(super) fn matches(&self, doc: &serde_json::Value) -> bool {
        // OR predicate: document matches if ANY clause group fully matches.
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
            "eq" => field_val == &self.value,
            "ne" | "neq" => field_val != &self.value,
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
            "is_null" => field_val.is_null(),
            "is_not_null" => !field_val.is_null(),
            _ => false,
        }
    }
}

/// Compare two optional JSON values for sorting.
pub(super) fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => {
            // Try numeric comparison first.
            if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                return af.partial_cmp(&bf).unwrap_or(Ordering::Equal);
            }
            // Try integer comparison.
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                return ai.cmp(&bi);
            }
            // Fall back to string comparison.
            let a_str = a
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{a}"));
            let b_str = b
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{b}"));
            a_str.cmp(&b_str)
        }
    }
}

/// Compute an aggregate function over a group of JSON documents.
///
/// Supported operations: count, sum, avg, min, max.
pub(super) fn compute_aggregate(
    op: &str,
    field: &str,
    docs: &[serde_json::Value],
) -> serde_json::Value {
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

        _ => serde_json::Value::Null,
    }
}
