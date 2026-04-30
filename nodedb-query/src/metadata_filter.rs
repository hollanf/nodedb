//! Bridge between `MetadataFilter` (nodedb-types) and document evaluation.
//!
//! Converts the typed `MetadataFilter` enum into runtime evaluation against
//! JSON documents. Used by both Origin (vector search pre-filter) and Lite
//! (vector search post-filter against CRDT state).

use nodedb_types::filter::MetadataFilter;

use crate::json_ops::{coerced_eq, compare_json_optional};

/// Evaluate a `MetadataFilter` against a JSON document.
///
/// Returns `true` if the document matches the filter.
pub fn matches_metadata_filter(doc: &serde_json::Value, filter: &MetadataFilter) -> bool {
    match filter {
        MetadataFilter::Eq { field, value } => {
            let field_val = doc.get(field.as_str());
            let filter_val = value_to_json(value);
            match field_val {
                Some(fv) => coerced_eq(fv, &filter_val),
                None => filter_val.is_null(),
            }
        }
        MetadataFilter::Ne { field, value } => {
            let field_val = doc.get(field.as_str());
            let filter_val = value_to_json(value);
            match field_val {
                Some(fv) => !coerced_eq(fv, &filter_val),
                None => !filter_val.is_null(),
            }
        }
        MetadataFilter::Gt { field, value } => {
            let field_val = doc.get(field.as_str());
            let filter_val = value_to_json(value);
            compare_json_optional(field_val, Some(&filter_val)) == std::cmp::Ordering::Greater
        }
        MetadataFilter::Gte { field, value } => {
            let field_val = doc.get(field.as_str());
            let filter_val = value_to_json(value);
            let cmp = compare_json_optional(field_val, Some(&filter_val));
            cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
        }
        MetadataFilter::Lt { field, value } => {
            let field_val = doc.get(field.as_str());
            let filter_val = value_to_json(value);
            compare_json_optional(field_val, Some(&filter_val)) == std::cmp::Ordering::Less
        }
        MetadataFilter::Lte { field, value } => {
            let field_val = doc.get(field.as_str());
            let filter_val = value_to_json(value);
            let cmp = compare_json_optional(field_val, Some(&filter_val));
            cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
        }
        MetadataFilter::In { field, values } => {
            let field_val = match doc.get(field.as_str()) {
                Some(v) => v,
                None => return false,
            };
            values
                .iter()
                .any(|v| coerced_eq(field_val, &value_to_json(v)))
        }
        MetadataFilter::NotIn { field, values } => {
            let field_val = match doc.get(field.as_str()) {
                Some(v) => v,
                None => return true,
            };
            !values
                .iter()
                .any(|v| coerced_eq(field_val, &value_to_json(v)))
        }
        MetadataFilter::And(filters) => filters.iter().all(|f| matches_metadata_filter(doc, f)),
        MetadataFilter::Or(filters) => filters.iter().any(|f| matches_metadata_filter(doc, f)),
        MetadataFilter::Not(inner) => !matches_metadata_filter(doc, inner),
        _ => false,
    }
}

/// Convert a `nodedb_types::Value` to `serde_json::Value` for comparison.
fn value_to_json(value: &nodedb_types::value::Value) -> serde_json::Value {
    match value {
        nodedb_types::value::Value::Null => serde_json::Value::Null,
        nodedb_types::value::Value::Bool(b) => serde_json::Value::Bool(*b),
        nodedb_types::value::Value::Integer(i) => serde_json::json!(i),
        nodedb_types::value::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        nodedb_types::value::Value::String(s) => serde_json::Value::String(s.clone()),
        _ => serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::filter::MetadataFilter;
    use nodedb_types::value::Value;
    use serde_json::json;

    #[test]
    fn eq_match() {
        let doc = json!({"status": "active", "age": 25});
        let filter = MetadataFilter::eq("status", "active");
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn eq_no_match() {
        let doc = json!({"status": "inactive"});
        let filter = MetadataFilter::eq("status", "active");
        assert!(!matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn gt_numeric() {
        let doc = json!({"age": 30});
        let filter = MetadataFilter::Gt {
            field: "age".into(),
            value: Value::Integer(25),
        };
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn and_filter() {
        let doc = json!({"status": "active", "age": 30});
        let filter = MetadataFilter::and(vec![
            MetadataFilter::eq("status", "active"),
            MetadataFilter::Gt {
                field: "age".into(),
                value: Value::Integer(25),
            },
        ]);
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn or_filter() {
        let doc = json!({"status": "inactive", "age": 30});
        let filter = MetadataFilter::or(vec![
            MetadataFilter::eq("status", "active"),
            MetadataFilter::Gt {
                field: "age".into(),
                value: Value::Integer(25),
            },
        ]);
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn not_filter() {
        let doc = json!({"status": "active"});
        let filter = MetadataFilter::Not(Box::new(MetadataFilter::eq("status", "inactive")));
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn in_filter() {
        let doc = json!({"role": "admin"});
        let filter = MetadataFilter::In {
            field: "role".into(),
            values: vec![Value::from("admin"), Value::from("superadmin")],
        };
        assert!(matches_metadata_filter(&doc, &filter));
    }

    #[test]
    fn missing_field() {
        let doc = json!({"name": "Alice"});
        let filter = MetadataFilter::eq("status", "active");
        assert!(!matches_metadata_filter(&doc, &filter));
    }
}
