//! Query filter types for vector search and graph traversal.
//!
//! Filters constrain which results are returned. `MetadataFilter` is used
//! for vector search pre/post-filtering. `EdgeFilter` constrains graph
//! traversal by edge label and/or properties.

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// Metadata filter for vector search. Applied as a pre-filter (Roaring bitmap)
/// or post-filter depending on selectivity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetadataFilter {
    /// Field equals a specific value.
    Eq { field: String, value: Value },
    /// Field does not equal a specific value.
    Ne { field: String, value: Value },
    /// Field is greater than a value (numeric/string).
    Gt { field: String, value: Value },
    /// Field is greater than or equal to a value.
    Gte { field: String, value: Value },
    /// Field is less than a value.
    Lt { field: String, value: Value },
    /// Field is less than or equal to a value.
    Lte { field: String, value: Value },
    /// Field value is in a set.
    In { field: String, values: Vec<Value> },
    /// Field value is not in a set.
    NotIn { field: String, values: Vec<Value> },
    /// Logical AND of multiple filters.
    And(Vec<MetadataFilter>),
    /// Logical OR of multiple filters.
    Or(Vec<MetadataFilter>),
    /// Logical NOT of a filter.
    Not(Box<MetadataFilter>),
}

impl MetadataFilter {
    /// Shorthand: `field == value`.
    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Shorthand: combine filters with AND.
    pub fn and(filters: Vec<MetadataFilter>) -> Self {
        Self::And(filters)
    }

    /// Shorthand: combine filters with OR.
    pub fn or(filters: Vec<MetadataFilter>) -> Self {
        Self::Or(filters)
    }
}

/// Edge filter for graph traversal. Constrains which edges are followed
/// during BFS/DFS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeFilter {
    /// Only follow edges with these labels. Empty = all labels.
    pub labels: Vec<String>,
    /// Only follow edges where properties match. Empty = no property filter.
    pub property_filters: Vec<MetadataFilter>,
}

impl EdgeFilter {
    /// Filter by edge label(s).
    pub fn labels(labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            labels: labels.into_iter().map(Into::into).collect(),
            property_filters: Vec::new(),
        }
    }

    /// No filter — follow all edges.
    pub fn all() -> Self {
        Self {
            labels: Vec::new(),
            property_filters: Vec::new(),
        }
    }
}

impl Default for EdgeFilter {
    fn default() -> Self {
        Self::all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_filter_eq() {
        let f = MetadataFilter::eq("status", "active");
        match &f {
            MetadataFilter::Eq { field, value } => {
                assert_eq!(field, "status");
                assert_eq!(value.as_str(), Some("active"));
            }
            _ => panic!("expected Eq"),
        }
    }

    #[test]
    fn metadata_filter_compound() {
        let f = MetadataFilter::and(vec![
            MetadataFilter::eq("type", "article"),
            MetadataFilter::Gt {
                field: "score".into(),
                value: Value::Float(0.5),
            },
        ]);
        assert!(matches!(f, MetadataFilter::And(ref v) if v.len() == 2));
    }

    #[test]
    fn edge_filter_labels() {
        let f = EdgeFilter::labels(["KNOWS", "FOLLOWS"]);
        assert_eq!(f.labels.len(), 2);
        assert!(f.property_filters.is_empty());
    }

    #[test]
    fn edge_filter_default_is_all() {
        let f = EdgeFilter::default();
        assert!(f.labels.is_empty());
        assert!(f.property_filters.is_empty());
    }
}
