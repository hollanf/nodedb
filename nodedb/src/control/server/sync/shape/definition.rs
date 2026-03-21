//! Shape definition schema: parameterized boundaries for sync subscriptions.
//!
//! A "shape" defines what subset of the database a Lite client sees.
//! Three shape types:
//!
//! - **Document shape**: `SELECT * FROM collection WHERE predicate`
//! - **Graph shape**: N-hop subgraph from a root node
//! - **Vector shape**: collection + optional namespace filter

use serde::{Deserialize, Serialize};

/// Unique shape identifier (globally unique per Origin).
pub type ShapeId = String;

/// A shape definition: describes which data falls within the subscription.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeDefinition {
    /// Unique shape ID.
    pub shape_id: ShapeId,
    /// Tenant scope.
    pub tenant_id: u32,
    /// Shape type with parameters.
    pub shape_type: ShapeType,
    /// Human-readable description (for debugging).
    pub description: String,
}

/// Shape type: determines how mutations are evaluated for inclusion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShapeType {
    /// Document shape: all documents in a collection matching a predicate.
    ///
    /// Predicate is serialized `Vec<ScanFilter>` (same format as DocumentScan).
    /// Empty predicate = all documents in the collection.
    Document {
        collection: String,
        /// Serialized `Vec<ScanFilter>` (MessagePack). Empty = no filter.
        predicate: Vec<u8>,
    },

    /// Graph shape: N-hop subgraph from root nodes.
    ///
    /// Includes all nodes and edges reachable within `max_depth` hops
    /// from the root nodes. Edge label filter is optional.
    Graph {
        root_nodes: Vec<String>,
        max_depth: usize,
        edge_label: Option<String>,
    },

    /// Vector shape: all vectors in a collection/namespace.
    ///
    /// Optionally filtered by field_name (named vector fields).
    Vector {
        collection: String,
        field_name: Option<String>,
    },
}

impl ShapeDefinition {
    /// Check if a mutation on a specific collection/document might match this shape.
    ///
    /// This is a fast pre-check — returns true if the mutation COULD match
    /// (the actual predicate evaluation happens separately for document shapes).
    pub fn could_match(&self, collection: &str, _doc_id: &str) -> bool {
        match &self.shape_type {
            ShapeType::Document {
                collection: shape_coll,
                ..
            } => shape_coll == collection,
            ShapeType::Graph { root_nodes, .. } => {
                // Graph shapes match if the document could be a node in the graph.
                // Conservative: any mutation to any collection could affect graph nodes.
                // A more precise check would verify the doc_id is reachable from roots.
                !root_nodes.is_empty()
            }
            ShapeType::Vector {
                collection: shape_coll,
                ..
            } => shape_coll == collection,
        }
    }

    /// Get the primary collection for this shape (if applicable).
    pub fn collection(&self) -> Option<&str> {
        match &self.shape_type {
            ShapeType::Document { collection, .. } => Some(collection),
            ShapeType::Vector { collection, .. } => Some(collection),
            ShapeType::Graph { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_shape_matches_collection() {
        let shape = ShapeDefinition {
            shape_id: "s1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "all orders".into(),
        };

        assert!(shape.could_match("orders", "o1"));
        assert!(!shape.could_match("users", "u1"));
        assert_eq!(shape.collection(), Some("orders"));
    }

    #[test]
    fn graph_shape() {
        let shape = ShapeDefinition {
            shape_id: "g1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Graph {
                root_nodes: vec!["alice".into()],
                max_depth: 2,
                edge_label: Some("KNOWS".into()),
            },
            description: "alice's network".into(),
        };

        assert!(shape.could_match("any_collection", "any_doc"));
        assert_eq!(shape.collection(), None);
    }

    #[test]
    fn vector_shape() {
        let shape = ShapeDefinition {
            shape_id: "v1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Vector {
                collection: "embeddings".into(),
                field_name: Some("title".into()),
            },
            description: "title embeddings".into(),
        };

        assert!(shape.could_match("embeddings", "e1"));
        assert!(!shape.could_match("other", "e1"));
    }
}
