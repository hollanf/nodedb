//! JavaScript/TypeScript bindings for NodeDB-Lite via wasm-bindgen.
//!
//! Uses `redb` with `InMemoryBackend` for storage (same engine as native).
//! Future: OPFS persistence via custom `redb::StorageBackend` over OPFS.
//!
//! ```js
//! import { NodeDbLiteWasm } from "nodedb-lite-wasm";
//!
//! const db = await NodeDbLiteWasm.open(1n);
//! await db.vectorInsert("embeddings", "v1", new Float32Array([1.0, 0.0]));
//! const results = await db.vectorSearch("embeddings", new Float32Array([1.0, 0.0]), 5);
//! ```

use wasm_bindgen::prelude::*;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::document::Document;
use nodedb_types::id::NodeId;
use nodedb_types::value::Value;

/// NodeDB-Lite instance for browser/WASM environments.
#[wasm_bindgen]
pub struct NodeDbLiteWasm {
    db: NodeDbLite<RedbStorage>,
}

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Create a new in-memory NodeDB-Lite database.
    ///
    /// `peer_id` is the unique identifier for this device/browser tab (for CRDT).
    #[wasm_bindgen]
    pub async fn open(peer_id: u64) -> Result<NodeDbLiteWasm, JsError> {
        let storage = RedbStorage::open_in_memory().map_err(|e| JsError::new(&e.to_string()))?;
        let db = NodeDbLite::open(storage, peer_id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self { db })
    }

    /// Insert a vector into a collection.
    #[wasm_bindgen(js_name = "vectorInsert")]
    pub async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> Result<(), JsError> {
        self.db
            .vector_insert(collection, id, embedding, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Search for the k nearest vectors. Returns JSON array.
    #[wasm_bindgen(js_name = "vectorSearch")]
    pub async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> Result<JsValue, JsError> {
        let results = self
            .db
            .vector_search(collection, query, k, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        let json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Delete a vector by ID.
    #[wasm_bindgen(js_name = "vectorDelete")]
    pub async fn vector_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        self.db
            .vector_delete(collection, id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Insert a directed graph edge.
    #[wasm_bindgen(js_name = "graphInsertEdge")]
    pub async fn graph_insert_edge(
        &self,
        from: &str,
        to: &str,
        edge_type: &str,
    ) -> Result<String, JsError> {
        let from_id = NodeId::new(from);
        let to_id = NodeId::new(to);
        let edge_id = self
            .db
            .graph_insert_edge(&from_id, &to_id, edge_type, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(edge_id.as_str().to_string())
    }

    /// Traverse the graph from a start node. Returns JSON.
    #[wasm_bindgen(js_name = "graphTraverse")]
    pub async fn graph_traverse(&self, start: &str, depth: u8) -> Result<JsValue, JsError> {
        let start_id = NodeId::new(start);
        let subgraph = self
            .db
            .graph_traverse(&start_id, depth, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        let json = serde_json::json!({
            "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({
                "id": n.id.as_str(),
                "depth": n.depth,
            })).collect::<Vec<_>>(),
            "edges": subgraph.edges.iter().map(|e| serde_json::json!({
                "from": e.from.as_str(),
                "to": e.to.as_str(),
                "label": e.label,
            })).collect::<Vec<_>>(),
        });

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Get a document by ID. Returns JSON or null.
    #[wasm_bindgen(js_name = "documentGet")]
    pub async fn document_get(&self, collection: &str, id: &str) -> Result<JsValue, JsError> {
        let doc = self
            .db
            .document_get(collection, id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        match doc {
            Some(d) => serde_wasm_bindgen::to_value(&d).map_err(|e| JsError::new(&e.to_string())),
            None => Ok(JsValue::NULL),
        }
    }

    /// Put (insert or update) a document. Takes a JSON string of fields.
    #[wasm_bindgen(js_name = "documentPut")]
    pub async fn document_put(
        &self,
        collection: &str,
        id: &str,
        fields_json: &str,
    ) -> Result<(), JsError> {
        let fields: std::collections::HashMap<String, Value> =
            serde_json::from_str(fields_json).map_err(|e| JsError::new(&e.to_string()))?;

        let mut doc = Document::new(id);
        for (k, v) in fields {
            doc.set(k, v);
        }

        self.db
            .document_put(collection, doc)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Delete a document by ID.
    #[wasm_bindgen(js_name = "documentDelete")]
    pub async fn document_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        self.db
            .document_delete(collection, id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Flush all in-memory state to storage.
    #[wasm_bindgen]
    pub async fn flush(&self) -> Result<(), JsError> {
        self.db
            .flush()
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }
}
