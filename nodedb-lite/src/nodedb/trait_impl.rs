//! `NodeDb` trait implementation for `NodeDbLite`.
//!
//! This module provides the `#[async_trait] impl NodeDb for NodeDbLite<S>` block,
//! wiring the public database API to the underlying HNSW, CSR, and CRDT engines.

use std::collections::HashMap;

use async_trait::async_trait;
use loro::LoroValue;

use nodedb_client::NodeDb;
use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph, SubGraphEdge, SubGraphNode};
use nodedb_types::value::Value;

use super::LockExt;
use super::NodeDbLite;
use super::convert::{loro_value_to_document, value_to_loro};
use crate::engine::graph::index::Direction;
use crate::storage::engine::StorageEngine;

#[async_trait]
impl<S: StorageEngine> NodeDb for NodeDbLite<S> {
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Try to reload evicted collection from storage lazily.
        {
            let has_it = self.hnsw_indices.lock_or_recover().contains_key(collection);

            if !has_it {
                let key = format!("hnsw:{collection}");
                if let Some(checkpoint) = self
                    .storage
                    .get(nodedb_types::Namespace::Vector, key.as_bytes())
                    .await?
                    && let Some(index) =
                        crate::engine::vector::graph::HnswIndex::from_checkpoint(&checkpoint)
                {
                    tracing::info!(collection, "lazy-loaded HNSW collection from storage");
                    self.hnsw_indices
                        .lock_or_recover()
                        .insert(collection.to_string(), index);
                }
            }
        }

        let indices = self.hnsw_indices.lock_or_recover();

        let Some(index) = indices.get(collection) else {
            return Ok(Vec::new());
        };

        // Over-fetch by 3x when filtering to maintain recall after post-filter.
        let fetch_k = if filter.is_some() { k * 3 } else { k };
        let raw_results = index.search(query, fetch_k, self.search_ef);

        let id_map = self.vector_id_map.lock_or_recover();
        let crdt = self.crdt.lock_or_recover();

        let results: Vec<SearchResult> = raw_results
            .into_iter()
            .filter(|r| !index.is_deleted(r.id))
            .filter_map(|r| {
                let composite_key = format!("{collection}:{}", r.id);
                let doc_id = id_map
                    .get(&composite_key)
                    .map(|(id, _)| id.clone())
                    .unwrap_or_else(|| r.id.to_string());

                // Read CRDT document to populate metadata and evaluate filter.
                let metadata = if let Some(loro_val) = crdt.read(collection, &doc_id) {
                    let doc = loro_value_to_document(&doc_id, &loro_val);
                    doc.fields
                        .into_iter()
                        .filter(|(k, _)| k != "embedding_dim")
                        .collect::<HashMap<String, Value>>()
                } else {
                    HashMap::new()
                };

                // Apply metadata filter if provided.
                if let Some(f) = filter {
                    let json_doc = serde_json::to_value(&metadata).unwrap_or_default();
                    if !nodedb_query::metadata_filter::matches_metadata_filter(&json_doc, f) {
                        return None;
                    }
                }

                Some(SearchResult {
                    id: doc_id,
                    node_id: None,
                    distance: r.distance,
                    metadata,
                })
            })
            .take(k) // Trim back to requested k after filtering.
            .collect();

        Ok(results)
    }

    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // ── Insert into HNSW ──
        let internal_id = {
            let mut indices = self.hnsw_indices.lock_or_recover();
            let index = Self::ensure_hnsw(&mut indices, collection, embedding.len());
            let id_before = index.len() as u32;
            index
                .insert(embedding.to_vec())
                .map_err(NodeDbError::bad_request)?;
            id_before
        };

        // ── Track ID mapping ──
        {
            let mut id_map = self.vector_id_map.lock_or_recover();
            id_map.insert(
                format!("{collection}:{internal_id}"),
                (id.to_string(), internal_id),
            );
        }

        // ── Record in CRDT ──
        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields = vec![("embedding_dim", LoroValue::I64(embedding.len() as i64))];
            if let Some(meta) = &metadata {
                for (k, v) in &meta.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }
            crdt.upsert(collection, id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }

    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        // Find internal ID from the map.
        let internal_id = {
            let id_map = self.vector_id_map.lock_or_recover();
            id_map
                .iter()
                .find(|(_, (doc_id, _))| doc_id == id)
                .map(|(_, (_, iid))| *iid)
        };

        if let Some(iid) = internal_id {
            let mut indices = self.hnsw_indices.lock_or_recover();
            if let Some(index) = indices.get_mut(collection) {
                index.delete(iid);
            }
        }

        // ── Record in CRDT ──
        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete(collection, id).map_err(NodeDbError::storage)?;
        }

        Ok(())
    }

    async fn graph_traverse(
        &self,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let csr = self.csr.lock_or_recover();

        // Multi-label filter: use ALL labels from the filter, not just the first.
        let label_strs: Vec<&str> = edge_filter
            .map(|f| f.labels.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();

        let result = csr.traverse_bfs_with_depth_multi(
            &[start.as_str()],
            &label_strs,
            Direction::Out,
            depth as usize,
        );

        let crdt = self.crdt.lock_or_recover();
        let mut nodes = Vec::with_capacity(result.len());
        let mut edges = Vec::new();

        for (node_name, d) in &result {
            // Populate node properties from CRDT if available.
            let properties = if let Some(loro_val) = crdt.read("__nodes", node_name) {
                let doc = loro_value_to_document(node_name, &loro_val);
                doc.fields
            } else {
                HashMap::new()
            };

            nodes.push(SubGraphNode {
                id: NodeId::new(node_name.clone()),
                depth: *d,
                properties,
            });

            let neighbors = csr.neighbors_multi(node_name, &label_strs, Direction::Out);
            for (label, dst) in &neighbors {
                if result.iter().any(|(n, _)| n == dst) {
                    // Read edge properties from CRDT.
                    let edge_id = EdgeId::from_components(node_name, dst, label);
                    let edge_props = if let Some(loro_val) = crdt.read("__edges", edge_id.as_str())
                    {
                        let doc = loro_value_to_document(edge_id.as_str(), &loro_val);
                        doc.fields
                            .into_iter()
                            .filter(|(k, _)| k != "src" && k != "dst" && k != "label")
                            .collect()
                    } else {
                        HashMap::new()
                    };

                    edges.push(SubGraphEdge {
                        id: edge_id,
                        from: NodeId::new(node_name.clone()),
                        to: NodeId::new(dst.clone()),
                        label: label.clone(),
                        properties: edge_props,
                    });
                }
            }
        }

        Ok(SubGraph { nodes, edges })
    }

    async fn graph_insert_edge(
        &self,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        {
            let mut csr = self.csr.lock_or_recover();
            csr.add_edge(from.as_str(), edge_type, to.as_str());
        }

        // ── Record in CRDT (including properties) ──
        let edge_id = EdgeId::from_components(from.as_str(), to.as_str(), edge_type);
        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields: Vec<(&str, LoroValue)> = vec![
                ("src", LoroValue::String(from.as_str().into())),
                ("dst", LoroValue::String(to.as_str().into())),
                ("label", LoroValue::String(edge_type.into())),
            ];

            // Store edge properties alongside the structural fields.
            if let Some(ref props) = properties {
                for (k, v) in &props.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }

            crdt.upsert("__edges", edge_id.as_str(), &fields)
                .map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(edge_id)
    }

    async fn graph_delete_edge(&self, edge_id: &EdgeId) -> NodeDbResult<()> {
        // Parse edge ID: "src--label-->dst"
        let id_str = edge_id.as_str();
        if let Some((src, rest)) = id_str.split_once("--")
            && let Some((label, dst)) = rest.split_once("-->")
        {
            let mut csr = self.csr.lock_or_recover();
            csr.remove_edge(src, label, dst);
        }

        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete("__edges", id_str)
                .map_err(NodeDbError::storage)?;
        }

        Ok(())
    }

    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>> {
        let crdt = self.crdt.lock_or_recover();

        let Some(value) = crdt.read(collection, id) else {
            return Ok(None);
        };

        Ok(Some(loro_value_to_document(id, &value)))
    }

    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();

        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        let fields: Vec<(&str, LoroValue)> = doc
            .fields
            .iter()
            .map(|(k, v)| (k.as_str(), value_to_loro(v)))
            .collect();

        crdt.upsert(collection, &doc_id, &fields)
            .map_err(NodeDbError::storage)?;

        Ok(())
    }

    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();

        crdt.delete(collection, id).map_err(NodeDbError::storage)?;

        Ok(())
    }

    async fn execute_sql(&self, query: &str, _params: &[Value]) -> NodeDbResult<QueryResult> {
        self.query_engine
            .execute_sql(query)
            .await
            .map_err(NodeDbError::storage)
    }
}
