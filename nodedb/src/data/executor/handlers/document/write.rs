//! Document write handlers: PointPut, BatchInsert, Upsert, Register, IndexLookup, DropIndex.

use sonic_rs;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_document_batch_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        documents: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = documents.len(), "document batch insert");
        let converted: Vec<(String, Vec<u8>)> = documents
            .iter()
            .map(|(id, val)| (id.clone(), val.clone()))
            .collect();
        let refs: Vec<(&str, &[u8])> = converted
            .iter()
            .map(|(id, val)| (id.as_str(), val.as_slice()))
            .collect();
        match self.sparse.batch_put(tid, collection, &refs) {
            Ok(()) => {
                // Auto-index text fields for full-text search (same as PointPut).
                // Also extract secondary indexes for any registered collection config.
                let config_key = format!("{tid}:{collection}");
                let index_paths: Vec<crate::engine::document::store::IndexPath> = self
                    .doc_configs
                    .get(&config_key)
                    .map(|c| c.index_paths.clone())
                    .unwrap_or_default();
                for (doc_id, val) in documents {
                    if let Some(doc) = super::super::super::doc_format::decode_document(val) {
                        // Full-text inverted index (includes nested block content).
                        let text_content = super::text_extract::extract_indexable_text(&doc);
                        if !text_content.is_empty() {
                            let _ = self
                                .inverted
                                .index_document(collection, doc_id, &text_content);
                        }

                        // Secondary index extraction.
                        self.apply_secondary_indexes(tid, collection, &doc, doc_id, &index_paths);
                    }
                }

                if let Some(ref m) = self.metrics {
                    m.record_document_insert();
                }
                match super::super::super::response_codec::encode_count("inserted", documents.len())
                {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Register a document collection's secondary index configuration.
    ///
    /// Stores the `CollectionConfig` in `self.doc_configs` so that subsequent
    /// `PointPut` and `DocumentBatchInsert` operations extract and write secondary
    /// index entries automatically.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_register_document_collection(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        index_paths: &[String],
        crdt_enabled: bool,
        storage_mode: &crate::bridge::physical_plan::StorageMode,
        enforcement: &crate::bridge::physical_plan::EnforcementOptions,
    ) -> Response {
        let mode_label = match storage_mode {
            crate::bridge::physical_plan::StorageMode::Schemaless => "schemaless",
            crate::bridge::physical_plan::StorageMode::Strict { .. } => "strict",
        };
        debug!(
            core = self.core_id,
            %collection,
            index_count = index_paths.len(),
            crdt_enabled,
            storage_mode = mode_label,
            append_only = enforcement.append_only,
            hash_chain = enforcement.hash_chain,
            balanced = enforcement.balanced.is_some(),
            "register document collection"
        );

        let mut config = crate::engine::document::store::CollectionConfig::new(collection);
        config.crdt_enabled = crdt_enabled;
        config.storage_mode = storage_mode.clone();
        config.enforcement = enforcement.clone();
        for path in index_paths {
            config = config.with_index(path);
        }

        let config_key = format!("{tid}:{collection}");
        self.doc_configs.insert(config_key, config);

        self.response_ok(task)
    }

    /// Execute a secondary index lookup: find all doc IDs where `path = value`.
    ///
    /// Delegates to `SparseEngine::range_scan` via a temporary `DocumentEngine`.
    /// Returns a JSON array of document IDs as the response payload.
    pub(in crate::data::executor) fn execute_document_index_lookup(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        path: &str,
        value: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %path,
            %value,
            "document index lookup"
        );

        let doc_engine = crate::engine::document::store::DocumentEngine::new(&self.sparse, tid);
        match doc_engine.index_lookup(collection, path, value) {
            Ok(doc_ids) => {
                let payload = serde_json::json!(doc_ids);
                match sonic_rs::to_vec(&payload) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("index lookup encode: {e}"),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Drop all secondary index entries for a field across the entire collection.
    ///
    /// Calls `SparseEngine::delete_index_entries_for_field` directly.
    /// Returns `{"removed": N}` as the response payload.
    pub(in crate::data::executor) fn execute_drop_document_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            "drop document index"
        );

        match self
            .sparse
            .delete_index_entries_for_field(tid, collection, field)
        {
            Ok(removed) => {
                match super::super::super::response_codec::encode_count("removed", removed) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("drop index encode: {e}"),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
