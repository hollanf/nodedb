//! Materialized view refresh handler.
//!
//! Scans all documents from the source collection and copies them to
//! the target (view) collection. Removes orphaned target docs that
//! no longer exist in the source.

use std::collections::HashSet;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a full materialized view refresh.
    ///
    /// 1. Scan all documents from the source collection
    /// 2. Write each document to the target (view) collection
    /// 3. Delete orphaned docs in target that are not in source
    /// 4. Return the number of rows materialized
    pub(in crate::data::executor) fn execute_refresh_materialized_view(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        view_name: &str,
        source_collection: &str,
    ) -> Response {
        tracing::debug!(
            core = self.core_id,
            view = view_name,
            source = source_collection,
            "refreshing materialized view"
        );

        // 1. Scan all documents from source.
        let source_docs = match self
            .sparse
            .scan_documents(tid, source_collection, usize::MAX)
        {
            Ok(docs) => docs,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "failed to scan source collection '{source_collection}': {e}"
                        ),
                    },
                );
            }
        };

        // Collect source IDs for orphan detection.
        let source_ids: HashSet<&str> = source_docs.iter().map(|(id, _)| id.as_str()).collect();

        // Check if target view has strict schema — validate/re-encode if so.
        let target_config_key = format!("{tid}:{view_name}");
        let target_strict_schema = self.doc_configs.get(&target_config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // 2. Write each source document to the target collection.
        let mut written = 0u64;
        for (doc_id, doc_bytes) in &source_docs {
            // For strict targets, re-encode through Binary Tuple validation.
            let stored = if let Some(ref schema) = target_strict_schema {
                match super::super::strict_format::bytes_to_binary_tuple(doc_bytes, schema) {
                    Ok(tuple) => tuple,
                    Err(e) => {
                        tracing::warn!(
                            view = view_name,
                            doc_id,
                            error = %e,
                            "skipping document that fails strict schema validation"
                        );
                        continue;
                    }
                }
            } else {
                doc_bytes.clone()
            };
            if let Err(e) = self.sparse.put(tid, view_name, doc_id, &stored) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("failed to write to view '{view_name}': {e}"),
                    },
                );
            }
            written += 1;
        }

        // 3. Delete orphaned docs in target that are not in source.
        let existing_target = self
            .sparse
            .scan_documents(tid, view_name, usize::MAX)
            .unwrap_or_default();
        let mut orphans_deleted = 0u64;
        for (target_id, _) in &existing_target {
            if !source_ids.contains(target_id.as_str()) {
                let _ = self.sparse.delete(tid, view_name, target_id);
                orphans_deleted += 1;
            }
        }

        tracing::info!(
            view = view_name,
            source = source_collection,
            rows = written,
            orphans_deleted,
            "materialized view refreshed"
        );

        let result = serde_json::json!({
            "rows_materialized": written,
            "orphans_deleted": orphans_deleted,
            "source": source_collection,
            "view": view_name,
        });
        match response_codec::encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
