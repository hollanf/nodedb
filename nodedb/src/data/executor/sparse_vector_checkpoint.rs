//! Sparse vector index checkpoint methods for [`CoreLoop`].
//!
//! Follows the same pattern as `vector_checkpoint.rs`: serialize each index
//! to `{data_dir}/sparse-vector-ckpt/{key}.ckpt` via atomic temp+rename.

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Write sparse vector index checkpoints to disk.
    ///
    /// Called periodically alongside HNSW checkpoints from the TPC event loop.
    pub fn checkpoint_sparse_vector_indexes(&self) -> usize {
        if self.sparse_vector_indexes.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("sparse-vector-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create sparse vector checkpoint dir"
            );
            return 0;
        }

        let mut checkpointed = 0;
        for ((tid, coll, field), index) in &self.sparse_vector_indexes {
            if index.is_empty() {
                continue;
            }
            let bytes = index.checkpoint_to_bytes();
            if bytes.is_empty() {
                continue;
            }
            // Atomic write via temp file + rename.
            // Key is "{tid}:{collection}:{field}", replace ':' with '_' for filename safety.
            let key_str = format!("{}:{}:{}", tid.as_u32(), coll, field);
            let safe_key = key_str.replace(':', "_");
            let ckpt_path = ckpt_dir.join(format!("{safe_key}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{safe_key}.ckpt.tmp"));
            if std::fs::write(&tmp_path, &bytes).is_ok()
                && std::fs::rename(&tmp_path, &ckpt_path).is_ok()
            {
                checkpointed += 1;
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.sparse_vector_indexes.len(),
                "sparse vector indexes checkpointed"
            );
        }
        checkpointed
    }

    /// Load sparse vector index checkpoints from disk on startup.
    pub fn load_sparse_vector_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("sparse-vector-ckpt");
        if !ckpt_dir.exists() {
            return;
        }

        let entries = match std::fs::read_dir(&ckpt_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let mut loaded = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            let safe_key = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if safe_key.is_empty() {
                continue;
            }

            // Restore original key format (underscore → colon, first two only).
            let key = unsanitize_sparse_key(&safe_key);

            if let Ok(bytes) = std::fs::read(&path)
                && let Some(index) =
                    crate::engine::vector::sparse::SparseInvertedIndex::from_checkpoint(&bytes)
            {
                let Some(map_key) = parse_sparse_key(&key) else {
                    tracing::warn!(
                        core = self.core_id,
                        %key,
                        "failed to parse sparse vector checkpoint key, skipping"
                    );
                    continue;
                };
                tracing::info!(
                    core = self.core_id,
                    %key,
                    docs = index.doc_count(),
                    dims = index.dim_count(),
                    "loaded sparse vector checkpoint"
                );
                self.sparse_vector_indexes.insert(map_key, index);
                loaded += 1;
            }
        }

        if loaded > 0 {
            tracing::info!(
                core = self.core_id,
                loaded,
                "sparse vector checkpoints loaded"
            );
        }
    }
}

/// Reverse sanitize: restore ':' separators (first two `_` → `:`)  .
fn unsanitize_sparse_key(sanitized: &str) -> String {
    let mut result = sanitized.to_string();
    if let Some(pos) = result.find('_') {
        result.replace_range(pos..pos + 1, ":");
        if let Some(pos2) = result[pos + 1..].find('_') {
            result.replace_range(pos + 1 + pos2..pos + 1 + pos2 + 1, ":");
        }
    }
    result
}

/// Parse "{tid}:{collection}:{field}" into a (TenantId, String, String) tuple.
fn parse_sparse_key(key: &str) -> Option<(crate::types::TenantId, String, String)> {
    let mut parts = key.splitn(3, ':');
    let tid_str = parts.next()?;
    let coll = parts.next()?.to_string();
    let field = parts.next().unwrap_or("_sparse").to_string();
    let tid_u32: u32 = tid_str.parse().ok()?;
    Some((crate::types::TenantId::new(tid_u32), coll, field))
}
