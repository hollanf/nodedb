//! Vector index checkpoint methods for [`CoreLoop`].
//!
//! Contains HNSW build completion polling and checkpoint load/save operations.

use super::core_loop::CoreLoop;

/// Parse a `"{tid}:{coll_key}"` string (used in `BuildComplete.key` and on-disk
/// checkpoint filenames) back into the `(TenantId, String)` tuple map key.
///
/// If the string has no `':'` separator the entire string is used as the
/// collection key with tenant 0 (should not happen in practice).
fn parse_build_key(s: &str) -> (crate::types::TenantId, String) {
    match s.split_once(':') {
        Some((tid_str, coll_key)) => {
            let tid = tid_str.parse::<u64>().unwrap_or(0);
            (crate::types::TenantId::new(tid), coll_key.to_string())
        }
        None => (crate::types::TenantId::new(0_u64), s.to_string()),
    }
}

impl CoreLoop {
    /// Drain completed HNSW builds from the background builder thread and
    /// promote the corresponding building segments to sealed segments.
    ///
    /// Called at the top of `tick()` before draining new requests.
    ///
    /// `BuildComplete.key` is the old-style `"{tid}:{coll}"` string (produced
    /// by `VectorCollection::seal` which still takes `&str`). Parse it back to
    /// the tuple key to look up the map.
    pub fn poll_build_completions(&mut self) {
        let Some(rx) = &self.build_rx else { return };
        while let Ok(complete) = rx.try_recv() {
            // Parse the string key `"{tid}:{coll_key}"` back into the tuple.
            let tuple_key = parse_build_key(&complete.key);
            if let Some(coll) = self.vector_collections.get_mut(&tuple_key) {
                coll.complete_build(complete.segment_id, complete.index);
                tracing::info!(
                    core = self.core_id,
                    key = %complete.key,
                    segment_id = complete.segment_id,
                    "HNSW build completed, segment promoted to sealed"
                );
            }
        }
    }

    /// Write HNSW checkpoints for all vector indexes to disk.
    ///
    /// Called periodically from the TPC event loop (e.g., every 5 minutes
    /// or when idle). Each index is serialized to a file at
    /// `{data_dir}/vector-ckpt/{index_key}.ckpt`.
    ///
    /// After checkpointing, WAL replay only needs to process entries
    /// since the checkpoint — not the entire history.
    pub fn checkpoint_vector_indexes(&self) -> usize {
        if self.vector_collections.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("vector-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create vector checkpoint dir"
            );
            return 0;
        }

        let mut checkpointed = 0;
        for (key, collection) in &self.vector_collections {
            if collection.is_empty() {
                continue;
            }
            let bytes = collection.checkpoint_to_bytes(self.vector_checkpoint_kek.as_ref());
            if bytes.is_empty() {
                continue;
            }
            // Checkpoint filename uses the old-style `"{tid}:{coll}"` form so
            // existing on-disk checkpoint files remain valid across upgrades.
            let filename = CoreLoop::vector_checkpoint_filename(key);
            let ckpt_path = ckpt_dir.join(format!("{filename}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{filename}.ckpt.tmp"));
            if nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &bytes).is_ok() {
                checkpointed += 1;
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.vector_collections.len(),
                "vector collections checkpointed"
            );
        }
        checkpointed
    }

    /// Load HNSW checkpoints from disk on startup, before WAL replay.
    ///
    /// For each checkpoint file, loads the index. WAL replay then only
    /// needs to process entries after the checkpoint LSN.
    pub fn load_vector_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("vector-ckpt");
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

            // Checkpoint filenames are `"{tid}:{coll}.ckpt"` — parse back to tuple.
            let filename = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if filename.is_empty() {
                continue;
            }
            let tuple_key = parse_build_key(&filename);

            let Ok(bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&path) else {
                continue;
            };
            let kek = self.vector_checkpoint_kek.as_ref();
            let load_result =
                crate::engine::vector::collection::VectorCollection::from_checkpoint(&bytes, kek);
            let collection = match load_result {
                Ok(Some(c)) => c,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        key = %filename,
                        error = %e,
                        "vector checkpoint rejected"
                    );
                    continue;
                }
            };
            tracing::info!(
                core = self.core_id,
                key = %filename,
                vectors = collection.len(),
                "loaded vector checkpoint"
            );
            self.vector_collections.insert(tuple_key, collection);
            loaded += 1;
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "vector checkpoints loaded");
        }
    }
}
