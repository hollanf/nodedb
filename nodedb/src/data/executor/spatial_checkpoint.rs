//! Spatial R-tree checkpoint methods for [`CoreLoop`].
//!
//! Saves and restores R-tree indexes and the doc_map to disk.
//! Follows the same pattern as `vector_checkpoint.rs`.

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Write R-tree checkpoints for all spatial indexes to disk.
    ///
    /// Each index is serialized via `nodedb_spatial::persist` to a file at
    /// `{data_dir}/spatial-ckpt/{index_key}.ckpt`. The doc_map is saved
    /// alongside as `{index_key}.docmap`.
    ///
    /// When `spatial_checkpoint_kek` is set, checkpoint files are written
    /// encrypted (AES-256-GCM SEGV framing) and plaintext loads are refused.
    pub fn checkpoint_spatial_indexes(&self) -> usize {
        if self.spatial_indexes.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("spatial-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create spatial checkpoint dir"
            );
            return 0;
        }

        let kek = self.spatial_checkpoint_kek.as_ref();

        let mut checkpointed = 0;
        for ((tid, coll, field), rtree) in &self.spatial_indexes {
            let key_str = format!("{}:{}:{}", tid.as_u64(), coll, field);
            let bytes = match rtree.checkpoint_to_bytes(kek) {
                Ok(b) if !b.is_empty() => b,
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!(%key_str, error = %e, "R-tree checkpoint failed");
                    continue;
                }
            };

            // Write R-tree checkpoint.
            let ckpt_path = ckpt_dir.join(format!("{}.ckpt", sanitize_key(&key_str)));
            let tmp_path = ckpt_dir.join(format!("{}.ckpt.tmp", sanitize_key(&key_str)));
            if nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &bytes).is_ok() {
                checkpointed += 1;
            }

            // Write doc_map entries for this index.
            let doc_entries: Vec<(u64, String)> = self
                .spatial_doc_map
                .iter()
                .filter(|((t, c, f, _), _)| t == tid && c == coll && f == field)
                .map(|((_, _, _, entry_id), doc_id)| (*entry_id, doc_id.clone()))
                .collect();
            if !doc_entries.is_empty() {
                let map_bytes = match zerompk::to_msgpack_vec(&doc_entries) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(%key_str, error = %e, "spatial doc_map serialization failed");
                        continue;
                    }
                };
                let map_path = ckpt_dir.join(format!("{}.docmap", sanitize_key(&key_str)));
                let map_tmp = ckpt_dir.join(format!("{}.docmap.tmp", sanitize_key(&key_str)));
                let _ = nodedb_wal::segment::atomic_write_fsync(&map_tmp, &map_path, &map_bytes);
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.spatial_indexes.len(),
                "spatial indexes checkpointed"
            );
        }
        checkpointed
    }

    /// Load R-tree checkpoints from disk on startup.
    ///
    /// When `spatial_checkpoint_kek` is set, plaintext checkpoint files are
    /// rejected and encrypted files are decrypted before loading.
    pub fn load_spatial_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("spatial-ckpt");
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

            let sanitized = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if sanitized.is_empty() {
                continue;
            }

            // Restore the original key from sanitized form.
            let key = unsanitize_key(&sanitized);

            let Ok(bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&path) else {
                continue;
            };

            let kek = self.spatial_checkpoint_kek.as_ref();
            let rtree = match crate::engine::spatial::RTree::from_checkpoint(&bytes, kek) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        %key,
                        error = %e,
                        "spatial checkpoint rejected"
                    );
                    continue;
                }
            };

            // Parse key string "{tid}:{coll}:{field}" back to tuple.
            let Some(map_key) = parse_spatial_key(&key) else {
                tracing::warn!(
                    core = self.core_id,
                    %key,
                    "failed to parse spatial checkpoint key, skipping"
                );
                continue;
            };
            tracing::info!(
                core = self.core_id,
                %key,
                entries = rtree.len(),
                "loaded spatial checkpoint"
            );
            self.spatial_indexes.insert(map_key.clone(), rtree);
            loaded += 1;

            // Load doc_map.
            let map_path = ckpt_dir.join(format!("{sanitized}.docmap"));
            if let Ok(map_bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&map_path)
                && let Ok(doc_entries) = zerompk::from_msgpack::<Vec<(u64, String)>>(&map_bytes)
            {
                for (entry_id, doc_id) in doc_entries {
                    let (tid, coll, field) = map_key.clone();
                    self.spatial_doc_map
                        .insert((tid, coll, field, entry_id), doc_id);
                }
            }
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "spatial checkpoints loaded");
        }
    }
}

/// Sanitize index key for filesystem: replace ':' and '\0' with '_'.
fn sanitize_key(key: &str) -> String {
    key.replace([':', '\0'], "_")
}

/// Reverse sanitize: restore ':' separators (best-effort).
///
/// Keys are "{tid}:{collection}:{field}" → sanitized as "{tid}_{collection}_{field}".
/// Only the first two `_` are replaced since `tid` is numeric (no underscores).
fn unsanitize_key(sanitized: &str) -> String {
    let mut result = sanitized.to_string();
    if let Some(pos) = result.find('_') {
        result.replace_range(pos..pos + 1, ":");
        if let Some(pos2) = result[pos + 1..].find('_') {
            result.replace_range(pos + 1 + pos2..pos + 1 + pos2 + 1, ":");
        }
    }
    result
}

/// Parse a key string of the form "{tid}:{collection}:{field}" into a tuple.
/// Returns `None` if the string doesn't have at least two ':' separators.
fn parse_spatial_key(key: &str) -> Option<(crate::types::TenantId, String, String)> {
    let mut parts = key.splitn(3, ':');
    let tid_str = parts.next()?;
    let coll = parts.next()?.to_string();
    let field = parts.next().unwrap_or("").to_string();
    let tid_u64: u64 = tid_str.parse().ok()?;
    Some((crate::types::TenantId::new(tid_u64), coll, field))
}
