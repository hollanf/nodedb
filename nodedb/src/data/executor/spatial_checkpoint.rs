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

        let mut checkpointed = 0;
        for ((tid, coll, field), rtree) in &self.spatial_indexes {
            let key_str = format!("{}:{}:{}", tid.as_u32(), coll, field);
            let bytes = match rtree.checkpoint_to_bytes() {
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
            if std::fs::write(&tmp_path, &bytes).is_ok()
                && std::fs::rename(&tmp_path, &ckpt_path).is_ok()
            {
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
                let map_bytes = zerompk::to_msgpack_vec(&doc_entries).unwrap_or_default();
                let map_path = ckpt_dir.join(format!("{}.docmap", sanitize_key(&key_str)));
                let _ = std::fs::write(&map_path, &map_bytes);
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

            if let Ok(bytes) = std::fs::read(&path)
                && let Ok(rtree) = crate::engine::spatial::RTree::from_checkpoint(&bytes)
            {
                // Parse key string "{tid}:{coll}:{field}" back to tuple.
                // unsanitize_key restores the ':' separators.
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
                if let Ok(map_bytes) = std::fs::read(&map_path)
                    && let Ok(doc_entries) = zerompk::from_msgpack::<Vec<(u64, String)>>(&map_bytes)
                {
                    for (entry_id, doc_id) in doc_entries {
                        let (tid, coll, field) = map_key.clone();
                        self.spatial_doc_map
                            .insert((tid, coll, field, entry_id), doc_id);
                    }
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
    let tid_u32: u32 = tid_str.parse().ok()?;
    Some((crate::types::TenantId::new(tid_u32), coll, field))
}
