//! Sparse-vector engine reclaim — unlink per-collection checkpoint files.
//!
//! Checkpoint layout (see `sparse_vector_checkpoint.rs`):
//! `{data_dir}/sparse-vector-ckpt/{sanitized_key}.ckpt`. `sanitize_key`
//! replaces `:` with `__`, so per-`(tenant, collection, field)` keys
//! land as `{tid}__{coll}__{field}.ckpt`.

use std::path::Path;

use tracing::{debug, warn};

use super::ReclaimStats;

/// Unlink every sparse-vector checkpoint file for
/// `(tenant_id, collection)`. Returns stats; idempotent.
pub fn reclaim_sparse_vector_checkpoints(
    data_dir: &Path,
    tenant_id: u64,
    collection: &str,
) -> ReclaimStats {
    let ckpt_dir = data_dir.join("sparse-vector-ckpt");
    if !ckpt_dir.exists() {
        return ReclaimStats::default();
    }
    let prefix_sanitized = format!("{tenant_id}__{collection}__");

    let mut stats = ReclaimStats::default();
    let entries = match std::fs::read_dir(&ckpt_dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(
                dir = %ckpt_dir.display(),
                error = %e,
                "sparse-vector reclaim: failed to read ckpt dir"
            );
            return stats;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with(&prefix_sanitized) {
            continue;
        }
        let is_ours = name.ends_with(".ckpt") || name.ends_with(".ckpt.tmp");
        if !is_ours {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                stats.files_unlinked = stats.files_unlinked.saturating_add(1);
                stats.bytes_freed = stats.bytes_freed.saturating_add(size);
                debug!(path = %path.display(), size, "sparse-vector reclaim: unlinked");
            }
            Err(e) => warn!(
                path = %path.display(),
                error = %e,
                "sparse-vector reclaim: unlink failed"
            ),
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn matches_tenant_collection_prefix() {
        let tmp = TempDir::new().unwrap();
        let ckpt = tmp.path().join("sparse-vector-ckpt");
        write(&ckpt.join("1__docs__title.ckpt"), b"x");
        write(&ckpt.join("1__docs__body.ckpt"), b"yy");
        write(&ckpt.join("1__posts__title.ckpt"), b"keep");
        write(&ckpt.join("2__docs__title.ckpt"), b"keep");

        let stats = reclaim_sparse_vector_checkpoints(tmp.path(), 1, "docs");
        assert_eq!(stats.files_unlinked, 2);
        assert!(ckpt.join("1__posts__title.ckpt").exists());
        assert!(ckpt.join("2__docs__title.ckpt").exists());
    }
}
