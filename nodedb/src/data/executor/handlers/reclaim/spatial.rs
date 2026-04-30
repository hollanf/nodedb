//! Spatial engine reclaim — unlink per-collection R*-tree checkpoint
//! + docmap files.
//!
//! Checkpoint layout (see `spatial_checkpoint.rs`):
//! `{data_dir}/spatial-ckpt/{sanitized_tid:coll:field}.ckpt` + `.docmap`.
//! `sanitize_key` replaces `:` with `__`. We walk the dir and unlink
//! any file whose desanitized stem is `{tid}:{coll}:*`.

use std::path::Path;

use tracing::{debug, warn};

use super::ReclaimStats;

/// Unlink every spatial checkpoint + docmap file for
/// `(tenant_id, collection)`. Returns stats; idempotent.
pub fn reclaim_spatial_checkpoints(
    data_dir: &Path,
    tenant_id: u64,
    collection: &str,
) -> ReclaimStats {
    let ckpt_dir = data_dir.join("spatial-ckpt");
    if !ckpt_dir.exists() {
        return ReclaimStats::default();
    }

    // Files on disk are sanitized (":" → "__"), so match the sanitized
    // prefix directly instead of round-tripping every entry.
    let prefix_sanitized = format!("{tenant_id}__{collection}__");

    let mut stats = ReclaimStats::default();
    let entries = match std::fs::read_dir(&ckpt_dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(
                dir = %ckpt_dir.display(),
                error = %e,
                "spatial reclaim: failed to read ckpt dir"
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
        // Only sweep `.ckpt`, `.ckpt.tmp`, `.docmap`, `.docmap.tmp`.
        let is_ours = name.ends_with(".ckpt")
            || name.ends_with(".ckpt.tmp")
            || name.ends_with(".docmap")
            || name.ends_with(".docmap.tmp");
        if !is_ours {
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                stats.files_unlinked = stats.files_unlinked.saturating_add(1);
                stats.bytes_freed = stats.bytes_freed.saturating_add(size);
                debug!(path = %path.display(), size, "spatial reclaim: unlinked");
            }
            Err(e) => warn!(
                path = %path.display(),
                error = %e,
                "spatial reclaim: unlink failed"
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
    fn unlinks_ckpt_and_docmap_for_matching_field_indexes() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        let ckpt = base.join("spatial-ckpt");
        write(&ckpt.join("1__places__geom.ckpt"), b"x");
        write(&ckpt.join("1__places__geom.docmap"), b"yy");
        write(&ckpt.join("1__places__home.ckpt"), b"zzz");
        // Keep: different collection.
        write(&ckpt.join("1__stores__geom.ckpt"), b"keep");
        // Keep: different tenant.
        write(&ckpt.join("2__places__geom.ckpt"), b"keep2");

        let stats = reclaim_spatial_checkpoints(base, 1, "places");
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, 1 + 2 + 3);
        assert!(ckpt.join("1__stores__geom.ckpt").exists());
        assert!(ckpt.join("2__places__geom.ckpt").exists());
    }

    #[test]
    fn empty_dir_is_noop() {
        let tmp = TempDir::new().unwrap();
        let s = reclaim_spatial_checkpoints(tmp.path(), 1, "x");
        assert_eq!(s.files_unlinked, 0);
    }
}
