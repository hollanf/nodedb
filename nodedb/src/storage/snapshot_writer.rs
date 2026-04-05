//! Snapshot creation: captures full engine state across all Data Plane cores.
//!
//! A snapshot is a directory containing:
//! - `manifest.msgpack` — snapshot metadata + per-core file list
//! - `core-{id}.snap` — serialized `CoreSnapshot` (MessagePack) per core
//!
//! ## Creation flow
//!
//! 1. Control Plane dispatches `PhysicalPlan::CreateSnapshot` to all cores.
//! 2. Each core calls `export_snapshot()` → serializes to bytes → responds.
//! 3. `SnapshotWriter` collects all core snapshots, writes them to disk
//!    with atomic temp+rename, and registers the snapshot in the catalog.
//!
//! ## Consistency
//!
//! The snapshot LSN is the minimum watermark across all cores at the time
//! of the snapshot. WAL records after this LSN may or may not be included
//! in individual core snapshots (cores are not paused during snapshot).
//! On restore, WAL replay from the snapshot LSN forward ensures consistency.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{info, warn};

use crate::data::snapshot::CoreSnapshot;
use crate::storage::snapshot::{SnapshotCatalog, SnapshotKind, SnapshotMeta};
use crate::types::Lsn;

/// Monotonic snapshot ID counter.
static SNAPSHOT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Snapshot manifest: stored as `manifest.msgpack` inside the snapshot directory.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotManifest {
    /// Snapshot metadata.
    pub meta: SnapshotMeta,
    /// Per-core snapshot file names (relative to snapshot dir).
    pub core_files: Vec<String>,
    /// Number of cores that contributed to this snapshot.
    pub num_cores: usize,
}

/// Directory where snapshots are stored.
pub fn snapshots_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("snapshots")
}

/// Build the directory name for a specific snapshot.
fn snapshot_dir_name(snapshot_id: u64, lsn: u64) -> String {
    format!("snap-{snapshot_id:06}-lsn{lsn:020}")
}

/// Create a base snapshot from core snapshots.
///
/// `core_snapshots` contains `(core_id, snapshot_bytes)` pairs collected
/// from all Data Plane cores via `PhysicalPlan::CreateSnapshot`.
///
/// Returns the snapshot metadata and the directory path where files were written.
pub fn create_base_snapshot(
    data_dir: &Path,
    core_snapshots: Vec<(usize, Vec<u8>)>,
    node_name: &str,
) -> crate::Result<(SnapshotMeta, PathBuf)> {
    if core_snapshots.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "no core snapshots provided".into(),
        });
    }

    // Determine the global snapshot LSN = minimum watermark across all cores.
    let mut min_watermark = u64::MAX;
    let mut max_watermark = 0u64;
    let mut total_data_bytes = 0u64;

    for (_core_id, bytes) in &core_snapshots {
        if let Some(snap) = CoreSnapshot::from_bytes(bytes) {
            min_watermark = min_watermark.min(snap.watermark);
            max_watermark = max_watermark.max(snap.watermark);
        }
        total_data_bytes += bytes.len() as u64;
    }

    if min_watermark == u64::MAX {
        min_watermark = 0;
    }

    let snapshot_id = SNAPSHOT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let snap_dir = snapshots_dir(data_dir).join(snapshot_dir_name(snapshot_id, min_watermark));

    // Create snapshot directory.
    fs::create_dir_all(&snap_dir).map_err(crate::Error::Io)?;

    // Write per-core snapshot files with atomic temp+rename.
    let mut core_files = Vec::with_capacity(core_snapshots.len());
    for (core_id, bytes) in &core_snapshots {
        let filename = format!("core-{core_id}.snap");
        let final_path = snap_dir.join(&filename);
        let tmp_path = snap_dir.join(format!("core-{core_id}.snap.tmp"));

        fs::write(&tmp_path, bytes).map_err(crate::Error::Io)?;
        fs::rename(&tmp_path, &final_path).map_err(crate::Error::Io)?;

        core_files.push(filename);
    }

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    let meta = SnapshotMeta {
        snapshot_id,
        begin_lsn: Lsn::new(min_watermark),
        end_lsn: Lsn::new(max_watermark),
        created_at_us: now_us,
        created_by: node_name.to_string(),
        kind: SnapshotKind::Base,
        parent_id: None,
        data_bytes: total_data_bytes,
    };

    let manifest = SnapshotManifest {
        meta: meta.clone(),
        core_files,
        num_cores: core_snapshots.len(),
    };

    // Write manifest with atomic temp+rename.
    let manifest_bytes =
        zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("snapshot manifest: {e}"),
        })?;
    let manifest_path = snap_dir.join("manifest.msgpack");
    let manifest_tmp = snap_dir.join("manifest.msgpack.tmp");
    fs::write(&manifest_tmp, &manifest_bytes).map_err(crate::Error::Io)?;
    fs::rename(&manifest_tmp, &manifest_path).map_err(crate::Error::Io)?;

    info!(
        snapshot_id,
        begin_lsn = min_watermark,
        end_lsn = max_watermark,
        cores = manifest.num_cores,
        data_bytes = total_data_bytes,
        path = %snap_dir.display(),
        "base snapshot created"
    );

    Ok((meta, snap_dir))
}

/// Load a snapshot manifest from a snapshot directory.
pub fn load_manifest(snap_dir: &Path) -> crate::Result<SnapshotManifest> {
    let manifest_path = snap_dir.join("manifest.msgpack");
    let bytes = fs::read(&manifest_path).map_err(crate::Error::Io)?;
    zerompk::from_msgpack(&bytes).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("snapshot manifest: {e}"),
    })
}

/// Load a per-core snapshot from a snapshot directory.
pub fn load_core_snapshot(snap_dir: &Path, core_id: usize) -> crate::Result<CoreSnapshot> {
    let path = snap_dir.join(format!("core-{core_id}.snap"));
    let bytes = fs::read(&path).map_err(crate::Error::Io)?;
    CoreSnapshot::from_bytes(&bytes).ok_or_else(|| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("failed to deserialize core-{core_id} snapshot"),
    })
}

/// Discover all snapshot directories in the data directory.
///
/// Returns manifests sorted by `end_lsn` (oldest first).
pub fn discover_snapshots(data_dir: &Path) -> Vec<(PathBuf, SnapshotManifest)> {
    let snap_root = snapshots_dir(data_dir);
    if !snap_root.exists() {
        return Vec::new();
    }

    let entries = match fs::read_dir(&snap_root) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "failed to read snapshots directory");
            return Vec::new();
        }
    };

    let mut results = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        match load_manifest(&path) {
            Ok(manifest) => results.push((path, manifest)),
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "skipping snapshot with invalid manifest"
                );
            }
        }
    }

    results.sort_by_key(|(_, m)| m.meta.end_lsn);
    results
}

/// Rebuild the snapshot catalog from disk on startup.
pub fn rebuild_catalog(data_dir: &Path) -> SnapshotCatalog {
    let mut catalog = SnapshotCatalog::new();
    for (_, manifest) in discover_snapshots(data_dir) {
        catalog.add(manifest.meta);
    }
    catalog
}

/// Delete a snapshot directory and all its files.
pub fn delete_snapshot(snap_dir: &Path) -> crate::Result<()> {
    fs::remove_dir_all(snap_dir).map_err(crate::Error::Io)?;
    info!(path = %snap_dir.display(), "snapshot deleted");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::snapshot::CoreSnapshot;

    fn make_core_snapshot(watermark: u64) -> Vec<u8> {
        let snap = CoreSnapshot {
            watermark,
            ..CoreSnapshot::empty()
        };
        snap.to_bytes().unwrap()
    }

    #[test]
    fn create_and_load_snapshot() {
        let dir = tempfile::tempdir().unwrap();

        let core_snaps = vec![(0, make_core_snapshot(100)), (1, make_core_snapshot(105))];

        let (meta, snap_dir) = create_base_snapshot(dir.path(), core_snaps, "test-node").unwrap();

        assert_eq!(meta.begin_lsn, Lsn::new(100)); // min watermark
        assert_eq!(meta.end_lsn, Lsn::new(105)); // max watermark
        assert_eq!(meta.kind, SnapshotKind::Base);
        assert!(meta.data_bytes > 0);

        // Load manifest.
        let manifest = load_manifest(&snap_dir).unwrap();
        assert_eq!(manifest.num_cores, 2);
        assert_eq!(manifest.core_files.len(), 2);
        assert_eq!(manifest.meta.snapshot_id, meta.snapshot_id);

        // Load per-core snapshots.
        let core0 = load_core_snapshot(&snap_dir, 0).unwrap();
        assert_eq!(core0.watermark, 100);
        let core1 = load_core_snapshot(&snap_dir, 1).unwrap();
        assert_eq!(core1.watermark, 105);
    }

    #[test]
    fn discover_and_rebuild_catalog() {
        let dir = tempfile::tempdir().unwrap();

        // Create two snapshots.
        create_base_snapshot(dir.path(), vec![(0, make_core_snapshot(50))], "n1").unwrap();
        create_base_snapshot(dir.path(), vec![(0, make_core_snapshot(200))], "n1").unwrap();

        let found = discover_snapshots(dir.path());
        assert_eq!(found.len(), 2);
        // Sorted by end_lsn.
        assert!(found[0].1.meta.end_lsn <= found[1].1.meta.end_lsn);

        let catalog = rebuild_catalog(dir.path());
        assert_eq!(catalog.len(), 2);
        assert!(catalog.find_base(Lsn::new(100)).is_some());
    }

    #[test]
    fn delete_snapshot_removes_dir() {
        let dir = tempfile::tempdir().unwrap();
        let (_, snap_dir) =
            create_base_snapshot(dir.path(), vec![(0, make_core_snapshot(10))], "n1").unwrap();

        assert!(snap_dir.exists());
        delete_snapshot(&snap_dir).unwrap();
        assert!(!snap_dir.exists());
    }

    #[test]
    fn empty_cores_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let result = create_base_snapshot(dir.path(), vec![], "n1");
        assert!(result.is_err());
    }

    #[test]
    fn snapshot_dir_naming() {
        let name = snapshot_dir_name(1, 42);
        assert_eq!(name, "snap-000001-lsn00000000000000000042");
    }
}
