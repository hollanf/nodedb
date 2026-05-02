//! Snapshot creation: captures full engine state across all Data Plane cores.
//!
//! A snapshot consists of a set of object-store keys:
//! - `snapshots/snap-{id:06}-lsn{lsn:020}/manifest.msgpack`
//! - `snapshots/snap-{id:06}-lsn{lsn:020}/core-{core_id}.snap`
//!
//! ## Creation flow
//!
//! 1. Control Plane dispatches `PhysicalPlan::CreateSnapshot` to all cores.
//! 2. Each core calls `export_snapshot()` → serializes to bytes → responds.
//! 3. `SnapshotWriter` collects all core snapshots, writes them to the
//!    configured object store, and registers the snapshot in the catalog.
//!
//! ## Consistency
//!
//! The snapshot LSN is the minimum watermark across all cores at the time
//! of the snapshot. WAL records after this LSN may or may not be included
//! in individual core snapshots (cores are not paused during snapshot).
//! On restore, WAL replay from the snapshot LSN forward ensures consistency.
//!
//! ## Storage backend
//!
//! All I/O goes through `Arc<dyn ObjectStore>`. With the `LocalFileSystem`
//! backend (default when no endpoint is configured) this is equivalent to the
//! former `std::fs` path. With an `AmazonS3` backend, data is written to S3.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tracing::{info, warn};

use crate::data::snapshot::CoreSnapshot;
use crate::storage::segment::{SegmentFooter, decrypt_segment_bytes, encrypt_segment_bytes};
use crate::storage::snapshot::{
    SNAPSHOT_FORMAT_VERSION, SnapshotCatalog, SnapshotKind, SnapshotMeta,
};
use crate::types::Lsn;

/// Monotonic snapshot ID counter.
static SNAPSHOT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Configuration for the snapshot storage layer.
#[derive(Debug, Clone)]
pub struct SnapshotStorageConfig {
    /// S3-compatible endpoint URL. Empty = local filesystem.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Prefix path within the bucket.
    pub prefix: String,
    /// Access key (empty = IAM role / instance credentials).
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
    /// Region (required for AWS S3; ignored by most S3-compatible stores).
    pub region: String,
    /// Local directory for snapshot storage (used when endpoint is empty).
    pub local_dir: Option<PathBuf>,
}

/// Build an `ObjectStore` from a `SnapshotStorageConfig`.
///
/// When `endpoint` is empty, uses `LocalFileSystem` backed by `local_dir`
/// (or `data_dir/snapshots` if `local_dir` is unset).
pub fn build_snapshot_store(
    config: &SnapshotStorageConfig,
    data_dir: &std::path::Path,
) -> crate::Result<Arc<dyn ObjectStore>> {
    build_object_store(
        &config.endpoint,
        &config.bucket,
        &config.region,
        &config.access_key,
        &config.secret_key,
        config
            .local_dir
            .as_deref()
            .unwrap_or(&data_dir.join("snapshots")),
        "snapshot",
    )
}

/// Shared helper: construct an `ObjectStore` from endpoint / S3 credentials or
/// fall back to `LocalFileSystem` when the endpoint is empty.
fn build_object_store(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    local_dir: &std::path::Path,
    label: &str,
) -> crate::Result<Arc<dyn ObjectStore>> {
    if endpoint.is_empty() {
        std::fs::create_dir_all(local_dir).map_err(crate::Error::Io)?;
        let store =
            LocalFileSystem::new_with_prefix(local_dir).map_err(|e| crate::Error::Storage {
                engine: label.into(),
                detail: format!("local {label} storage init: {e}"),
            })?;
        Ok(Arc::new(store))
    } else {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_allow_http(endpoint.starts_with("http://"));
        if !access_key.is_empty() {
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);
        }
        let s3 = builder.build().map_err(|e| crate::Error::Storage {
            engine: label.into(),
            detail: format!("S3 {label} client init: {e}"),
        })?;
        Ok(Arc::new(s3))
    }
}

/// Snapshot manifest: stored as `manifest.msgpack` inside the snapshot prefix.
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
    /// Per-core snapshot object key names (relative to snapshot prefix).
    pub core_files: Vec<String>,
    /// Number of cores that contributed to this snapshot.
    pub num_cores: usize,
}

/// Build the prefix for a specific snapshot (relative to the store root).
fn snapshot_prefix(snapshot_id: u64, lsn: u64) -> String {
    format!("snap-{snapshot_id:06}-lsn{lsn:020}")
}

/// Create a base snapshot from core snapshots using an `ObjectStore` backend.
///
/// `core_snapshots` contains `(core_id, snapshot_bytes)` pairs collected
/// from all Data Plane cores via `PhysicalPlan::CreateSnapshot`.
///
/// When `encryption_key` is `Some`, each core `.snap` payload is encrypted
/// (AES-256-GCM) before being written. The manifest is always plaintext.
///
/// Returns the snapshot metadata and the object-store prefix where files
/// were written (e.g. `"snap-000001-lsn00000000000000000100"`).
pub async fn create_base_snapshot(
    store: &Arc<dyn ObjectStore>,
    core_snapshots: Vec<(usize, Vec<u8>)>,
    node_name: &str,
    encryption_key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<(SnapshotMeta, String)> {
    if core_snapshots.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "no core snapshots provided".into(),
        });
    }

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
    let prefix = snapshot_prefix(snapshot_id, min_watermark);

    let mut core_files = Vec::with_capacity(core_snapshots.len());
    for (core_id, bytes) in &core_snapshots {
        let filename = format!("core-{core_id}.snap");
        let object_key = ObjectPath::from(format!("{prefix}/{filename}"));

        let payload_bytes: Vec<u8> = if let Some(key) = encryption_key {
            let watermark = CoreSnapshot::from_bytes(bytes)
                .map(|s| s.watermark)
                .unwrap_or(min_watermark);
            let lsn = Lsn::new(watermark);
            let footer = SegmentFooter::new(node_name, crc32c::crc32c(bytes), lsn, lsn);
            encrypt_segment_bytes(bytes, &footer, Some(key))?
        } else {
            bytes.clone()
        };

        store
            .put(&object_key, PutPayload::from(payload_bytes))
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("put {object_key}: {e}"),
            })?;

        core_files.push(filename);
    }

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    let meta = SnapshotMeta {
        format_version: SNAPSHOT_FORMAT_VERSION,
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

    let manifest_bytes =
        zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("snapshot manifest: {e}"),
        })?;

    let manifest_key = ObjectPath::from(format!("{prefix}/manifest.msgpack"));
    store
        .put(&manifest_key, PutPayload::from(manifest_bytes))
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("put manifest {manifest_key}: {e}"),
        })?;

    info!(
        snapshot_id,
        begin_lsn = min_watermark,
        end_lsn = max_watermark,
        cores = manifest.num_cores,
        data_bytes = total_data_bytes,
        prefix = %prefix,
        "base snapshot created"
    );

    Ok((meta, prefix))
}

/// Load a snapshot manifest from the object store.
pub async fn load_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> crate::Result<SnapshotManifest> {
    let manifest_key = ObjectPath::from(format!("{prefix}/manifest.msgpack"));
    let result = store
        .get(&manifest_key)
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("get manifest {manifest_key}: {e}"),
        })?;
    let bytes = result.bytes().await.map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("read manifest bytes: {e}"),
    })?;
    let manifest: SnapshotManifest =
        zerompk::from_msgpack(&bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("snapshot manifest: {e}"),
        })?;
    manifest.meta.validate_format_version()?;
    Ok(manifest)
}

/// Load a per-core snapshot from the object store.
///
/// When `encryption_key` is `Some`, the bytes are decrypted before
/// deserialization. When `None`, raw bytes are deserialized directly.
pub async fn load_core_snapshot(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    core_id: usize,
    encryption_key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<CoreSnapshot> {
    let key = ObjectPath::from(format!("{prefix}/core-{core_id}.snap"));
    let result = store.get(&key).await.map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("get core-{core_id} snapshot: {e}"),
    })?;
    let raw = result.bytes().await.map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("read core-{core_id} bytes: {e}"),
    })?;

    let bytes = if encryption_key.is_some() {
        decrypt_segment_bytes(&raw, encryption_key)?
    } else {
        raw.to_vec()
    };

    CoreSnapshot::from_bytes(&bytes).ok_or_else(|| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("failed to deserialize core-{core_id} snapshot"),
    })
}

/// Discover all snapshot prefixes in the object store.
///
/// Returns manifests sorted by `end_lsn` (oldest first).
pub async fn discover_snapshots(store: &Arc<dyn ObjectStore>) -> Vec<(String, SnapshotManifest)> {
    use object_store::ListResult;

    let list_result: ListResult = match store.list_with_delimiter(None).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "failed to list snapshots from object store");
            return Vec::new();
        }
    };

    let mut results = Vec::new();
    for common_prefix in list_result.common_prefixes {
        // The prefix path ends with "/"; strip it to get the plain prefix name.
        let prefix_str = common_prefix.as_ref().trim_end_matches('/').to_string();
        match load_manifest(store, &prefix_str).await {
            Ok(manifest) => results.push((prefix_str, manifest)),
            Err(e) => {
                warn!(
                    prefix = %prefix_str,
                    error = %e,
                    "skipping snapshot with invalid manifest"
                );
            }
        }
    }

    results.sort_by_key(|(_, m)| m.meta.end_lsn);
    results
}

/// Rebuild the snapshot catalog from the object store on startup.
pub async fn rebuild_catalog(store: &Arc<dyn ObjectStore>) -> SnapshotCatalog {
    let mut catalog = SnapshotCatalog::new();
    for (_, manifest) in discover_snapshots(store).await {
        catalog.add(manifest.meta);
    }
    catalog
}

/// Delete a snapshot and all its objects from the object store.
pub async fn delete_snapshot(store: &Arc<dyn ObjectStore>, prefix: &str) -> crate::Result<()> {
    use futures::TryStreamExt;

    let list_prefix = ObjectPath::from(format!("{prefix}/"));
    let objects: Vec<_> = store
        .list(Some(&list_prefix))
        .try_collect()
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("list objects for deletion: {e}"),
        })?;

    for obj in objects {
        store
            .delete(&obj.location)
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("delete {}: {e}", obj.location),
            })?;
    }

    info!(prefix = %prefix, "snapshot deleted");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::snapshot::CoreSnapshot;
    use object_store::memory::InMemory;

    fn make_core_snapshot(watermark: u64) -> Vec<u8> {
        let snap = CoreSnapshot {
            watermark,
            ..CoreSnapshot::empty()
        };
        snap.to_bytes().unwrap()
    }

    fn in_memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn create_and_load_snapshot() {
        let store = in_memory_store();
        let core_snaps = vec![(0, make_core_snapshot(100)), (1, make_core_snapshot(105))];

        let (meta, prefix) = create_base_snapshot(&store, core_snaps, "test-node", None)
            .await
            .unwrap();

        assert_eq!(meta.begin_lsn, Lsn::new(100));
        assert_eq!(meta.end_lsn, Lsn::new(105));
        assert_eq!(meta.kind, SnapshotKind::Base);
        assert!(meta.data_bytes > 0);

        let manifest = load_manifest(&store, &prefix).await.unwrap();
        assert_eq!(manifest.num_cores, 2);
        assert_eq!(manifest.core_files.len(), 2);
        assert_eq!(manifest.meta.snapshot_id, meta.snapshot_id);

        let core0 = load_core_snapshot(&store, &prefix, 0, None).await.unwrap();
        assert_eq!(core0.watermark, 100);
        let core1 = load_core_snapshot(&store, &prefix, 1, None).await.unwrap();
        assert_eq!(core1.watermark, 105);
    }

    #[tokio::test]
    async fn discover_and_rebuild_catalog() {
        let store = in_memory_store();

        create_base_snapshot(&store, vec![(0, make_core_snapshot(50))], "n1", None)
            .await
            .unwrap();
        create_base_snapshot(&store, vec![(0, make_core_snapshot(200))], "n1", None)
            .await
            .unwrap();

        let found = discover_snapshots(&store).await;
        assert_eq!(found.len(), 2);
        assert!(found[0].1.meta.end_lsn <= found[1].1.meta.end_lsn);

        let catalog = rebuild_catalog(&store).await;
        assert_eq!(catalog.len(), 2);
        assert!(catalog.find_base(Lsn::new(100)).is_some());
    }

    #[tokio::test]
    async fn delete_snapshot_removes_objects() {
        let store = in_memory_store();
        let (_, prefix) =
            create_base_snapshot(&store, vec![(0, make_core_snapshot(10))], "n1", None)
                .await
                .unwrap();

        // Manifest should be present.
        let key = ObjectPath::from(format!("{prefix}/manifest.msgpack"));
        assert!(store.head(&key).await.is_ok());

        delete_snapshot(&store, &prefix).await.unwrap();

        // Manifest must be gone.
        assert!(store.head(&key).await.is_err());
    }

    #[tokio::test]
    async fn empty_cores_rejected() {
        let store = in_memory_store();
        let result = create_base_snapshot(&store, vec![], "n1", None).await;
        assert!(result.is_err());
    }

    #[test]
    fn snapshot_prefix_naming() {
        let name = snapshot_prefix(1, 42);
        assert_eq!(name, "snap-000001-lsn00000000000000000042");
    }

    #[tokio::test]
    async fn local_filesystem_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let core_snaps = vec![(0, make_core_snapshot(77))];
        let (meta, prefix) = create_base_snapshot(&store, core_snaps, "local-node", None)
            .await
            .unwrap();

        assert_eq!(meta.begin_lsn, Lsn::new(77));

        let loaded = load_core_snapshot(&store, &prefix, 0, None).await.unwrap();
        assert_eq!(loaded.watermark, 77);
    }
}
