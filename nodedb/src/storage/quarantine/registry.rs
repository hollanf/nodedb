//! In-process quarantine registry and startup-scan rebuild.
//!
//! # Persistent state
//!
//! Persistence uses the renamed file as the sole record:
//! `<original_path>.quarantined.<unix_ts_ms>`. On startup, each engine's
//! segment directory is scanned for files matching `*.quarantined.*`, and the
//! registry is rebuilt from those filenames. No metadata schema changes are
//! needed.
//!
//! # Concurrency
//!
//! `QuarantineRegistry` is `Send + Sync`. The hot-path read (does this segment
//! need to be refused?) is a single `RwLock::read` + `HashMap::contains_key`.
//! Writes happen at most twice per segment (first strike → insert; second
//! strike → mark quarantined + rename file). After quarantine, writes stop.
//!
//! # Strike accounting
//!
//! Two consecutive CRC failures quarantine a segment. A successful read resets
//! the strike counter. "Consecutive" is defined per process lifetime: the
//! segment counter does not persist across restarts (a restart always retries
//! once on first open, matching the "retry once" contract).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;

use super::error::QuarantineError;

/// Configuration for the quarantine archive storage layer.
#[derive(Debug, Clone)]
pub struct QuarantineStorageConfig {
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
    /// Region.
    pub region: String,
    /// Local directory for quarantine storage (used when endpoint is empty).
    pub local_dir: Option<PathBuf>,
}

/// Build an `ObjectStore` from a `QuarantineStorageConfig`.
///
/// When `endpoint` is empty, uses `LocalFileSystem` backed by `local_dir`
/// (or `data_dir/quarantine` if `local_dir` is unset).
pub fn build_quarantine_store(
    config: &QuarantineStorageConfig,
    data_dir: &Path,
) -> crate::Result<Arc<dyn ObjectStore>> {
    let dir = config
        .local_dir
        .clone()
        .unwrap_or_else(|| data_dir.join("quarantine"));

    if config.endpoint.is_empty() {
        std::fs::create_dir_all(&dir).map_err(crate::Error::Io)?;
        let store = LocalFileSystem::new_with_prefix(&dir).map_err(|e| crate::Error::Storage {
            engine: "quarantine".into(),
            detail: format!("local quarantine storage init: {e}"),
        })?;
        Ok(Arc::new(store))
    } else {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_allow_http(config.endpoint.starts_with("http://"));
        if !config.access_key.is_empty() {
            builder = builder
                .with_access_key_id(&config.access_key)
                .with_secret_access_key(&config.secret_key);
        }
        let s3 = builder.build().map_err(|e| crate::Error::Storage {
            engine: "quarantine".into(),
            detail: format!("S3 quarantine client init: {e}"),
        })?;
        Ok(Arc::new(s3))
    }
}

/// Identifies an engine kind, used as a label in metrics and HTTP.
///
/// No catch-all (`_`) — every variant is handled explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuarantineEngine {
    Columnar,
    Fts,
    Vector,
    Raft,
}

impl QuarantineEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            QuarantineEngine::Columnar => "columnar",
            QuarantineEngine::Fts => "fts",
            QuarantineEngine::Vector => "vector",
            QuarantineEngine::Raft => "raft",
        }
    }
}

impl std::fmt::Display for QuarantineEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Uniquely identifies a segment across all engines and collections.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentKey {
    pub engine: QuarantineEngine,
    pub collection: String,
    pub segment_id: String,
}

/// In-memory record for a segment that has been observed to fail CRC.
#[derive(Debug, Clone)]
pub struct QuarantineRecord {
    /// Number of consecutive CRC failures observed since last success.
    pub strikes: u32,
    /// Unix timestamp (ms) of the first observed failure in this run.
    pub first_seen_ms: u64,
    /// Error message from the most recent failure.
    pub last_error: String,
    /// If quarantined: the renamed path, and the timestamp at quarantine.
    pub quarantine_info: Option<QuarantineInfo>,
}

/// Details recorded once a segment transitions to the quarantined state.
#[derive(Debug, Clone)]
pub struct QuarantineInfo {
    /// Path to the `.quarantined.<ts>` file on disk.
    pub quarantined_path: PathBuf,
    /// Unix ms timestamp at which the file was renamed.
    pub quarantined_at_ms: u64,
}

/// Thread-safe quarantine registry.
///
/// Stored as `Arc<QuarantineRegistry>` in `SharedState` and injected into
/// engine read wrappers. All public methods are `&self` (interior mutability
/// through the `RwLock`).
#[derive(Debug, Default)]
pub struct QuarantineRegistry {
    entries: RwLock<HashMap<SegmentKey, QuarantineRecord>>,
}

impl QuarantineRegistry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Called by engine read wrappers when a CRC-class error is observed.
    ///
    /// Behaviour:
    /// - If the segment is already quarantined → returns `Err(SegmentQuarantined)`.
    /// - If this is the first strike → records it and returns `Ok(())`, so the
    ///   caller retries once.
    /// - If this is the second strike → renames the file (if a path is provided)
    ///   and returns `Err(SegmentQuarantined)`.
    ///
    /// A successful read should be reported via `record_success` to reset
    /// the strike counter for transient failures.
    pub fn record_failure(
        &self,
        key: SegmentKey,
        error_summary: &str,
        segment_path: Option<&Path>,
    ) -> Result<(), QuarantineError> {
        let now_ms = unix_ms_now();

        // Fast path: already quarantined — no write lock needed.
        {
            let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
            if let Some(rec) = read.get(&key)
                && let Some(ref qi) = rec.quarantine_info
            {
                return Err(QuarantineError::SegmentQuarantined {
                    engine: key.engine.to_string(),
                    collection: key.collection.clone(),
                    segment_id: key.segment_id.clone(),
                    quarantined_at_unix_ms: qi.quarantined_at_ms,
                });
            }
        }

        // Acquire write lock to update strike count.
        let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());

        let rec = write.entry(key.clone()).or_insert(QuarantineRecord {
            strikes: 0,
            first_seen_ms: now_ms,
            last_error: error_summary.to_string(),
            quarantine_info: None,
        });

        // If it was quarantined between our fast-path check and acquiring the write
        // lock, return immediately.
        if let Some(ref qi) = rec.quarantine_info {
            return Err(QuarantineError::SegmentQuarantined {
                engine: key.engine.to_string(),
                collection: key.collection.clone(),
                segment_id: key.segment_id.clone(),
                quarantined_at_unix_ms: qi.quarantined_at_ms,
            });
        }

        rec.strikes += 1;
        rec.last_error = error_summary.to_string();

        if rec.strikes < 2 {
            // First strike — caller should retry once.
            return Ok(());
        }

        // Second strike — quarantine the segment.
        let quarantined_at_ms = now_ms;
        let quarantined_path = if let Some(path) = segment_path {
            let new_path = path.with_extension(format!(
                "{}.quarantined.{quarantined_at_ms}",
                path.extension().and_then(|s| s.to_str()).unwrap_or("seg")
            ));
            if let Err(e) = std::fs::rename(path, &new_path) {
                tracing::error!(
                    engine = key.engine.as_str(),
                    collection = %key.collection,
                    segment_id = %key.segment_id,
                    path = %path.display(),
                    error = %e,
                    "failed to rename corrupt segment file; segment will still be quarantined in memory"
                );
                // Even if rename fails, we still mark the segment quarantined in
                // memory — subsequent reads return SegmentQuarantined immediately.
                // The operator must investigate the original file manually.
                path.to_path_buf()
            } else {
                tracing::error!(
                    engine = key.engine.as_str(),
                    collection = %key.collection,
                    segment_id = %key.segment_id,
                    quarantined_path = %new_path.display(),
                    "segment quarantined after two consecutive CRC failures; \
                     file renamed, collection degraded but other segments remain readable"
                );
                new_path
            }
        } else {
            // No path provided (e.g. in-memory segment, Raft snapshot chunk).
            tracing::error!(
                engine = key.engine.as_str(),
                collection = %key.collection,
                segment_id = %key.segment_id,
                "segment quarantined after two consecutive CRC failures (no file path)"
            );
            PathBuf::new()
        };

        rec.quarantine_info = Some(QuarantineInfo {
            quarantined_path,
            quarantined_at_ms,
        });

        Err(QuarantineError::SegmentQuarantined {
            engine: key.engine.to_string(),
            collection: key.collection.clone(),
            segment_id: key.segment_id.clone(),
            quarantined_at_unix_ms: quarantined_at_ms,
        })
    }

    /// Returns `true` if `key` is currently in the quarantined state.
    ///
    /// Callers that want to reject a segment without even attempting a read can
    /// use this as a fast pre-check. Uses a read-lock only and is cheap to call
    /// on every inbound RPC or segment open.
    pub fn is_quarantined(&self, key: &SegmentKey) -> bool {
        let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
        read.get(key)
            .map(|rec| rec.quarantine_info.is_some())
            .unwrap_or(false)
    }

    /// Called when a segment read succeeds. Clears any pending strikes so
    /// a later failure starts the two-strike count fresh.
    ///
    /// Does nothing if the segment has already been quarantined — a quarantined
    /// segment stays quarantined until the operator removes the file and
    /// the process restarts.
    pub fn record_success(&self, key: &SegmentKey) {
        let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());
        if let Some(rec) = write.get_mut(key)
            && rec.quarantine_info.is_none()
        {
            rec.strikes = 0;
        }
    }

    /// Rebuild the registry by scanning an object store for keys matching the
    /// `.quarantined.<ts>` pattern.
    ///
    /// Called at startup. Each matching object key represents a previously
    /// quarantined segment. The segment is immediately inserted as fully
    /// quarantined (strikes = 2) so reads against it return
    /// `SegmentQuarantined` without a retry.
    ///
    /// `engine_key_from_filename` maps a key name (the final path component)
    /// to `(collection, segment_id)` using the engine-specific naming
    /// convention. If the mapping returns `None` (unrecognised key), it is
    /// skipped.
    pub async fn rebuild_from_store(
        &self,
        engine: QuarantineEngine,
        store: &Arc<dyn ObjectStore>,
        engine_key_from_filename: &(dyn Fn(&str) -> Option<(String, String)> + Send + Sync),
    ) {
        use futures::TryStreamExt;

        let objects: Vec<_> = match store.list(None).try_collect().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "quarantine rebuild: cannot list object store");
                return;
            }
        };

        for obj in objects {
            // Use only the final component of the object path as the filename.
            let key_str = obj.location.as_ref();
            let fname_str = key_str.rsplit('/').next().unwrap_or(key_str);

            if !fname_str.contains(".quarantined.") {
                continue;
            }

            let (collection, segment_id) = match engine_key_from_filename(fname_str) {
                Some(pair) => pair,
                None => {
                    tracing::warn!(
                        file = fname_str,
                        engine = engine.as_str(),
                        "quarantine rebuild: cannot parse filename, skipping"
                    );
                    continue;
                }
            };

            let quarantined_at_ms = fname_str
                .rsplit('.')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            let key = SegmentKey {
                engine,
                collection,
                segment_id,
            };
            // Record the object store key path as a local path string for
            // compatibility with the existing `QuarantineInfo` shape. Operators
            // can locate the file via the object store path.
            let quarantined_path = PathBuf::from(obj.location.as_ref());
            let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());
            write.insert(
                key,
                QuarantineRecord {
                    strikes: 2,
                    first_seen_ms: quarantined_at_ms,
                    last_error: "rebuilt from object store on startup".to_string(),
                    quarantine_info: Some(QuarantineInfo {
                        quarantined_path,
                        quarantined_at_ms,
                    }),
                },
            );
        }
    }

    /// Rebuild the registry from files with the `.quarantined.<ts>` pattern in
    /// the given local filesystem directory.
    ///
    /// This is a convenience wrapper for local-filesystem deployments where
    /// the quarantine directory lives on-disk. For remote object store
    /// deployments use `rebuild_from_store`.
    pub fn rebuild_from_dir(
        &self,
        engine: QuarantineEngine,
        dir: &Path,
        engine_key_from_filename: &dyn Fn(&str) -> Option<(String, String)>,
    ) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(dir = %dir.display(), error = %e, "quarantine rebuild: cannot read dir");
                return;
            }
        };
        for entry in entries.flatten() {
            let fname = entry.file_name();
            let fname_str = match fname.to_str() {
                Some(s) => s,
                None => continue,
            };
            if !fname_str.contains(".quarantined.") {
                continue;
            }
            let (collection, segment_id) = match engine_key_from_filename(fname_str) {
                Some(pair) => pair,
                None => {
                    tracing::warn!(
                        file = fname_str,
                        engine = engine.as_str(),
                        "quarantine rebuild: cannot parse filename, skipping"
                    );
                    continue;
                }
            };
            let quarantined_at_ms = fname_str
                .rsplit('.')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            let key = SegmentKey {
                engine,
                collection,
                segment_id,
            };
            let quarantined_path = dir.join(fname_str);
            let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());
            write.insert(
                key,
                QuarantineRecord {
                    strikes: 2,
                    first_seen_ms: quarantined_at_ms,
                    last_error: "rebuilt from disk on startup".to_string(),
                    quarantine_info: Some(QuarantineInfo {
                        quarantined_path,
                        quarantined_at_ms,
                    }),
                },
            );
        }
    }

    /// Snapshot of all currently quarantined segments (for HTTP/metrics).
    pub fn quarantined_snapshot(&self) -> Vec<QuarantineSnapshot> {
        let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
        read.iter()
            .filter_map(|(key, rec)| {
                rec.quarantine_info.as_ref().map(|qi| QuarantineSnapshot {
                    engine: key.engine.to_string(),
                    collection: key.collection.clone(),
                    segment_id: key.segment_id.clone(),
                    quarantined_at_unix_ms: qi.quarantined_at_ms,
                    last_error_summary: rec.last_error.clone(),
                    strikes: rec.strikes,
                })
            })
            .collect()
    }

    /// Count of currently-quarantined segments per `(engine, collection)`.
    /// Used for the active gauge metric.
    pub fn active_counts(&self) -> HashMap<(String, String), u64> {
        let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let mut counts: HashMap<(String, String), u64> = HashMap::new();
        for (key, rec) in read.iter() {
            if rec.quarantine_info.is_some() {
                *counts
                    .entry((key.engine.to_string(), key.collection.clone()))
                    .or_insert(0) += 1;
            }
        }
        counts
    }

    /// Cumulative count of segments that have ever been quarantined (i.e., reached
    /// second strike) per `(engine, collection)`. Same data as `active_counts` in
    /// this implementation because quarantines are permanent within a process run.
    /// A separate total counter would only differ if un-quarantine were supported.
    pub fn total_counts(&self) -> HashMap<(String, String), u64> {
        self.active_counts()
    }
}

/// A point-in-time snapshot of one quarantined entry, used in HTTP response.
#[derive(Debug, Clone)]
pub struct QuarantineSnapshot {
    pub engine: String,
    pub collection: String,
    pub segment_id: String,
    pub quarantined_at_unix_ms: u64,
    pub last_error_summary: String,
    pub strikes: u32,
}

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn key(engine: QuarantineEngine, collection: &str, segment_id: &str) -> SegmentKey {
        SegmentKey {
            engine,
            collection: collection.to_string(),
            segment_id: segment_id.to_string(),
        }
    }

    #[test]
    fn first_strike_returns_ok() {
        let reg = QuarantineRegistry::new();
        let k = key(QuarantineEngine::Columnar, "coll", "seg1");
        assert!(reg.record_failure(k, "crc error", None).is_ok());
    }

    #[test]
    fn second_strike_returns_quarantined() {
        let reg = QuarantineRegistry::new();
        let k = key(QuarantineEngine::Columnar, "coll", "seg1");
        reg.record_failure(k.clone(), "crc error", None).unwrap();
        let err = reg.record_failure(k, "crc error", None).unwrap_err();
        assert!(matches!(err, QuarantineError::SegmentQuarantined { .. }));
    }

    #[test]
    fn success_clears_strikes() {
        let reg = QuarantineRegistry::new();
        let k = key(QuarantineEngine::Fts, "coll", "seg2");
        // First failure — strike 1.
        reg.record_failure(k.clone(), "crc error", None).unwrap();
        // Success resets it.
        reg.record_success(&k);
        // Failure again — strike 1, not strike 2.
        assert!(reg.record_failure(k, "crc error", None).is_ok());
    }

    #[test]
    fn quarantined_stays_quarantined_after_success_attempt() {
        let reg = QuarantineRegistry::new();
        let k = key(QuarantineEngine::Vector, "coll", "seg3");
        reg.record_failure(k.clone(), "crc", None).unwrap();
        reg.record_failure(k.clone(), "crc", None).unwrap_err();
        // record_success must not un-quarantine.
        reg.record_success(&k);
        let err = reg.record_failure(k, "crc", None).unwrap_err();
        assert!(matches!(err, QuarantineError::SegmentQuarantined { .. }));
    }

    #[test]
    fn concurrent_claims_only_one_quarantine() {
        // Two threads both encounter second failure simultaneously.
        let reg = Arc::new(QuarantineRegistry::new());

        // Pre-seed with one strike.
        let k = key(QuarantineEngine::Raft, "coll", "seg4");
        reg.record_failure(k.clone(), "first", None).unwrap();

        let reg1 = Arc::clone(&reg);
        let k1 = k.clone();
        let reg2 = Arc::clone(&reg);
        let k2 = k.clone();

        let t1 = std::thread::spawn(move || reg1.record_failure(k1, "second", None));
        let t2 = std::thread::spawn(move || reg2.record_failure(k2, "second", None));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Both must see SegmentQuarantined (one gets it immediately as the
        // claimer, the other sees the already-quarantined record).
        assert!(r1.is_err() && r2.is_err());
        let snap = reg.quarantined_snapshot();
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn rebuild_from_dir_restores_quarantine() {
        let tmp = tempfile::tempdir().unwrap();
        let ts = 1_700_000_000_000u64;
        std::fs::write(tmp.path().join(format!("seg5.quarantined.{ts}")), b"").unwrap();

        let reg = QuarantineRegistry::new();
        reg.rebuild_from_dir(QuarantineEngine::Columnar, tmp.path(), &|fname| {
            let stem = fname.split(".quarantined.").next()?;
            Some(("default".to_string(), stem.to_string()))
        });

        let k = key(QuarantineEngine::Columnar, "default", "seg5");
        let err = reg.record_failure(k, "crc", None).unwrap_err();
        assert!(
            matches!(err, QuarantineError::SegmentQuarantined { quarantined_at_unix_ms, .. } if quarantined_at_unix_ms == ts)
        );
    }

    #[tokio::test]
    async fn rebuild_from_store_restores_quarantine() {
        use object_store::ObjectStoreExt;
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ts = 1_700_000_000_111u64;
        let key_path = ObjectPath::from(format!("seg6.quarantined.{ts}"));
        store
            .put(&key_path, object_store::PutPayload::from(b"".as_ref()))
            .await
            .unwrap();

        let reg = QuarantineRegistry::new();
        reg.rebuild_from_store(QuarantineEngine::Columnar, &store, &|fname| {
            let stem = fname.split(".quarantined.").next()?;
            Some(("default".to_string(), stem.to_string()))
        })
        .await;

        let k = key(QuarantineEngine::Columnar, "default", "seg6");
        let err = reg.record_failure(k, "crc", None).unwrap_err();
        assert!(
            matches!(err, QuarantineError::SegmentQuarantined { quarantined_at_unix_ms, .. } if quarantined_at_unix_ms == ts),
            "expected ts={ts} but got: {err}"
        );
    }
}
