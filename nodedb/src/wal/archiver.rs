//! Continuous WAL archiving for disaster recovery.
//!
//! WAL segments are streamed to remote storage (S3/GCS). Combined with
//! layered snapshots, this enables PITR to any microsecond before failure.
//!
//! The archiver runs on the Control Plane (Tokio) — it never touches the
//! Data Plane. It reads completed WAL segment files and uploads them.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

use crate::types::Lsn;

/// Configuration for the WAL archiver.
#[derive(Debug, Clone)]
pub struct ArchiverConfig {
    /// Local WAL directory to watch for completed segments.
    pub wal_dir: PathBuf,
    /// Remote archive destination (e.g., "s3://bucket/wal/").
    pub archive_url: String,
    /// Maximum number of segments to archive in one batch.
    pub batch_size: usize,
    /// Whether to verify checksums before archiving.
    pub verify_checksums: bool,
    /// Whether to delete local segments after successful archive.
    pub delete_after_archive: bool,
}

impl Default for ArchiverConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from("/tmp/nodedb/wal"),
            archive_url: String::new(),
            batch_size: 16,
            verify_checksums: true,
            delete_after_archive: false,
        }
    }
}

/// Tracks the archival state of WAL segments.
#[derive(Debug, Clone)]
pub struct ArchiverState {
    /// Last LSN that has been successfully archived.
    pub last_archived_lsn: Lsn,
    /// Total segments archived.
    pub segments_archived: u64,
    /// Total bytes archived.
    pub bytes_archived: u64,
    /// Archive failures (for alerting).
    pub failures: u64,
}

impl ArchiverState {
    pub fn new() -> Self {
        Self {
            last_archived_lsn: Lsn::ZERO,
            segments_archived: 0,
            bytes_archived: 0,
            failures: 0,
        }
    }

    /// RPO (Recovery Point Objective) gap: how far behind is the archive?
    pub fn rpo_gap(&self, current_lsn: Lsn) -> u64 {
        current_lsn
            .as_u64()
            .saturating_sub(self.last_archived_lsn.as_u64())
    }
}

impl Default for ArchiverState {
    fn default() -> Self {
        Self::new()
    }
}

/// A WAL segment file ready for archival.
#[derive(Debug, Clone)]
pub struct WalSegment {
    /// Local file path.
    pub path: PathBuf,
    /// First LSN in this segment.
    pub first_lsn: Lsn,
    /// Last LSN in this segment.
    pub last_lsn: Lsn,
    /// File size in bytes.
    pub size_bytes: u64,
}

/// WAL archiver — manages continuous streaming of WAL segments to remote storage.
///
/// The archiver itself does NOT perform network I/O. It produces `ArchiveTask`
/// descriptors that the caller (Control Plane async runtime) executes via
/// `object_store` or equivalent.
pub struct WalArchiver {
    config: ArchiverConfig,
    state: ArchiverState,
}

impl WalArchiver {
    pub fn new(config: ArchiverConfig) -> Self {
        Self {
            config,
            state: ArchiverState::new(),
        }
    }

    /// Scan the WAL directory for segments that need archiving.
    ///
    /// Returns segments with LSN > last_archived_lsn, sorted by LSN.
    pub fn pending_segments(&self) -> std::io::Result<Vec<WalSegment>> {
        let dir = &self.config.wal_dir;
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut segments = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            // Parse segment filename: "wal-{first_lsn}-{last_lsn}.seg"
            if let Some(seg) = parse_segment_filename(&path)
                && seg.last_lsn > self.state.last_archived_lsn
            {
                segments.push(seg);
            }
        }

        segments.sort_by_key(|s| s.first_lsn);
        if segments.len() > self.config.batch_size {
            segments.truncate(self.config.batch_size);
        }

        Ok(segments)
    }

    /// Generate archive tasks for pending segments.
    ///
    /// Each task describes a source (local path) and destination (remote URL).
    /// The caller executes these asynchronously.
    pub fn plan_archive(&self, segments: &[WalSegment]) -> Vec<ArchiveTask> {
        segments
            .iter()
            .map(|seg| {
                let remote_key = format!(
                    "{}/wal-{:016x}-{:016x}.seg",
                    self.config.archive_url.trim_end_matches('/'),
                    seg.first_lsn.as_u64(),
                    seg.last_lsn.as_u64(),
                );
                ArchiveTask {
                    source: seg.path.clone(),
                    destination: remote_key,
                    first_lsn: seg.first_lsn,
                    last_lsn: seg.last_lsn,
                    size_bytes: seg.size_bytes,
                    verify_checksum: self.config.verify_checksums,
                }
            })
            .collect()
    }

    /// Mark a segment as successfully archived.
    pub fn mark_archived(&mut self, task: &ArchiveTask) {
        if task.last_lsn > self.state.last_archived_lsn {
            self.state.last_archived_lsn = task.last_lsn;
        }
        self.state.segments_archived += 1;
        self.state.bytes_archived += task.size_bytes;

        info!(
            last_archived_lsn = task.last_lsn.as_u64(),
            total_segments = self.state.segments_archived,
            total_bytes = self.state.bytes_archived,
            "WAL segment archived"
        );

        // Delete local segment if configured.
        if self.config.delete_after_archive
            && let Err(e) = std::fs::remove_file(&task.source)
        {
            warn!(
                path = %task.source.display(),
                error = %e,
                "failed to delete archived WAL segment"
            );
        }
    }

    /// Record an archive failure.
    pub fn mark_failed(&mut self, _task: &ArchiveTask, reason: &str) {
        self.state.failures += 1;
        warn!(failures = self.state.failures, reason, "WAL archive failed");
    }

    pub fn state(&self) -> &ArchiverState {
        &self.state
    }

    pub fn config(&self) -> &ArchiverConfig {
        &self.config
    }
}

/// A task describing one WAL segment to archive.
#[derive(Debug, Clone)]
pub struct ArchiveTask {
    /// Local source path.
    pub source: PathBuf,
    /// Remote destination URL/key.
    pub destination: String,
    /// First LSN in the segment.
    pub first_lsn: Lsn,
    /// Last LSN in the segment.
    pub last_lsn: Lsn,
    /// Segment size.
    pub size_bytes: u64,
    /// Whether to verify checksum before upload.
    pub verify_checksum: bool,
}

/// Parse a WAL segment filename into a WalSegment.
///
/// Expected format: `wal-{first_lsn_hex}-{last_lsn_hex}.seg`
fn parse_segment_filename(path: &Path) -> Option<WalSegment> {
    let stem = path.file_stem()?.to_str()?;
    let parts: Vec<&str> = stem.split('-').collect();
    if parts.len() != 3 || parts[0] != "wal" {
        return None;
    }

    let first_lsn = u64::from_str_radix(parts[1], 16).ok()?;
    let last_lsn = u64::from_str_radix(parts[2], 16).ok()?;
    let size_bytes = std::fs::metadata(path).ok()?.len();

    Some(WalSegment {
        path: path.to_path_buf(),
        first_lsn: Lsn::new(first_lsn),
        last_lsn: Lsn::new(last_lsn),
        size_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archiver_state_defaults() {
        let state = ArchiverState::new();
        assert_eq!(state.last_archived_lsn, Lsn::ZERO);
        assert_eq!(state.segments_archived, 0);
        assert_eq!(state.rpo_gap(Lsn::new(100)), 100);
    }

    #[test]
    fn rpo_gap_calculation() {
        let mut state = ArchiverState::new();
        state.last_archived_lsn = Lsn::new(90);
        assert_eq!(state.rpo_gap(Lsn::new(100)), 10);
        assert_eq!(state.rpo_gap(Lsn::new(90)), 0);
    }

    #[test]
    fn parse_valid_segment_filename() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-000000000000000a-0000000000000064.seg");
        std::fs::write(&path, b"test data").unwrap();

        let seg = parse_segment_filename(&path).unwrap();
        assert_eq!(seg.first_lsn, Lsn::new(10));
        assert_eq!(seg.last_lsn, Lsn::new(100));
        assert_eq!(seg.size_bytes, 9);
    }

    #[test]
    fn parse_invalid_filenames() {
        let dir = tempfile::tempdir().unwrap();

        // Wrong prefix.
        let path = dir.path().join("data-0a-64.seg");
        std::fs::write(&path, b"x").unwrap();
        assert!(parse_segment_filename(&path).is_none());

        // Wrong part count.
        let path = dir.path().join("wal-0a.seg");
        std::fs::write(&path, b"x").unwrap();
        assert!(parse_segment_filename(&path).is_none());
    }

    #[test]
    fn pending_segments_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config = ArchiverConfig {
            wal_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let archiver = WalArchiver::new(config);
        let pending = archiver.pending_segments().unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn pending_segments_finds_new() {
        let dir = tempfile::tempdir().unwrap();

        // Create two segment files.
        std::fs::write(
            dir.path().join("wal-0000000000000001-000000000000000a.seg"),
            b"seg1",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("wal-000000000000000b-0000000000000014.seg"),
            b"seg2",
        )
        .unwrap();

        let config = ArchiverConfig {
            wal_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let archiver = WalArchiver::new(config);
        let pending = archiver.pending_segments().unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending[0].first_lsn < pending[1].first_lsn); // sorted
    }

    #[test]
    fn plan_archive_generates_tasks() {
        let config = ArchiverConfig {
            archive_url: "s3://my-bucket/wal".into(),
            ..Default::default()
        };
        let archiver = WalArchiver::new(config);

        let segments = vec![WalSegment {
            path: PathBuf::from("/tmp/wal-01-0a.seg"),
            first_lsn: Lsn::new(1),
            last_lsn: Lsn::new(10),
            size_bytes: 4096,
        }];

        let tasks = archiver.plan_archive(&segments);
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].destination.starts_with("s3://my-bucket/wal/"));
        assert!(tasks[0].verify_checksum);
    }

    #[test]
    fn mark_archived_advances_lsn() {
        let config = ArchiverConfig::default();
        let mut archiver = WalArchiver::new(config);

        let task = ArchiveTask {
            source: PathBuf::from("/tmp/seg"),
            destination: "s3://bucket/seg".into(),
            first_lsn: Lsn::new(1),
            last_lsn: Lsn::new(100),
            size_bytes: 1024,
            verify_checksum: false,
        };

        archiver.mark_archived(&task);
        assert_eq!(archiver.state().last_archived_lsn, Lsn::new(100));
        assert_eq!(archiver.state().segments_archived, 1);
        assert_eq!(archiver.state().bytes_archived, 1024);
    }

    #[test]
    fn mark_failed_increments_counter() {
        let config = ArchiverConfig::default();
        let mut archiver = WalArchiver::new(config);

        let task = ArchiveTask {
            source: PathBuf::from("/tmp/seg"),
            destination: "s3://bucket/seg".into(),
            first_lsn: Lsn::new(1),
            last_lsn: Lsn::new(10),
            size_bytes: 512,
            verify_checksum: false,
        };

        archiver.mark_failed(&task, "network timeout");
        assert_eq!(archiver.state().failures, 1);
    }

    #[test]
    fn batch_size_limits_pending() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0u64..20 {
            std::fs::write(
                dir.path()
                    .join(format!("wal-{:016x}-{:016x}.seg", i * 10 + 1, (i + 1) * 10)),
                b"data",
            )
            .unwrap();
        }

        let config = ArchiverConfig {
            wal_dir: dir.path().to_path_buf(),
            batch_size: 5,
            ..Default::default()
        };
        let archiver = WalArchiver::new(config);
        let pending = archiver.pending_segments().unwrap();
        assert_eq!(pending.len(), 5);
    }
}
