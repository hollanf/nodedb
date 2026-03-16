//! Tiered bucketing for timeseries data.
//!
//! Data flows through three tiers:
//! - **L0 (RAM)**: Active memtable receiving ingest.
//! - **L1 (NVMe)**: Hourly flush to segment files (Gorilla blocks for metrics,
//!   Zstd-compressed blocks for logs).
//! - **L2 (Remote/Parquet)**: Daily compaction into Parquet columnar format
//!   for cold storage (S3/GCS).
//!
//! Flush scheduling respects cache protection: timeseries flushes MUST NOT
//! evict protected Vector/Metadata cache tiers (TDD §3.2 normative).

use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{debug, info};

use super::compress::{LogDictionary, compress_log};
use super::memtable::{FlushedKind, FlushedSeries, SegmentIndex, SegmentKind, SegmentRef};

/// Configuration for the tiered bucketing system.
#[derive(Debug, Clone)]
pub struct BucketConfig {
    /// L1 segment directory on NVMe.
    pub l1_dir: PathBuf,
    /// L2 output directory (local staging before remote push).
    pub l2_dir: PathBuf,
    /// How often to flush L0 → L1 (default: 1 hour).
    pub l1_flush_interval: Duration,
    /// How often to compact L1 → L2 (default: 24 hours).
    pub l2_compaction_interval: Duration,
    /// Zstd compression level for log blocks (default: 3).
    pub log_compression_level: i32,
    /// Maximum L1 segment size in bytes before rotation (default: 64 MiB).
    pub max_segment_bytes: usize,
    /// Rate limit for flush I/O (bytes/sec, 0 = unlimited).
    /// Prevents timeseries flushes from evicting protected cache tiers.
    pub flush_rate_limit_bytes_per_sec: u64,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            l1_dir: PathBuf::from("/tmp/nodedb/ts/l1"),
            l2_dir: PathBuf::from("/tmp/nodedb/ts/l2"),
            l1_flush_interval: Duration::from_secs(3600),
            l2_compaction_interval: Duration::from_secs(86400),
            log_compression_level: 3,
            max_segment_bytes: 64 * 1024 * 1024,
            flush_rate_limit_bytes_per_sec: 0,
        }
    }
}

/// Manages the L0→L1→L2 tiered lifecycle for timeseries data.
pub struct BucketManager {
    config: BucketConfig,
    /// Segment index tracking all L1 segments.
    segment_index: SegmentIndex,
    /// Monotonic segment counter for unique filenames.
    segment_counter: u64,
    /// Total bytes flushed to L1.
    l1_bytes_written: u64,
    /// Total segments flushed to L1.
    l1_segments_written: u64,
    /// Total L2 compaction runs.
    l2_compactions: u64,
}

impl BucketManager {
    pub fn new(config: BucketConfig) -> Self {
        Self {
            config,
            segment_index: SegmentIndex::new(),
            segment_counter: 0,
            l1_bytes_written: 0,
            l1_segments_written: 0,
            l2_compactions: 0,
        }
    }

    /// Flush drained memtable data to L1 NVMe segments.
    ///
    /// Each series gets its own segment file. Metrics are already Gorilla-
    /// compressed; logs are Zstd-compressed here.
    ///
    /// Returns the number of segments written.
    pub fn flush_to_l1(
        &mut self,
        flushed: Vec<FlushedSeries>,
        dict: Option<&LogDictionary>,
    ) -> std::io::Result<usize> {
        if flushed.is_empty() {
            return Ok(0);
        }

        std::fs::create_dir_all(&self.config.l1_dir)?;
        let mut segments_written = 0;

        for series in flushed {
            let seg_id = self.next_segment_id();
            let kind_label = match &series.kind {
                FlushedKind::Metric { .. } => "metric",
                FlushedKind::Log { .. } => "log",
            };
            let filename = format!(
                "ts-{:016x}-{}-{seg_id:08}.seg",
                series.series_id, kind_label
            );
            let path = self.config.l1_dir.join(&filename);

            let bytes_written = match &series.kind {
                FlushedKind::Metric {
                    gorilla_block,
                    sample_count,
                } => self.write_metric_segment(&path, gorilla_block, *sample_count)?,
                FlushedKind::Log { entries, .. } => self.write_log_segment(&path, entries, dict)?,
            };

            let seg_kind = match &series.kind {
                FlushedKind::Metric { .. } => SegmentKind::Metric,
                FlushedKind::Log { .. } => SegmentKind::Log,
            };

            self.segment_index.add(
                series.series_id,
                SegmentRef {
                    path: filename,
                    min_ts: series.min_ts,
                    max_ts: series.max_ts,
                    kind: seg_kind,
                },
            );

            self.l1_bytes_written += bytes_written as u64;
            self.l1_segments_written += 1;
            segments_written += 1;

            debug!(
                series_id = series.series_id,
                segment = seg_id,
                bytes = bytes_written,
                "flushed L1 segment"
            );
        }

        info!(
            segments = segments_written,
            total_l1_bytes = self.l1_bytes_written,
            "L0→L1 flush complete"
        );

        Ok(segments_written)
    }

    fn write_metric_segment(
        &self,
        path: &Path,
        gorilla_block: &[u8],
        sample_count: u64,
    ) -> std::io::Result<usize> {
        // Segment format: header(16) + gorilla_block.
        // Header: magic(4) + kind(1) + sample_count(8) + block_len(4).
        let mut buf = Vec::with_capacity(17 + gorilla_block.len());
        buf.extend_from_slice(b"TSEG"); // magic
        buf.push(0x01); // kind = metric
        buf.extend_from_slice(&sample_count.to_le_bytes());
        buf.extend_from_slice(&(gorilla_block.len() as u32).to_le_bytes());
        buf.extend_from_slice(gorilla_block);

        std::fs::write(path, &buf)?;
        Ok(buf.len())
    }

    fn write_log_segment(
        &self,
        path: &Path,
        entries: &[super::memtable::LogEntry],
        dict: Option<&LogDictionary>,
    ) -> std::io::Result<usize> {
        // Concatenate all log entries, then compress as one block.
        let mut raw = Vec::new();
        for entry in entries {
            raw.extend_from_slice(&entry.timestamp_ms.to_le_bytes());
            raw.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
            raw.extend_from_slice(&entry.data);
        }

        let compressed = compress_log(&raw, dict, self.config.log_compression_level);

        // Segment format: header(13) + compressed_block.
        let mut buf = Vec::with_capacity(13 + compressed.len());
        buf.extend_from_slice(b"TSEG"); // magic
        buf.push(0x02); // kind = log
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        buf.extend_from_slice(&compressed);

        std::fs::write(path, &buf)?;
        Ok(buf.len())
    }

    /// Schedule L2 compaction: merge L1 segments into Parquet columnar files.
    ///
    /// This is a placeholder for the full Parquet writer integration.
    /// Currently produces a manifest file listing segments to be compacted.
    pub fn compact_to_l2(&mut self, before_ts: i64) -> std::io::Result<CompactionResult> {
        std::fs::create_dir_all(&self.config.l2_dir)?;

        let compacted_segments = 0u64;
        let manifest_path = self
            .config
            .l2_dir
            .join(format!("compaction-{:016x}.manifest", before_ts as u64));

        // In production, this would:
        // 1. Read L1 segments older than `before_ts`.
        // 2. Decode Gorilla blocks / decompress log blocks.
        // 3. Write Parquet columnar files with predicate pushdown metadata.
        // 4. Delete compacted L1 segments.
        // 5. Update segment index.

        // For now, emit a manifest as proof of the compaction contract.
        let manifest = format!(
            "compaction_ts={before_ts}\nl2_dir={}\nstatus=scheduled\n",
            self.config.l2_dir.display()
        );
        std::fs::write(&manifest_path, manifest)?;

        self.l2_compactions += 1;

        info!(
            before_ts,
            compacted = compacted_segments,
            l2_compactions = self.l2_compactions,
            "L1→L2 compaction scheduled"
        );

        Ok(CompactionResult {
            segments_compacted: compacted_segments,
            manifest_path,
        })
    }

    /// Get a reference to the segment index (for query path).
    pub fn segment_index(&self) -> &SegmentIndex {
        &self.segment_index
    }

    pub fn l1_bytes_written(&self) -> u64 {
        self.l1_bytes_written
    }

    pub fn l1_segments_written(&self) -> u64 {
        self.l1_segments_written
    }

    pub fn l2_compactions(&self) -> u64 {
        self.l2_compactions
    }

    fn next_segment_id(&mut self) -> u64 {
        self.segment_counter += 1;
        self.segment_counter
    }
}

/// Result of an L1→L2 compaction run.
#[derive(Debug)]
pub struct CompactionResult {
    pub segments_compacted: u64,
    pub manifest_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::super::memtable::{FlushedKind, FlushedSeries, LogEntry};
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> BucketConfig {
        BucketConfig {
            l1_dir: dir.path().join("l1"),
            l2_dir: dir.path().join("l2"),
            ..Default::default()
        }
    }

    #[test]
    fn flush_metric_segments() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));

        let flushed = vec![FlushedSeries {
            series_id: 1,
            kind: FlushedKind::Metric {
                gorilla_block: vec![0xDE, 0xAD, 0xBE, 0xEF],
                sample_count: 100,
            },
            min_ts: 1000,
            max_ts: 2000,
        }];

        let count = mgr.flush_to_l1(flushed, None).unwrap();
        assert_eq!(count, 1);
        assert_eq!(mgr.l1_segments_written(), 1);
        assert!(mgr.l1_bytes_written() > 0);

        // Segment file exists.
        let entries: Vec<_> = std::fs::read_dir(dir.path().join("l1")).unwrap().collect();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn flush_log_segments() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));

        let flushed = vec![FlushedSeries {
            series_id: 2,
            kind: FlushedKind::Log {
                entries: vec![
                    LogEntry {
                        timestamp_ms: 100,
                        data: b"log line 1".to_vec(),
                    },
                    LogEntry {
                        timestamp_ms: 200,
                        data: b"log line 2".to_vec(),
                    },
                ],
                total_bytes: 20,
            },
            min_ts: 100,
            max_ts: 200,
        }];

        let count = mgr.flush_to_l1(flushed, None).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn flush_empty_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));
        let count = mgr.flush_to_l1(vec![], None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn segment_index_populated_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));

        let flushed = vec![
            FlushedSeries {
                series_id: 1,
                kind: FlushedKind::Metric {
                    gorilla_block: vec![1, 2, 3],
                    sample_count: 10,
                },
                min_ts: 0,
                max_ts: 3_600_000,
            },
            FlushedSeries {
                series_id: 1,
                kind: FlushedKind::Metric {
                    gorilla_block: vec![4, 5, 6],
                    sample_count: 10,
                },
                min_ts: 3_600_000,
                max_ts: 7_200_000,
            },
        ];

        mgr.flush_to_l1(flushed, None).unwrap();

        use super::super::memtable::TimeRange;
        let segs = mgr
            .segment_index()
            .query(1, &TimeRange::new(1_800_000, 5_400_000));
        assert_eq!(segs.len(), 2);
    }

    #[test]
    fn l2_compaction_creates_manifest() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));
        let result = mgr.compact_to_l2(1_000_000).unwrap();
        assert!(result.manifest_path.exists());
        assert_eq!(mgr.l2_compactions(), 1);
    }

    #[test]
    fn multiple_flushes_increment_counters() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));

        for i in 0..3 {
            let flushed = vec![FlushedSeries {
                series_id: i,
                kind: FlushedKind::Metric {
                    gorilla_block: vec![0; 10],
                    sample_count: 5,
                },
                min_ts: i as i64 * 1000,
                max_ts: (i as i64 + 1) * 1000,
            }];
            mgr.flush_to_l1(flushed, None).unwrap();
        }

        assert_eq!(mgr.l1_segments_written(), 3);
    }

    #[test]
    fn config_defaults() {
        let cfg = BucketConfig::default();
        assert_eq!(cfg.l1_flush_interval, Duration::from_secs(3600));
        assert_eq!(cfg.l2_compaction_interval, Duration::from_secs(86400));
        assert_eq!(cfg.log_compression_level, 3);
        assert_eq!(cfg.max_segment_bytes, 64 * 1024 * 1024);
    }
}
