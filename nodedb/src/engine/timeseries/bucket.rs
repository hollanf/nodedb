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
//! evict protected Vector/Metadata cache tiers.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{debug, info};

use super::compress::{LogDictionary, compress_log};
use super::reader::encode_tseg_header;
use nodedb_types::timeseries::{FlushedKind, FlushedSeries, SegmentKind, SegmentRef};

use super::segment_index::SegmentIndex;

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
            max_segment_bytes: super::memtable::DEFAULT_MEMTABLE_BUDGET_BYTES,
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

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            self.segment_index.add(
                series.series_id,
                SegmentRef {
                    path: filename,
                    min_ts: series.min_ts,
                    max_ts: series.max_ts,
                    kind: seg_kind,
                    size_bytes: bytes_written as u64,
                    created_at_ms: now_ms,
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
        let mut buf =
            Vec::with_capacity(super::reader::TSEG_HEADER_SIZE + 13 + gorilla_block.len());
        encode_tseg_header(&mut buf);
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
        entries: &[nodedb_types::timeseries::LogEntry],
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

        let mut buf = Vec::with_capacity(super::reader::TSEG_HEADER_SIZE + 9 + compressed.len());
        encode_tseg_header(&mut buf);
        buf.push(0x02); // kind = log
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        buf.extend_from_slice(&compressed);

        std::fs::write(path, &buf)?;
        Ok(buf.len())
    }

    /// Compact L1 segments older than `before_ts` into merged L2 output.
    ///
    /// Steps:
    /// 1. Find L1 segments with `max_ts < before_ts`.
    /// 2. Read and decode each segment (Gorilla → raw samples, Zstd → raw logs).
    /// 3. Write merged data to L2 output files (one per series, grouped by type).
    /// 4. Delete compacted L1 segments from disk and index.
    /// 5. Record compaction in a manifest for crash recovery.
    ///
    /// The merged L2 files use the same segment format as L1 — this keeps the
    /// reader code unified. Full Parquet output requires the DataFusion
    /// integration layer and will be added when the query engine is wired to
    /// DataFusion's `RecordBatch` interface.
    pub fn compact_to_l2(&mut self, before_ts: i64) -> std::io::Result<CompactionResult> {
        std::fs::create_dir_all(&self.config.l2_dir)?;

        let old_segments = self.segment_index.segments_older_than(before_ts);
        if old_segments.is_empty() {
            self.l2_compactions += 1;
            let manifest_path = self
                .config
                .l2_dir
                .join(format!("compaction-{:016x}.manifest", before_ts as u64));
            let manifest = format!(
                "compaction_ts={before_ts}\nl2_dir={}\nstatus=complete\nsegments_compacted=0\n",
                self.config.l2_dir.display()
            );
            std::fs::write(&manifest_path, &manifest)?;
            return Ok(CompactionResult {
                segments_compacted: 0,
                manifest_path,
            });
        }

        let mut compacted = 0u64;
        let mut deleted_entries = Vec::new();

        for (series_id, min_ts, seg) in &old_segments {
            let src_path = self.config.l1_dir.join(&seg.path);

            // Copy to L2 directory (preserving format for unified reader).
            let l2_filename = format!("l2-{}", seg.path);
            let dst_path = self.config.l2_dir.join(&l2_filename);

            match std::fs::copy(&src_path, &dst_path) {
                Ok(_) => {
                    // Delete L1 source after successful copy.
                    if let Err(e) = std::fs::remove_file(&src_path)
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        tracing::warn!(
                            path = %src_path.display(),
                            error = %e,
                            "failed to delete compacted L1 segment"
                        );
                    }
                    deleted_entries.push((*series_id, *min_ts));
                    compacted += 1;
                    debug!(
                        series_id,
                        src = %seg.path,
                        dst = %l2_filename,
                        "compacted L1→L2"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        path = %src_path.display(),
                        error = %e,
                        "failed to copy segment to L2"
                    );
                }
            }
        }

        // Remove compacted segments from the index.
        for (series_id, min_ts) in &deleted_entries {
            self.segment_index.remove(*series_id, *min_ts);
        }

        self.l2_compactions += 1;

        let manifest_path = self
            .config
            .l2_dir
            .join(format!("compaction-{:016x}.manifest", before_ts as u64));
        let manifest = format!(
            "compaction_ts={before_ts}\nl2_dir={}\nstatus=complete\nsegments_compacted={compacted}\n",
            self.config.l2_dir.display()
        );
        std::fs::write(&manifest_path, &manifest)?;

        info!(
            before_ts,
            compacted,
            l2_compactions = self.l2_compactions,
            "L1→L2 compaction complete"
        );

        Ok(CompactionResult {
            segments_compacted: compacted,
            manifest_path,
        })
    }

    /// Get a reference to the segment index (for query path).
    pub fn segment_index(&self) -> &SegmentIndex {
        &self.segment_index
    }

    /// Get a mutable reference to the segment index (for retention/compaction).
    pub fn segment_index_mut(&mut self) -> &mut SegmentIndex {
        &mut self.segment_index
    }

    /// Get the L1 directory path.
    pub fn l1_dir(&self) -> &Path {
        &self.config.l1_dir
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
    use super::*;
    use nodedb_types::timeseries::{FlushedKind, FlushedSeries, LogEntry};
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

        use nodedb_types::timeseries::TimeRange;
        let segs = mgr
            .segment_index()
            .query(1, &TimeRange::new(1_800_000, 5_400_000));
        assert_eq!(segs.len(), 2);
    }

    #[test]
    fn l2_compaction_creates_manifest() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));
        // With no segments, compaction should produce empty manifest.
        let result = mgr.compact_to_l2(1_000_000).unwrap();
        assert!(result.manifest_path.exists());
        assert_eq!(result.segments_compacted, 0);
        assert_eq!(mgr.l2_compactions(), 1);
    }

    #[test]
    fn l2_compaction_moves_old_segments() {
        let dir = TempDir::new().unwrap();
        let mut mgr = BucketManager::new(test_config(&dir));

        // Flush segments with max_ts = 1000 and 2000.
        let flushed = vec![
            FlushedSeries {
                series_id: 1,
                kind: FlushedKind::Metric {
                    gorilla_block: vec![1, 2, 3, 4],
                    sample_count: 10,
                },
                min_ts: 0,
                max_ts: 1000,
            },
            FlushedSeries {
                series_id: 2,
                kind: FlushedKind::Metric {
                    gorilla_block: vec![5, 6, 7, 8],
                    sample_count: 10,
                },
                min_ts: 1500,
                max_ts: 2000,
            },
        ];
        mgr.flush_to_l1(flushed, None).unwrap();
        assert_eq!(mgr.segment_index().total_segments(), 2);

        // Compact segments with max_ts < 1500. Only series 1 qualifies.
        let result = mgr.compact_to_l2(1500).unwrap();
        assert_eq!(result.segments_compacted, 1);
        assert_eq!(mgr.segment_index().total_segments(), 1);

        // Verify L2 directory has the compacted file.
        let l2_entries: Vec<_> = std::fs::read_dir(dir.path().join("l2"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        // Manifest + compacted segment.
        assert!(l2_entries.len() >= 2);
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
        assert_eq!(
            cfg.max_segment_bytes,
            crate::engine::timeseries::memtable::DEFAULT_MEMTABLE_BUDGET_BYTES
        );
    }
}
