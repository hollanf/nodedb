//! Double-buffered memtable manager with admission control.
//!
//! Implements the industry-standard pattern used by RocksDB, ClickHouse, and
//! Prometheus: two memtables (active + frozen) so that ingest can continue
//! while the frozen memtable is being flushed to L1.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │  TimeseriesManager                                    │
//! │                                                       │
//! │  ┌─────────────┐     freeze()    ┌────────────────┐  │
//! │  │   Active     │ ──────────────▶│    Frozen       │  │
//! │  │  Memtable    │                │   Memtable      │  │
//! │  │  (writable)  │                │  (read-only,    │  │
//! │  │              │                │   being flushed) │  │
//! │  └─────────────┘                └────────────────┘  │
//! │         │                               │            │
//! │      ingest()                    flush_frozen()       │
//! │                                         │            │
//! │                               ┌─────────▼──────────┐│
//! │                               │   BucketManager     ││
//! │                               │   (L1 segments)     ││
//! │                               └────────────────────┘│
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! ## Memory Budget
//!
//! Total memory budget is split: active memtable gets 60%, frozen reservation
//! gets 40%. This ensures the frozen memtable always has room to exist
//! alongside the active one during flush.

use super::bucket::{BucketConfig, BucketManager};
use super::compress::LogDictionary;
use nodedb_types::timeseries::{FlushedSeries, IngestResult, LogEntry, MetricSample, SeriesId};

use super::memtable::{MemtableConfig, TimeseriesMemtable};

/// Default total timeseries memory budget (100 MiB, covers both memtables).
/// Sourced from `TimeseriesToning::total_budget_bytes` at runtime.
pub(super) const DEFAULT_TOTAL_BUDGET_BYTES: usize = 100 * 1024 * 1024;

/// Configuration for the timeseries manager.
#[derive(Debug, Clone)]
pub struct TimeseriesManagerConfig {
    /// Total memory budget for the timeseries engine (both memtables).
    pub total_memory_budget: usize,
    /// Maximum cardinality (unique series) per memtable.
    pub max_series: usize,
    /// Bucket/tiering configuration.
    pub bucket_config: BucketConfig,
    /// Fraction of total budget for the active memtable (default: 0.6).
    pub active_budget_fraction: f64,
}

impl Default for TimeseriesManagerConfig {
    fn default() -> Self {
        Self {
            total_memory_budget: DEFAULT_TOTAL_BUDGET_BYTES,
            max_series: 500_000,
            bucket_config: BucketConfig::default(),
            active_budget_fraction: 0.6,
        }
    }
}

/// Manages the timeseries engine lifecycle: double-buffered memtables,
/// flush coordination, and admission control.
///
/// NOT thread-safe — lives on a single Data Plane core (!Send by design).
pub struct TimeseriesManager {
    /// Active memtable receiving ingest.
    active: TimeseriesMemtable,
    /// Frozen memtable being flushed (if any).
    frozen: Option<Vec<FlushedSeries>>,
    /// Bucket manager for L0→L1→L2 tiering.
    bucket: BucketManager,
    /// Optional log compression dictionary.
    log_dict: Option<LogDictionary>,
    /// Configuration.
    config: TimeseriesManagerConfig,
    /// Stats: total flushes completed.
    flush_count: u64,
    /// Stats: total samples ingested.
    total_ingested: u64,
    /// Stats: total ingest rejections.
    total_rejected: u64,
}

impl TimeseriesManager {
    pub fn new(config: TimeseriesManagerConfig) -> Self {
        let active_budget =
            (config.total_memory_budget as f64 * config.active_budget_fraction) as usize;
        let hard_limit = config.total_memory_budget;

        let mt_config = MemtableConfig {
            max_memory_bytes: active_budget,
            max_series: config.max_series,
            hard_memory_limit: hard_limit,
        };

        Self {
            active: TimeseriesMemtable::with_config(mt_config),
            frozen: None,
            bucket: BucketManager::new(config.bucket_config.clone()),
            log_dict: None,
            config,
            flush_count: 0,
            total_ingested: 0,
            total_rejected: 0,
        }
    }

    /// Set the log compression dictionary.
    pub fn set_log_dictionary(&mut self, dict: LogDictionary) {
        self.log_dict = Some(dict);
    }

    /// Ingest a metric sample.
    ///
    /// If the active memtable signals flush needed, the caller should call
    /// `try_flush()` to rotate and flush the frozen memtable.
    pub fn ingest_metric(&mut self, series_id: SeriesId, sample: MetricSample) -> IngestResult {
        self.do_ingest(|mt| mt.ingest_metric(series_id, sample))
    }

    /// Ingest a log entry.
    pub fn ingest_log(&mut self, series_id: SeriesId, entry: LogEntry) -> IngestResult {
        let entry_clone = entry.clone();
        self.do_ingest(|mt| mt.ingest_log(series_id, entry_clone.clone()))
    }

    /// Shared ingest logic: try active, emergency-flush frozen if rejected, retry.
    fn do_ingest(
        &mut self,
        ingest_fn: impl Fn(&mut TimeseriesMemtable) -> IngestResult,
    ) -> IngestResult {
        let result = ingest_fn(&mut self.active);
        match result {
            IngestResult::Rejected => {
                if self.frozen.is_some() {
                    self.flush_frozen();
                }
                let retry = ingest_fn(&mut self.active);
                if retry.is_rejected() {
                    self.total_rejected += 1;
                }
                self.total_ingested += 1;
                retry
            }
            _ => {
                self.total_ingested += 1;
                result
            }
        }
    }

    /// Attempt to rotate and flush. Call this when `IngestResult::FlushNeeded`.
    ///
    /// 1. If frozen is still pending, flush it first (blocking).
    /// 2. Freeze the active memtable (drain → frozen).
    /// 3. Create a new empty active memtable.
    /// 4. Flush the frozen data to L1.
    ///
    /// Returns the number of segments written to L1.
    pub fn try_flush(&mut self) -> std::io::Result<usize> {
        // If there's a pending frozen buffer, flush it first.
        if self.frozen.is_some() {
            self.flush_frozen();
        }

        // Freeze current active memtable.
        let flushed_data = self.active.drain();
        if flushed_data.is_empty() {
            return Ok(0);
        }

        self.frozen = Some(flushed_data);
        self.flush_frozen();

        Ok(self.bucket.l1_segments_written() as usize)
    }

    /// Flush the frozen memtable to L1 segments.
    fn flush_frozen(&mut self) {
        let Some(data) = self.frozen.take() else {
            return;
        };

        let dict = self.log_dict.as_ref();
        match self.bucket.flush_to_l1(data, dict) {
            Ok(count) => {
                self.flush_count += 1;
                tracing::info!(
                    segments = count,
                    flush_count = self.flush_count,
                    "timeseries L0→L1 flush complete"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "timeseries L0→L1 flush failed");
                // Data is lost if flush fails. In production, this should
                // trigger an alert and the WAL replay path should recover.
            }
        }
    }

    /// Run retention enforcement: delete segments older than the given TTL.
    ///
    /// Returns the number of segments deleted.
    pub fn enforce_retention(&mut self, max_age_ms: i64, now_ms: i64) -> std::io::Result<usize> {
        let cutoff = now_ms - max_age_ms;
        let old_segments = self.bucket.segment_index().segments_older_than(cutoff);

        let mut deleted = 0;
        for (series_id, min_ts, seg) in &old_segments {
            let full_path = self.config.bucket_config.l1_dir.join(&seg.path);
            match std::fs::remove_file(&full_path) {
                Ok(()) => {
                    self.bucket.segment_index_mut().remove(*series_id, *min_ts);
                    deleted += 1;
                    tracing::debug!(
                        path = %seg.path,
                        series_id = series_id,
                        "deleted expired segment"
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already deleted, just clean up index.
                    self.bucket.segment_index_mut().remove(*series_id, *min_ts);
                    deleted += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        path = %seg.path,
                        error = %e,
                        "failed to delete expired segment"
                    );
                }
            }
        }

        if deleted > 0 {
            tracing::info!(
                deleted,
                cutoff_ms = cutoff,
                "retention enforcement complete"
            );
        }

        Ok(deleted)
    }

    /// Run L1→L2 compaction for segments older than the given timestamp.
    pub fn compact_to_l2(
        &mut self,
        before_ts: i64,
    ) -> std::io::Result<super::bucket::CompactionResult> {
        self.bucket.compact_to_l2(before_ts)
    }

    /// Get a reference to the bucket manager (for queries).
    pub fn bucket(&self) -> &BucketManager {
        &self.bucket
    }

    // --- Stats ---

    pub fn flush_count(&self) -> u64 {
        self.flush_count
    }

    pub fn total_ingested(&self) -> u64 {
        self.total_ingested
    }

    pub fn total_rejected(&self) -> u64 {
        self.total_rejected
    }

    pub fn active_memory_bytes(&self) -> usize {
        self.active.memory_bytes()
    }

    pub fn active_series_count(&self) -> usize {
        self.active.series_count()
    }

    pub fn active_eviction_count(&self) -> u64 {
        self.active.eviction_count()
    }

    pub fn has_frozen(&self) -> bool {
        self.frozen.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> TimeseriesManagerConfig {
        TimeseriesManagerConfig {
            total_memory_budget: 1024 * 1024, // 1 MiB
            max_series: 100,
            bucket_config: BucketConfig {
                l1_dir: dir.path().join("l1"),
                l2_dir: dir.path().join("l2"),
                ..Default::default()
            },
            active_budget_fraction: 0.6,
        }
    }

    #[test]
    fn basic_ingest_and_flush() {
        let dir = TempDir::new().unwrap();
        let mut mgr = TimeseriesManager::new(test_config(&dir));

        for i in 0..100 {
            mgr.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: i as f64,
                },
            );
        }

        let segments = mgr.try_flush().unwrap();
        assert!(segments > 0);
        assert_eq!(mgr.flush_count(), 1);
        assert_eq!(mgr.active_series_count(), 0);
    }

    #[test]
    fn double_buffer_allows_continuous_ingest() {
        let dir = TempDir::new().unwrap();
        let config = TimeseriesManagerConfig {
            total_memory_budget: 2048,
            max_series: 10,
            bucket_config: BucketConfig {
                l1_dir: dir.path().join("l1"),
                l2_dir: dir.path().join("l2"),
                ..Default::default()
            },
            active_budget_fraction: 0.6,
        };
        let mut mgr = TimeseriesManager::new(config);

        // Fill until flush needed.
        let mut flush_needed = false;
        for i in 0..500 {
            let result = mgr.ingest_metric(
                (i % 5) as u64,
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: i as f64,
                },
            );
            if result.is_flush_needed() {
                flush_needed = true;
                mgr.try_flush().unwrap();
                // Can continue ingesting after flush.
                let post_flush = mgr.ingest_metric(
                    1,
                    MetricSample {
                        timestamp_ms: 999_999,
                        value: 0.0,
                    },
                );
                assert!(!post_flush.is_rejected());
                break;
            }
        }
        assert!(flush_needed, "should have triggered flush");
    }

    #[test]
    fn retention_deletes_old_segments() {
        let dir = TempDir::new().unwrap();
        let mut mgr = TimeseriesManager::new(test_config(&dir));

        // Ingest and flush to create segments.
        mgr.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        mgr.ingest_metric(
            2,
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        mgr.try_flush().unwrap();

        // All segments have max_ts <= 200. With now=1000 and max_age=500,
        // cutoff = 500, so max_ts(200) < 500 → should be deleted.
        let deleted = mgr.enforce_retention(500, 1000).unwrap();
        assert_eq!(deleted, 2);
    }

    #[test]
    fn cardinality_eviction_through_manager() {
        let dir = TempDir::new().unwrap();
        let config = TimeseriesManagerConfig {
            total_memory_budget: 10 * 1024 * 1024,
            max_series: 3,
            bucket_config: BucketConfig {
                l1_dir: dir.path().join("l1"),
                l2_dir: dir.path().join("l2"),
                ..Default::default()
            },
            active_budget_fraction: 0.6,
        };
        let mut mgr = TimeseriesManager::new(config);

        // Fill to cardinality limit.
        mgr.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        mgr.ingest_metric(
            2,
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        mgr.ingest_metric(
            3,
            MetricSample {
                timestamp_ms: 300,
                value: 3.0,
            },
        );

        // 4th series triggers eviction.
        mgr.ingest_metric(
            4,
            MetricSample {
                timestamp_ms: 400,
                value: 4.0,
            },
        );
        assert_eq!(mgr.active_series_count(), 3);
        assert_eq!(mgr.active_eviction_count(), 1);
    }
}
