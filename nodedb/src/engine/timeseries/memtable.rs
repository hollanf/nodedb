//! L0 RAM memtable for timeseries ingest.
//!
//! Incoming metrics and log entries are buffered in the memtable before
//! being flushed to L1 NVMe segment files. The memtable is organized
//! by series key (metric name + tag set hash) for cache-efficient
//! writes and reads.
//!
//! ## Cardinality Protection
//!
//! The memtable enforces a hard cardinality limit (`max_series`). When the
//! limit is reached, the coldest series (by last write timestamp) is evicted
//! to make room.
//!
//! ## Admission Control
//!
//! `ingest_metric` / `ingest_log` return `IngestResult` which tells the
//! caller whether the memtable needs flushing or has rejected the write.

use std::collections::HashMap;

use nodedb_types::timeseries::{
    FlushedKind, FlushedSeries, IngestResult, LogEntry, MetricSample, SeriesId,
};

use super::gorilla::GorillaEncoder;

// ── Internal buffer types ──────────────────────────────────────────

#[derive(Debug)]
enum SeriesBuffer {
    Metric(MetricBuffer),
    Log(LogBuffer),
}

impl SeriesBuffer {
    fn memory_bytes(&self) -> usize {
        const SERIES_OVERHEAD: usize = 80;
        match self {
            SeriesBuffer::Metric(m) => SERIES_OVERHEAD + m.encoder.compressed_size() + 64,
            SeriesBuffer::Log(l) => SERIES_OVERHEAD + l.total_bytes + l.entries.len() * 40,
        }
    }
}

#[derive(Debug)]
struct MetricBuffer {
    encoder: GorillaEncoder,
    min_ts: i64,
    max_ts: i64,
    sample_count: u64,
}

#[derive(Debug)]
struct LogBuffer {
    entries: Vec<LogEntry>,
    total_bytes: usize,
    min_ts: i64,
    max_ts: i64,
}

#[derive(Debug, Clone, Copy)]
struct SeriesMeta {
    last_write_ts: i64,
    count: u64,
}

// ── Configuration ──────────────────────────────────────────────────

/// Default per-memtable memory budget (64 MiB).
/// Sourced from `TimeseriesToning::memtable_budget_bytes` at runtime.
pub(super) const DEFAULT_MEMTABLE_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Configuration for the timeseries memtable.
#[derive(Debug, Clone)]
pub struct MemtableConfig {
    /// Maximum memory usage before flush is triggered (bytes).
    pub max_memory_bytes: usize,
    /// Maximum number of unique series (cardinality limit).
    pub max_series: usize,
    /// Hard memory ceiling — ingest is rejected above this.
    pub hard_memory_limit: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: DEFAULT_MEMTABLE_BUDGET_BYTES,
            max_series: 500_000,
            hard_memory_limit: 80 * 1024 * 1024,
        }
    }
}

// ── Memtable ───────────────────────────────────────────────────────

/// L0 RAM memtable for timeseries data.
///
/// NOT thread-safe — lives on a single Data Plane core (!Send by design).
pub struct TimeseriesMemtable {
    series: HashMap<SeriesId, SeriesBuffer>,
    series_meta: HashMap<SeriesId, SeriesMeta>,
    memory_bytes: usize,
    config: MemtableConfig,
    metric_count: u64,
    log_count: u64,
    oldest_ts: Option<i64>,
    eviction_count: u64,
    evicted: Vec<FlushedSeries>,
}

impl TimeseriesMemtable {
    pub fn new(max_memory_bytes: usize) -> Self {
        Self::with_config(MemtableConfig {
            max_memory_bytes,
            hard_memory_limit: max_memory_bytes + max_memory_bytes / 4,
            ..Default::default()
        })
    }

    pub fn with_config(config: MemtableConfig) -> Self {
        Self {
            series: HashMap::new(),
            series_meta: HashMap::new(),
            memory_bytes: 0,
            config,
            metric_count: 0,
            log_count: 0,
            oldest_ts: None,
            eviction_count: 0,
            evicted: Vec::new(),
        }
    }

    /// Ingest a metric sample.
    pub fn ingest_metric(&mut self, series_id: SeriesId, sample: MetricSample) -> IngestResult {
        if self.memory_bytes >= self.config.hard_memory_limit {
            return IngestResult::Rejected;
        }

        if !self.series.contains_key(&series_id) && self.series.len() >= self.config.max_series {
            self.evict_coldest_series();
        }

        let is_new = !self.series.contains_key(&series_id);
        let buf = self.series.entry(series_id).or_insert_with(|| {
            SeriesBuffer::Metric(MetricBuffer {
                encoder: GorillaEncoder::new(),
                min_ts: sample.timestamp_ms,
                max_ts: sample.timestamp_ms,
                sample_count: 0,
            })
        });

        if let SeriesBuffer::Metric(m) = buf {
            m.encoder.encode(sample.timestamp_ms, sample.value);
            if sample.timestamp_ms < m.min_ts {
                m.min_ts = sample.timestamp_ms;
            }
            if sample.timestamp_ms > m.max_ts {
                m.max_ts = sample.timestamp_ms;
            }
            m.sample_count += 1;
            self.metric_count += 1;
        }

        self.update_meta(series_id, sample.timestamp_ms);

        if is_new {
            self.recompute_memory();
        } else {
            self.memory_bytes += 3; // Gorilla: ~1-3 bytes per sample
        }

        self.update_oldest(sample.timestamp_ms);
        self.check_flush_state()
    }

    /// Ingest a log entry.
    pub fn ingest_log(&mut self, series_id: SeriesId, entry: LogEntry) -> IngestResult {
        if self.memory_bytes >= self.config.hard_memory_limit {
            return IngestResult::Rejected;
        }

        if !self.series.contains_key(&series_id) && self.series.len() >= self.config.max_series {
            self.evict_coldest_series();
        }

        let entry_size = entry.data.len();
        let ts = entry.timestamp_ms;

        let buf = self.series.entry(series_id).or_insert_with(|| {
            SeriesBuffer::Log(LogBuffer {
                entries: Vec::new(),
                total_bytes: 0,
                min_ts: ts,
                max_ts: ts,
            })
        });

        if let SeriesBuffer::Log(l) = buf {
            if ts < l.min_ts {
                l.min_ts = ts;
            }
            if ts > l.max_ts {
                l.max_ts = ts;
            }
            l.total_bytes += entry_size;
            l.entries.push(entry);
            self.log_count += 1;
        }

        self.update_meta(series_id, ts);
        self.memory_bytes += entry_size + 40;

        self.update_oldest(ts);
        self.check_flush_state()
    }

    /// Whether the memtable should be flushed (memory pressure).
    pub fn should_flush(&self) -> bool {
        self.memory_bytes >= self.config.max_memory_bytes
    }

    /// Drain the memtable, returning all buffered data organized by series.
    pub fn drain(&mut self) -> Vec<FlushedSeries> {
        let mut result = Vec::with_capacity(self.series.len() + self.evicted.len());
        result.append(&mut self.evicted);

        for (series_id, buf) in self.series.drain() {
            match buf {
                SeriesBuffer::Metric(m) => {
                    let sample_count = m.sample_count;
                    let compressed = m.encoder.finish();
                    result.push(FlushedSeries {
                        series_id,
                        kind: FlushedKind::Metric {
                            gorilla_block: compressed,
                            sample_count,
                        },
                        min_ts: m.min_ts,
                        max_ts: m.max_ts,
                    });
                }
                SeriesBuffer::Log(l) => {
                    result.push(FlushedSeries {
                        series_id,
                        kind: FlushedKind::Log {
                            entries: l.entries,
                            total_bytes: l.total_bytes,
                        },
                        min_ts: l.min_ts,
                        max_ts: l.max_ts,
                    });
                }
            }
        }

        self.series_meta.clear();
        self.memory_bytes = 0;
        self.metric_count = 0;
        self.log_count = 0;
        self.oldest_ts = None;

        result
    }

    pub fn metric_count(&self) -> u64 {
        self.metric_count
    }
    pub fn log_count(&self) -> u64 {
        self.log_count
    }
    pub fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }
    pub fn series_count(&self) -> usize {
        self.series.len()
    }
    pub fn oldest_timestamp(&self) -> Option<i64> {
        self.oldest_ts
    }
    pub fn is_empty(&self) -> bool {
        self.series.is_empty() && self.evicted.is_empty()
    }
    pub fn eviction_count(&self) -> u64 {
        self.eviction_count
    }
    pub fn config(&self) -> &MemtableConfig {
        &self.config
    }

    // ── Private helpers ────────────────────────────────────────────

    fn check_flush_state(&self) -> IngestResult {
        if self.memory_bytes >= self.config.max_memory_bytes {
            IngestResult::FlushNeeded
        } else {
            IngestResult::Ok
        }
    }

    fn update_meta(&mut self, series_id: SeriesId, ts: i64) {
        self.series_meta
            .entry(series_id)
            .and_modify(|m| {
                m.last_write_ts = ts;
                m.count += 1;
            })
            .or_insert(SeriesMeta {
                last_write_ts: ts,
                count: 1,
            });
    }

    fn evict_coldest_series(&mut self) {
        let coldest = self
            .series_meta
            .iter()
            .min_by_key(|(_, meta)| meta.last_write_ts)
            .map(|(id, _)| *id);

        let Some(coldest_id) = coldest else { return };

        if let Some(buf) = self.series.remove(&coldest_id) {
            let evicted_mem = buf.memory_bytes();
            let flushed = match buf {
                SeriesBuffer::Metric(m) => {
                    let sample_count = m.sample_count;
                    let compressed = m.encoder.finish();
                    self.metric_count = self.metric_count.saturating_sub(sample_count);
                    FlushedSeries {
                        series_id: coldest_id,
                        kind: FlushedKind::Metric {
                            gorilla_block: compressed,
                            sample_count,
                        },
                        min_ts: m.min_ts,
                        max_ts: m.max_ts,
                    }
                }
                SeriesBuffer::Log(l) => {
                    self.log_count = self.log_count.saturating_sub(l.entries.len() as u64);
                    let total_bytes = l.total_bytes;
                    FlushedSeries {
                        series_id: coldest_id,
                        kind: FlushedKind::Log {
                            entries: l.entries,
                            total_bytes,
                        },
                        min_ts: l.min_ts,
                        max_ts: l.max_ts,
                    }
                }
            };
            self.evicted.push(flushed);
            self.series_meta.remove(&coldest_id);
            self.memory_bytes = self.memory_bytes.saturating_sub(evicted_mem);
            self.eviction_count += 1;
        }
    }

    fn recompute_memory(&mut self) {
        let base_overhead = self.series.len() * 64;
        let series_bytes: usize = self.series.values().map(|b| b.memory_bytes()).sum();
        let meta_bytes = self.series_meta.len() * 32;
        let evicted_bytes: usize = self
            .evicted
            .iter()
            .map(|f| match &f.kind {
                FlushedKind::Metric { gorilla_block, .. } => gorilla_block.len() + 32,
                FlushedKind::Log { total_bytes, .. } => *total_bytes + 32,
                _ => 32,
            })
            .sum();
        self.memory_bytes = base_overhead + series_bytes + meta_bytes + evicted_bytes;
    }

    fn update_oldest(&mut self, ts: i64) {
        match self.oldest_ts {
            None => self.oldest_ts = Some(ts),
            Some(old) if ts < old => self.oldest_ts = Some(ts),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_memtable() {
        let mt = TimeseriesMemtable::new(1024 * 1024);
        assert!(mt.is_empty());
        assert_eq!(mt.metric_count(), 0);
        assert_eq!(mt.log_count(), 0);
    }

    #[test]
    fn ingest_metrics() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i * 10,
                    value: 42.0 + i as f64,
                },
            );
        }
        assert_eq!(mt.metric_count(), 100);
        assert_eq!(mt.series_count(), 1);
        assert_eq!(mt.oldest_timestamp(), Some(1000));
    }

    #[test]
    fn ingest_logs() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        for i in 0..50 {
            mt.ingest_log(
                2,
                LogEntry {
                    timestamp_ms: 2000 + i * 100,
                    data: format!("log line {i}").into_bytes(),
                },
            );
        }
        assert_eq!(mt.log_count(), 50);
        assert_eq!(mt.series_count(), 1);
    }

    #[test]
    fn multiple_series() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        mt.ingest_metric(
            2,
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        assert_eq!(mt.series_count(), 2);
        assert_eq!(mt.metric_count(), 2);
    }

    #[test]
    fn drain_returns_all() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        for i in 0..5 {
            mt.ingest_metric(
                i,
                MetricSample {
                    timestamp_ms: 1000 + i as i64,
                    value: i as f64,
                },
            );
        }
        let flushed = mt.drain();
        assert_eq!(flushed.len(), 5);
        assert!(mt.is_empty());
        assert_eq!(mt.metric_count(), 0);
    }

    #[test]
    fn cardinality_eviction() {
        let config = MemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            max_series: 3,
            hard_memory_limit: 20 * 1024 * 1024,
        };
        let mut mt = TimeseriesMemtable::with_config(config);

        // Insert 3 series (at limit).
        for id in 0..3 {
            mt.ingest_metric(
                id,
                MetricSample {
                    timestamp_ms: 1000 + id as i64,
                    value: 1.0,
                },
            );
        }
        assert_eq!(mt.series_count(), 3);
        assert_eq!(mt.eviction_count(), 0);

        // Insert a 4th series — should evict the coldest.
        mt.ingest_metric(
            99,
            MetricSample {
                timestamp_ms: 5000,
                value: 99.0,
            },
        );
        assert_eq!(mt.series_count(), 3);
        assert_eq!(mt.eviction_count(), 1);

        // Drain should include the evicted series.
        let flushed = mt.drain();
        assert_eq!(flushed.len(), 4); // 3 active + 1 evicted
    }

    #[test]
    fn hard_limit_rejection() {
        let config = MemtableConfig {
            max_memory_bytes: 512,
            max_series: 100,
            hard_memory_limit: 1024,
        };
        let mut mt = TimeseriesMemtable::with_config(config);

        // Fill until hard limit.
        let mut rejected = false;
        for i in 0..10_000 {
            let result = mt.ingest_log(
                i % 10,
                LogEntry {
                    timestamp_ms: i as i64,
                    data: vec![0u8; 100],
                },
            );
            if result.is_rejected() {
                rejected = true;
                break;
            }
        }
        assert!(rejected, "should reject at hard memory limit");
    }

    #[test]
    fn segment_index_basic() {
        use super::super::segment_index::SegmentIndex;
        use nodedb_types::timeseries::{SegmentKind, SegmentRef, TimeRange};

        let mut idx = SegmentIndex::new();
        idx.add(
            1,
            SegmentRef {
                path: "seg1.bin".into(),
                min_ts: 1000,
                max_ts: 2000,
                kind: SegmentKind::Metric,
                size_bytes: 4096,
                created_at_ms: 0,
            },
        );
        idx.add(
            1,
            SegmentRef {
                path: "seg2.bin".into(),
                min_ts: 3000,
                max_ts: 4000,
                kind: SegmentKind::Metric,
                size_bytes: 8192,
                created_at_ms: 0,
            },
        );

        let hits = idx.query(1, &TimeRange::new(1500, 2500));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "seg1.bin");

        let all = idx.query(1, &TimeRange::new(0, 5000));
        assert_eq!(all.len(), 2);

        assert_eq!(idx.total_segments(), 2);
        assert_eq!(idx.total_bytes(), 4096 + 8192);
    }
}
