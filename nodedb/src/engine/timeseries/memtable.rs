//! L0 RAM memtable for timeseries ingest.
//!
//! Incoming metrics and log entries are buffered in the memtable before
//! being flushed to L1 NVMe segment files. The memtable is organized
//! by series key (metric name + tag set hash) for cache-efficient
//! writes and reads.

use std::collections::{BTreeMap, HashMap};

use super::gorilla::GorillaEncoder;

/// Unique identifier for a timeseries (metric name + tag set).
pub type SeriesId = u64;

/// A single metric sample.
#[derive(Debug, Clone, Copy)]
pub struct MetricSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A single log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp_ms: i64,
    pub data: Vec<u8>,
}

/// Per-series state in the memtable.
#[derive(Debug)]
enum SeriesBuffer {
    Metric(MetricBuffer),
    Log(LogBuffer),
}

/// Buffer for metric samples (Gorilla-compressed in-memory).
#[derive(Debug)]
struct MetricBuffer {
    encoder: GorillaEncoder,
    min_ts: i64,
    max_ts: i64,
}

/// Buffer for log entries (raw bytes, compressed on flush).
#[derive(Debug)]
struct LogBuffer {
    entries: Vec<LogEntry>,
    total_bytes: usize,
    min_ts: i64,
    max_ts: i64,
}

/// L0 RAM memtable for timeseries data.
///
/// Organizes data by series ID. Metric samples are Gorilla-compressed
/// incrementally. Log entries are buffered raw and compressed on flush.
///
/// The memtable is NOT thread-safe — it lives on a single Data Plane
/// core (!Send by design).
pub struct TimeseriesMemtable {
    series: HashMap<SeriesId, SeriesBuffer>,
    /// Total approximate memory usage in bytes.
    memory_bytes: usize,
    /// Maximum memory before forced flush (bytes).
    max_memory_bytes: usize,
    /// Metric sample count across all series.
    metric_count: u64,
    /// Log entry count across all series.
    log_count: u64,
    /// Creation timestamp of oldest unflushed data.
    oldest_ts: Option<i64>,
}

impl TimeseriesMemtable {
    pub fn new(max_memory_bytes: usize) -> Self {
        Self {
            series: HashMap::new(),
            memory_bytes: 0,
            max_memory_bytes,
            metric_count: 0,
            log_count: 0,
            oldest_ts: None,
        }
    }

    /// Ingest a metric sample. Returns true if memtable should be flushed.
    pub fn ingest_metric(&mut self, series_id: SeriesId, sample: MetricSample) -> bool {
        let buf = self.series.entry(series_id).or_insert_with(|| {
            SeriesBuffer::Metric(MetricBuffer {
                encoder: GorillaEncoder::new(),
                min_ts: sample.timestamp_ms,
                max_ts: sample.timestamp_ms,
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
            // Approximate: 2 bytes per sample (Gorilla overhead).
            self.memory_bytes += 2;
            self.metric_count += 1;
        }

        self.update_oldest(sample.timestamp_ms);
        self.should_flush()
    }

    /// Ingest a log entry. Returns true if memtable should be flushed.
    pub fn ingest_log(&mut self, series_id: SeriesId, entry: LogEntry) -> bool {
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
            self.memory_bytes += entry_size + 16; // entry + overhead
            self.log_count += 1;
        }

        self.update_oldest(ts);
        self.should_flush()
    }

    /// Whether the memtable should be flushed (memory pressure).
    pub fn should_flush(&self) -> bool {
        self.memory_bytes >= self.max_memory_bytes
    }

    /// Drain the memtable, returning all buffered data organized by series.
    ///
    /// After drain, the memtable is empty and ready for new ingest.
    pub fn drain(&mut self) -> Vec<FlushedSeries> {
        let mut result = Vec::with_capacity(self.series.len());

        for (series_id, buf) in self.series.drain() {
            match buf {
                SeriesBuffer::Metric(m) => {
                    let sample_count = m.encoder.count();
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
        self.series.is_empty()
    }

    fn update_oldest(&mut self, ts: i64) {
        match self.oldest_ts {
            None => self.oldest_ts = Some(ts),
            Some(old) if ts < old => self.oldest_ts = Some(ts),
            _ => {}
        }
    }
}

/// Data from a single series after memtable drain.
#[derive(Debug)]
pub struct FlushedSeries {
    pub series_id: SeriesId,
    pub kind: FlushedKind,
    pub min_ts: i64,
    pub max_ts: i64,
}

/// Type-specific flushed data.
#[derive(Debug)]
pub enum FlushedKind {
    Metric {
        /// Gorilla-compressed block.
        gorilla_block: Vec<u8>,
        sample_count: u64,
    },
    Log {
        entries: Vec<LogEntry>,
        total_bytes: usize,
    },
}

/// Time range for queries.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeRange {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    pub fn contains(&self, ts: i64) -> bool {
        ts >= self.start_ms && ts <= self.end_ms
    }
}

/// Read-only index over flushed L1 segments for time-range queries.
///
/// Maps (series_id, time_range) → segment file references.
/// Used by the query path to locate relevant segments without scanning.
#[derive(Debug)]
pub struct SegmentIndex {
    /// series_id → sorted list of (min_ts, max_ts, segment_path).
    entries: HashMap<SeriesId, BTreeMap<i64, SegmentRef>>,
}

#[derive(Debug, Clone)]
pub struct SegmentRef {
    pub path: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub kind: SegmentKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Metric,
    Log,
}

impl SegmentIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Register a flushed segment.
    pub fn add(&mut self, series_id: SeriesId, seg: SegmentRef) {
        self.entries
            .entry(series_id)
            .or_default()
            .insert(seg.min_ts, seg);
    }

    /// Find segments overlapping a time range for a given series.
    pub fn query(&self, series_id: SeriesId, range: &TimeRange) -> Vec<&SegmentRef> {
        let Some(tree) = self.entries.get(&series_id) else {
            return Vec::new();
        };
        tree.values()
            .filter(|seg| seg.max_ts >= range.start_ms && seg.min_ts <= range.end_ms)
            .collect()
    }

    pub fn series_count(&self) -> usize {
        self.entries.len()
    }
}

impl Default for SegmentIndex {
    fn default() -> Self {
        Self::new()
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
        mt.ingest_log(
            3,
            LogEntry {
                timestamp_ms: 300,
                data: b"test".to_vec(),
            },
        );
        assert_eq!(mt.series_count(), 3);
    }

    #[test]
    fn drain_clears_state() {
        let mut mt = TimeseriesMemtable::new(1024 * 1024);
        for i in 0..10 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: i as f64,
                },
            );
        }
        mt.ingest_log(
            2,
            LogEntry {
                timestamp_ms: 500,
                data: b"hello".to_vec(),
            },
        );

        let flushed = mt.drain();
        assert_eq!(flushed.len(), 2);
        assert!(mt.is_empty());
        assert_eq!(mt.metric_count(), 0);
        assert_eq!(mt.log_count(), 0);
        assert_eq!(mt.memory_bytes(), 0);
    }

    #[test]
    fn flush_on_memory_pressure() {
        // 100 bytes budget.
        let mut mt = TimeseriesMemtable::new(100);
        for i in 0..100 {
            let should_flush = mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: 42.0,
                },
            );
            if should_flush {
                assert!(mt.memory_bytes() >= 100);
                return;
            }
        }
        // Should have triggered flush at some point.
        panic!("expected flush trigger");
    }

    #[test]
    fn segment_index_query() {
        let mut idx = SegmentIndex::new();
        idx.add(
            1,
            SegmentRef {
                path: "seg-001.ts".into(),
                min_ts: 0,
                max_ts: 3_600_000,
                kind: SegmentKind::Metric,
            },
        );
        idx.add(
            1,
            SegmentRef {
                path: "seg-002.ts".into(),
                min_ts: 3_600_000,
                max_ts: 7_200_000,
                kind: SegmentKind::Metric,
            },
        );

        // Query spanning both segments.
        let range = TimeRange::new(1_800_000, 5_400_000);
        let segs = idx.query(1, &range);
        assert_eq!(segs.len(), 2);

        // Query only second segment.
        let range = TimeRange::new(4_000_000, 5_000_000);
        let segs = idx.query(1, &range);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].path, "seg-002.ts");

        // Non-existent series.
        assert!(idx.query(999, &range).is_empty());
    }
}
