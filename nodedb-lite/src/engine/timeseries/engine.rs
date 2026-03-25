//! Lite timeseries engine: columnar memtable + Gorilla compression + redb persistence.
//!
//! Reuses shared types from `nodedb-types`: GorillaEncoder/Decoder, SeriesKey,
//! SeriesCatalog, SymbolDictionary, TieredPartitionConfig, PartitionMeta.
//!
//! Architecture:
//! - In-memory columnar memtable (same layout as Origin)
//! - Flush to redb with partition key prefix (`ts:{collection}:{partition_start}:{column}`)
//! - Auto-partitioning biased toward coarser intervals (DAY/WEEK for Lite)
//! - Auto retention (7d default)
//! - Gorilla compression for timestamp and f64 columns

use std::collections::HashMap;

use nodedb_types::gorilla::{GorillaDecoder, GorillaEncoder};
use nodedb_types::timeseries::{
    MetricSample, PartitionMeta, PartitionState, SeriesCatalog, SeriesId, SeriesKey,
    TieredPartitionConfig, TimeRange,
};

/// Lite timeseries engine.
///
/// Not `Send` — owned by a single task. The `NodeDbLite` wrapper handles
/// async bridging.
pub struct TimeseriesEngine {
    /// Per-collection columnar data: `collection → CollectionTs`.
    collections: HashMap<String, CollectionTs>,
    /// Series catalog (shared across all collections for collision safety).
    catalog: SeriesCatalog,
    /// Global config (Lite defaults: 7d retention, 4MB memtable, etc.)
    config: TieredPartitionConfig,
    /// Per-series sync watermark: `series_id → last_synced_lsn`.
    /// Tracks which data has been successfully synced to Origin.
    /// Only data after these watermarks is included in sync pushes.
    sync_watermarks: HashMap<SeriesId, u64>,
    /// Global sync LSN counter (monotonically increasing).
    next_sync_lsn: u64,
}

/// Per-collection timeseries state.
struct CollectionTs {
    /// In-memory columnar buffers (hot data).
    timestamps: Vec<i64>,
    values: Vec<f64>,
    series_ids: Vec<SeriesId>,
    /// Total memory estimate in bytes.
    memory_bytes: usize,
    /// Partition boundaries for flushed data.
    partitions: Vec<FlushedPartition>,
    /// Whether data has been ingested since last sync push.
    dirty: bool,
}

/// A flushed partition stored in redb.
#[derive(Debug, Clone)]
struct FlushedPartition {
    meta: PartitionMeta,
    /// redb key prefix: `ts:{collection}:{start_ms}`.
    key_prefix: String,
}

impl CollectionTs {
    fn new() -> Self {
        Self {
            timestamps: Vec::with_capacity(4096),
            values: Vec::with_capacity(4096),
            series_ids: Vec::with_capacity(4096),
            dirty: false,
            memory_bytes: 0,
            partitions: Vec::new(),
        }
    }

    fn row_count(&self) -> usize {
        self.timestamps.len()
    }
}

impl Default for TimeseriesEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeseriesEngine {
    /// Create with Lite defaults.
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
            catalog: SeriesCatalog::new(),
            config: TieredPartitionConfig::lite_defaults(),
            sync_watermarks: HashMap::new(),
            next_sync_lsn: 1,
        }
    }

    /// Create with custom config.
    pub fn with_config(config: TieredPartitionConfig) -> Self {
        Self {
            collections: HashMap::new(),
            catalog: SeriesCatalog::new(),
            config,
            sync_watermarks: HashMap::new(),
            next_sync_lsn: 1,
        }
    }

    // ─── Ingest ──────────────────────────────────────────────────────

    /// Ingest a metric sample.
    ///
    /// Returns true if the memtable needs flushing (memory pressure).
    pub fn ingest_metric(
        &mut self,
        collection: &str,
        metric_name: &str,
        tags: Vec<(String, String)>,
        sample: MetricSample,
    ) -> bool {
        let key = SeriesKey::new(metric_name, tags);
        let series_id = self.catalog.resolve(&key);

        let coll = self
            .collections
            .entry(collection.to_string())
            .or_insert_with(CollectionTs::new);

        coll.timestamps.push(sample.timestamp_ms);
        coll.values.push(sample.value);
        coll.series_ids.push(series_id);
        coll.memory_bytes += 24; // 8 + 8 + 8 bytes per row
        coll.dirty = true;

        coll.memory_bytes >= self.config.memtable_max_memory_bytes as usize
    }

    /// Ingest a batch of samples for one series.
    pub fn ingest_batch(
        &mut self,
        collection: &str,
        metric_name: &str,
        tags: Vec<(String, String)>,
        samples: &[MetricSample],
    ) -> bool {
        let key = SeriesKey::new(metric_name, tags);
        let series_id = self.catalog.resolve(&key);

        let coll = self
            .collections
            .entry(collection.to_string())
            .or_insert_with(CollectionTs::new);

        for sample in samples {
            coll.timestamps.push(sample.timestamp_ms);
            coll.values.push(sample.value);
            coll.series_ids.push(series_id);
        }
        coll.memory_bytes += samples.len() * 24;
        coll.dirty = true;

        coll.memory_bytes >= self.config.memtable_max_memory_bytes as usize
    }

    // ─── Flush to redb ───────────────────────────────────────────────

    /// Flush a collection's memtable to Gorilla-compressed redb entries.
    ///
    /// Returns the serialized partition entries for redb storage.
    /// The caller is responsible for writing to redb.
    pub fn flush(&mut self, collection: &str) -> Option<FlushResult> {
        let coll = self.collections.get_mut(collection)?;
        if coll.timestamps.is_empty() {
            return None;
        }

        let row_count = coll.row_count();
        let min_ts = *coll.timestamps.iter().min().unwrap_or(&0);
        let max_ts = *coll.timestamps.iter().max().unwrap_or(&0);

        // Gorilla-encode timestamps.
        let mut ts_encoder = GorillaEncoder::new();
        for &ts in &coll.timestamps {
            ts_encoder.encode(ts, 0.0);
        }
        let ts_block = ts_encoder.finish();

        // Gorilla-encode values.
        let mut val_encoder = GorillaEncoder::new();
        for (i, &val) in coll.values.iter().enumerate() {
            val_encoder.encode(i as i64, val);
        }
        let val_block = val_encoder.finish();

        // Series IDs as raw LE bytes.
        let series_block: Vec<u8> = coll
            .series_ids
            .iter()
            .flat_map(|&id| id.to_le_bytes())
            .collect();

        let key_prefix = format!("ts:{collection}:{min_ts}");

        let meta = PartitionMeta {
            min_ts,
            max_ts,
            row_count: row_count as u64,
            size_bytes: (ts_block.len() + val_block.len() + series_block.len()) as u64,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: (max_ts - min_ts) as u64,
            last_flushed_wal_lsn: 0,
            column_stats: std::collections::HashMap::new(),
        };

        let partition = FlushedPartition {
            meta: meta.clone(),
            key_prefix: key_prefix.clone(),
        };
        coll.partitions.push(partition);

        // Clear memtable.
        coll.timestamps.clear();
        coll.values.clear();
        coll.series_ids.clear();
        coll.memory_bytes = 0;

        Some(FlushResult {
            key_prefix,
            ts_block,
            val_block,
            series_block,
            meta,
        })
    }

    // ─── Query ───────────────────────────────────────────────────────

    /// Scan metric samples in a time range.
    ///
    /// Returns (timestamp, value, series_id) triples from both memtable
    /// (hot data) and flushed partitions (cold data).
    pub fn scan(&self, collection: &str, range: &TimeRange) -> Vec<(i64, f64, SeriesId)> {
        let Some(coll) = self.collections.get(collection) else {
            return Vec::new();
        };

        let mut results = Vec::new();

        // Scan memtable (hot data).
        for i in 0..coll.timestamps.len() {
            let ts = coll.timestamps[i];
            if range.contains(ts) {
                results.push((ts, coll.values[i], coll.series_ids[i]));
            }
        }

        // Results from flushed partitions would be read from redb by the caller
        // (the engine doesn't hold redb references). The caller passes decoded
        // partition data via `scan_with_partitions`.

        // Sort by timestamp.
        results.sort_by_key(|(ts, _, _)| *ts);
        results
    }

    /// Aggregate over a time range with time_bucket grouping.
    ///
    /// Returns `(bucket_start, count, sum, min, max)` per bucket.
    pub fn aggregate_by_bucket(
        &self,
        collection: &str,
        range: &TimeRange,
        bucket_ms: i64,
    ) -> Vec<(i64, u64, f64, f64, f64)> {
        let rows = self.scan(collection, range);
        if rows.is_empty() || bucket_ms <= 0 {
            return Vec::new();
        }

        let mut buckets: std::collections::BTreeMap<i64, (u64, f64, f64, f64)> =
            std::collections::BTreeMap::new();

        for (ts, val, _) in &rows {
            let bucket = (*ts / bucket_ms) * bucket_ms;
            let entry = buckets
                .entry(bucket)
                .or_insert((0, 0.0, f64::INFINITY, f64::NEG_INFINITY));
            entry.0 += 1;
            entry.1 += val;
            if *val < entry.2 {
                entry.2 = *val;
            }
            if *val > entry.3 {
                entry.3 = *val;
            }
        }

        buckets
            .into_iter()
            .map(|(bucket, (count, sum, min, max))| (bucket, count, sum, min, max))
            .collect()
    }

    // ─── Retention ───────────────────────────────────────────────────

    /// Drop partitions older than the retention period.
    ///
    /// Returns the key prefixes of dropped partitions (for redb cleanup).
    pub fn apply_retention(&mut self, now_ms: i64) -> Vec<String> {
        if self.config.retention_period_ms == 0 {
            return Vec::new();
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        let mut dropped = Vec::new();

        for coll in self.collections.values_mut() {
            coll.partitions.retain(|p| {
                if p.meta.max_ts < cutoff {
                    dropped.push(p.key_prefix.clone());
                    false
                } else {
                    true
                }
            });
        }
        dropped
    }

    // ─── Accessors ────

    pub fn collection_names(&self) -> Vec<&str> {
        self.collections.keys().map(|s| s.as_str()).collect()
    }

    pub fn row_count(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.row_count())
            .unwrap_or(0)
    }

    pub fn memory_bytes(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.memory_bytes)
            .unwrap_or(0)
    }

    pub fn partition_count(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.partitions.len())
            .unwrap_or(0)
    }

    pub fn catalog(&self) -> &SeriesCatalog {
        &self.catalog
    }

    pub fn config(&self) -> &TieredPartitionConfig {
        &self.config
    }

    /// Decode a flushed timestamp block (Gorilla-encoded).
    pub fn decode_timestamps(block: &[u8]) -> Vec<i64> {
        let mut dec = GorillaDecoder::new(block);
        dec.decode_all().into_iter().map(|(ts, _)| ts).collect()
    }

    /// Decode a flushed value block (Gorilla-encoded).
    pub fn decode_values(block: &[u8]) -> Vec<f64> {
        let mut dec = GorillaDecoder::new(block);
        dec.decode_all().into_iter().map(|(_, v)| v).collect()
    }

    /// Decode a flushed series_id block (raw LE u64).
    ///
    /// Each series ID is 8 bytes (u64 LE). Trailing bytes that don't form
    /// a complete u64 are silently ignored (via `chunks_exact`).
    pub fn decode_series_ids(block: &[u8]) -> Vec<SeriesId> {
        block
            .chunks_exact(8)
            .map(|chunk| {
                // Safety: chunks_exact(8) guarantees exactly 8 bytes.
                let arr: [u8; 8] = chunk
                    .try_into()
                    .expect("chunks_exact(8) guarantees 8 bytes");
                u64::from_le_bytes(arr)
            })
            .collect()
    }

    // ─── Sync Watermarks ──

    /// Get the sync watermark for a series (highest LSN synced to Origin).
    pub fn sync_watermark(&self, series_id: SeriesId) -> u64 {
        self.sync_watermarks.get(&series_id).copied().unwrap_or(0)
    }

    /// Update sync watermark after Origin acknowledges a push.
    ///
    /// Stores the max timestamp per series that has been synced.
    /// Called when `TimeseriesAckMsg` is received.
    pub fn acknowledge_sync(&mut self, collection: &str, max_synced_ts: u64) {
        if let Some(coll) = self.collections.get_mut(collection) {
            // Find the actual max timestamp per series that was synced.
            for i in 0..coll.series_ids.len() {
                let sid = coll.series_ids[i];
                let ts = coll.timestamps[i] as u64;
                if ts <= max_synced_ts {
                    self.sync_watermarks
                        .entry(sid)
                        .and_modify(|w| *w = (*w).max(ts))
                        .or_insert(ts);
                }
            }
            coll.dirty = false;
        }
    }

    /// Get all current sync watermarks (for persistence to redb).
    pub fn export_watermarks(&self) -> &HashMap<SeriesId, u64> {
        &self.sync_watermarks
    }

    /// Import watermarks from redb (cold start restore).
    pub fn import_watermarks(&mut self, watermarks: HashMap<SeriesId, u64>) {
        self.sync_watermarks = watermarks;
    }

    /// Get collections that have unsynced data.
    pub fn dirty_collections(&self) -> Vec<&str> {
        self.collections
            .iter()
            .filter(|(_, c)| c.dirty || !c.timestamps.is_empty())
            .map(|(name, _)| name.as_str())
            .collect()
    }

    // ─── Lifecycle: Retention vs Sync ──

    /// Apply retention with sync-awareness.
    ///
    /// **Default behavior** (`retain_until_synced = false`): retention drops
    /// partitions by age regardless of sync status. If the device was offline
    /// longer than the retention period, data is lost before it syncs.
    ///
    /// **Guarded behavior** (`retain_until_synced = true`): partitions with
    /// unsynced data are kept past the retention period. This prevents data
    /// loss but may cause unbounded local storage growth.
    ///
    /// Returns `(dropped_keys, warnings)` where warnings are
    /// `TimeseriesDataDroppedBeforeSync` events for the application.
    pub fn apply_retention_with_sync(
        &mut self,
        now_ms: i64,
    ) -> (Vec<String>, Vec<UnsyncedDropWarning>) {
        if self.config.retention_period_ms == 0 {
            return (Vec::new(), Vec::new());
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        let retain_until_synced = self.config.retain_until_synced;
        let mut dropped = Vec::new();
        let mut warnings = Vec::new();

        for (collection, coll) in &mut self.collections {
            coll.partitions.retain(|p| {
                if p.meta.max_ts >= cutoff {
                    return true; // Not expired yet.
                }

                // Check if this partition has unsynced data.
                let has_unsynced = p.meta.last_flushed_wal_lsn == 0; // Simplified: LSN 0 = never synced.

                if has_unsynced && retain_until_synced {
                    // Keep: guarded mode prevents data loss.
                    return true;
                }

                if has_unsynced {
                    // Dropping unsynced data — emit warning.
                    warnings.push(UnsyncedDropWarning {
                        collection: collection.clone(),
                        partition_key: p.key_prefix.clone(),
                        min_ts: p.meta.min_ts,
                        max_ts: p.meta.max_ts,
                        row_count: p.meta.row_count,
                    });
                }

                dropped.push(p.key_prefix.clone());
                false
            });
        }

        (dropped, warnings)
    }

    /// Assign a sync LSN to data and advance the counter.
    pub fn assign_sync_lsn(&mut self) -> u64 {
        let lsn = self.next_sync_lsn;
        self.next_sync_lsn += 1;
        lsn
    }

    // ─── Sync Push Payload ───────────────────────────────────────────

    /// Build a sync push payload for a collection.
    ///
    /// If `sync_resolution_ms > 0`, downsamples to rollup buckets instead
    /// of sending raw samples. This reduces bandwidth for mobile agents.
    ///
    /// Only includes data after the per-series sync watermark (delta sync).
    pub fn build_sync_payload(
        &mut self,
        collection: &str,
        lite_id: &str,
    ) -> Option<nodedb_types::sync::wire::TimeseriesPushMsg> {
        let coll = self.collections.get(collection)?;
        if coll.timestamps.is_empty() {
            return None;
        }

        let sync_resolution = self.config.sync_resolution_ms;

        // Filter to only unsynced data.
        // Watermark stores the max timestamp that's been synced per series.
        // Only include samples with timestamp > watermark for that series.
        let mut ts_to_sync = Vec::new();
        let mut val_to_sync = Vec::new();
        for i in 0..coll.timestamps.len() {
            let sid = coll.series_ids[i];
            let ts = coll.timestamps[i];
            let last_synced_ts = self.sync_watermark(sid) as i64;
            if ts > last_synced_ts {
                ts_to_sync.push(ts);
                val_to_sync.push(coll.values[i]);
            }
        }

        if ts_to_sync.is_empty() {
            return None;
        }

        // Apply pre-sync downsampling if configured.
        let (final_ts, final_vals) = if sync_resolution > 0 {
            let mut buckets: std::collections::BTreeMap<i64, (f64, u64)> =
                std::collections::BTreeMap::new();
            for i in 0..ts_to_sync.len() {
                let bucket = (ts_to_sync[i] / sync_resolution as i64) * sync_resolution as i64;
                let entry = buckets.entry(bucket).or_insert((0.0, 0));
                entry.0 += val_to_sync[i];
                entry.1 += 1;
            }
            let ts: Vec<i64> = buckets.keys().copied().collect();
            let vals: Vec<f64> = buckets
                .values()
                .map(|(sum, count)| sum / *count as f64)
                .collect();
            (ts, vals)
        } else {
            (ts_to_sync, val_to_sync)
        };

        // Gorilla-encode for wire.
        let mut ts_enc = nodedb_types::GorillaEncoder::new();
        for &t in &final_ts {
            ts_enc.encode(t, 0.0);
        }
        let mut val_enc = nodedb_types::GorillaEncoder::new();
        for (i, &v) in final_vals.iter().enumerate() {
            val_enc.encode(i as i64, v);
        }

        let min_ts = final_ts.iter().copied().min().unwrap_or(0);
        let max_ts = final_ts.iter().copied().max().unwrap_or(0);

        // Build watermarks map for the push.
        let watermarks: HashMap<u64, u64> =
            self.sync_watermarks.iter().map(|(&k, &v)| (k, v)).collect();

        Some(nodedb_types::sync::wire::TimeseriesPushMsg {
            lite_id: lite_id.to_string(),
            collection: collection.to_string(),
            ts_block: ts_enc.finish(),
            val_block: val_enc.finish(),
            series_block: Vec::new(), // Simplified: series IDs not needed for rollups.
            sample_count: final_ts.len() as u64,
            min_ts,
            max_ts,
            watermarks,
        })
    }

    /// Get the configured sync interval in milliseconds.
    pub fn sync_interval_ms(&self) -> u64 {
        self.config.sync_interval_ms
    }
}

/// Warning emitted when retention drops unsynced data.
///
/// The application can surface this to the user or adjust retention.
#[derive(Debug, Clone)]
pub struct UnsyncedDropWarning {
    /// Collection name.
    pub collection: String,
    /// redb key prefix of the dropped partition.
    pub partition_key: String,
    /// Oldest timestamp in the dropped data.
    pub min_ts: i64,
    /// Newest timestamp in the dropped data.
    pub max_ts: i64,
    /// Number of rows lost.
    pub row_count: u64,
}

/// A key-value entry for redb persistence.
pub type RedbEntry = (Vec<u8>, Vec<u8>);

/// Result of flushing a collection's memtable.
pub struct FlushResult {
    /// redb key prefix for this partition.
    pub key_prefix: String,
    /// Gorilla-encoded timestamps.
    pub ts_block: Vec<u8>,
    /// Gorilla-encoded values.
    pub val_block: Vec<u8>,
    /// Raw LE u64 series IDs.
    pub series_block: Vec<u8>,
    /// Partition metadata.
    pub meta: PartitionMeta,
}

impl FlushResult {
    /// redb key-value pairs to persist this partition.
    ///
    /// Returns `Err` if metadata serialization fails.
    pub fn to_redb_entries(&self) -> Result<Vec<RedbEntry>, serde_json::Error> {
        let meta_bytes = serde_json::to_vec(&self.meta)?;
        Ok(vec![
            (
                format!("{}:ts", self.key_prefix).into_bytes(),
                self.ts_block.clone(),
            ),
            (
                format!("{}:val", self.key_prefix).into_bytes(),
                self.val_block.clone(),
            ),
            (
                format!("{}:series", self.key_prefix).into_bytes(),
                self.series_block.clone(),
            ),
            (format!("{}:meta", self.key_prefix).into_bytes(), meta_bytes),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_and_scan() {
        let mut engine = TimeseriesEngine::new();

        for i in 0..100 {
            engine.ingest_metric(
                "metrics",
                "cpu_usage",
                vec![("host".into(), "prod-1".into())],
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: 50.0 + (i as f64) * 0.1,
                },
            );
        }

        assert_eq!(engine.row_count("metrics"), 100);

        let results = engine.scan("metrics", &TimeRange::new(1000, 1099));
        assert_eq!(results.len(), 100);
        assert_eq!(results[0].0, 1000); // sorted by timestamp
        assert_eq!(results[99].0, 1099);
    }

    #[test]
    fn aggregate_by_bucket() {
        let mut engine = TimeseriesEngine::new();

        for i in 0..100 {
            engine.ingest_metric(
                "metrics",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 10,
                    value: i as f64,
                },
            );
        }

        // 10ms bucket → 100 rows / 10ms per bucket = ~1 row per bucket at 10ms interval
        let buckets = engine.aggregate_by_bucket("metrics", &TimeRange::new(0, 999), 100);
        assert_eq!(buckets.len(), 10); // 0, 100, 200, ..., 900
        assert_eq!(buckets[0].1, 10); // 10 rows per 100ms bucket
    }

    #[test]
    fn flush_and_decode() {
        let mut engine = TimeseriesEngine::new();

        for i in 0..50 {
            engine.ingest_metric(
                "metrics",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: 5000 + i * 100,
                    value: (i as f64) * 0.5,
                },
            );
        }

        let flush = engine.flush("metrics").unwrap();
        assert_eq!(flush.meta.row_count, 50);
        assert_eq!(flush.meta.min_ts, 5000);

        // Decode and verify roundtrip.
        let decoded_ts = TimeseriesEngine::decode_timestamps(&flush.ts_block);
        assert_eq!(decoded_ts.len(), 50);
        assert_eq!(decoded_ts[0], 5000);
        assert_eq!(decoded_ts[49], 5000 + 49 * 100);

        let decoded_vals = TimeseriesEngine::decode_values(&flush.val_block);
        assert_eq!(decoded_vals.len(), 50);
        assert!((decoded_vals[0] - 0.0).abs() < f64::EPSILON);

        // Memtable should be empty after flush.
        assert_eq!(engine.row_count("metrics"), 0);
        assert_eq!(engine.partition_count("metrics"), 1);
    }

    #[test]
    fn retention() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            retention_period_ms: 1000, // 1 second
            ..TieredPartitionConfig::lite_defaults()
        });

        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("metrics");

        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 2000,
                value: 2.0,
            },
        );
        engine.flush("metrics");

        assert_eq!(engine.partition_count("metrics"), 2);

        // Apply retention at t=2500 → partition at 100 (max_ts=100) is older than 1000ms ago.
        let dropped = engine.apply_retention(2500);
        assert_eq!(dropped.len(), 1);
        assert_eq!(engine.partition_count("metrics"), 1);
    }

    #[test]
    fn series_catalog_integration() {
        let mut engine = TimeseriesEngine::new();

        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![("host".into(), "a".into())],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![("host".into(), "b".into())],
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        engine.ingest_metric(
            "metrics",
            "mem",
            vec![("host".into(), "a".into())],
            MetricSample {
                timestamp_ms: 300,
                value: 3.0,
            },
        );

        // 3 distinct series in the catalog.
        assert_eq!(engine.catalog().len(), 3);
    }

    #[test]
    fn flush_result_redb_entries() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 1000,
                value: 42.0,
            },
        );
        let flush = engine.flush("m").unwrap();
        let entries = flush.to_redb_entries().unwrap();
        assert_eq!(entries.len(), 4); // ts, val, series, meta
        assert!(entries[0].0.starts_with(b"ts:m:1000:ts"));
    }

    #[test]
    fn empty_scan() {
        let engine = TimeseriesEngine::new();
        assert!(
            engine
                .scan("nonexistent", &TimeRange::new(0, 1000))
                .is_empty()
        );
    }

    #[test]
    fn batch_ingest() {
        let mut engine = TimeseriesEngine::new();
        let samples: Vec<MetricSample> = (0..1000)
            .map(|i| MetricSample {
                timestamp_ms: i * 10,
                value: i as f64,
            })
            .collect();
        engine.ingest_batch("metrics", "cpu", vec![], &samples);
        assert_eq!(engine.row_count("metrics"), 1000);
    }

    #[test]
    fn batch_ingest_sets_dirty() {
        let mut engine = TimeseriesEngine::new();
        assert!(engine.dirty_collections().is_empty());

        let samples = vec![MetricSample {
            timestamp_ms: 100,
            value: 1.0,
        }];
        engine.ingest_batch("metrics", "cpu", vec![], &samples);
        assert_eq!(engine.dirty_collections(), vec!["metrics"]);
    }

    #[test]
    fn sync_watermark_defaults_to_zero() {
        let engine = TimeseriesEngine::new();
        assert_eq!(engine.sync_watermark(42), 0);
    }

    #[test]
    fn acknowledge_sync_updates_watermarks() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![("h".into(), "a".into())],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.ingest_metric(
            "m",
            "cpu",
            vec![("h".into(), "a".into())],
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );

        let sid = engine
            .catalog
            .resolve(&SeriesKey::new("cpu", vec![("h".into(), "a".into())]));
        assert_eq!(engine.sync_watermark(sid), 0);

        // Acknowledge sync up to timestamp 100.
        engine.acknowledge_sync("m", 100);
        assert_eq!(engine.sync_watermark(sid), 100);

        // Acknowledge sync up to timestamp 200 — advances.
        engine.acknowledge_sync("m", 200);
        assert_eq!(engine.sync_watermark(sid), 200);

        // Lower timestamp does NOT regress (monotonic).
        engine.acknowledge_sync("m", 50);
        assert_eq!(engine.sync_watermark(sid), 200);
    }

    #[test]
    fn acknowledge_sync_clears_dirty() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        assert!(!engine.dirty_collections().is_empty());

        engine.acknowledge_sync("m", 100);
        // Dirty cleared, but memtable still has data so dirty_collections includes it.
        // After flush + ack, it should be clean.
        engine.flush("m");
        engine.acknowledge_sync("m", 100);
        assert!(engine.dirty_collections().is_empty());
    }

    #[test]
    fn export_import_watermarks() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.acknowledge_sync("m", 100);

        let exported = engine.export_watermarks().clone();
        assert!(!exported.is_empty());

        // Cold restart: new engine imports watermarks.
        let mut engine2 = TimeseriesEngine::new();
        engine2.import_watermarks(exported.clone());
        assert_eq!(engine2.export_watermarks(), &exported);
    }

    #[test]
    fn assign_sync_lsn_monotonic() {
        let mut engine = TimeseriesEngine::new();
        let lsn1 = engine.assign_sync_lsn();
        let lsn2 = engine.assign_sync_lsn();
        let lsn3 = engine.assign_sync_lsn();
        assert!(lsn1 < lsn2);
        assert!(lsn2 < lsn3);
    }

    #[test]
    fn retention_with_sync_default_drops_unsynced() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            retention_period_ms: 1000,
            retain_until_synced: false,
            ..TieredPartitionConfig::lite_defaults()
        });

        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("m"); // Partition with max_ts=100, never synced (LSN=0).

        let (dropped, warnings) = engine.apply_retention_with_sync(5000);
        assert_eq!(dropped.len(), 1);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].collection, "m");
        assert_eq!(warnings[0].row_count, 1);
    }

    #[test]
    fn retention_with_sync_guarded_retains_unsynced() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            retention_period_ms: 1000,
            retain_until_synced: true,
            ..TieredPartitionConfig::lite_defaults()
        });

        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("m");

        // Guarded mode: keep unsynced data past retention.
        let (dropped, warnings) = engine.apply_retention_with_sync(5000);
        assert!(dropped.is_empty());
        assert!(warnings.is_empty());
        assert_eq!(engine.partition_count("m"), 1); // Still retained.
    }
}
