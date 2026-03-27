//! Continuous aggregation for Lite timeseries: automatic rollups on flush.
//!
//! When a timeseries collection flushes its memtable, registered continuous
//! aggregates compute bucketed rollups (sum, count, min, max, avg) and store
//! the results in-memory for O(1) query access.
//!
//! Simpler than Origin's columnar-based continuous agg: Lite processes raw
//! `(timestamp, value)` pairs from the flush buffer directly.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Definition of a continuous aggregate for Lite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousAggDef {
    /// Name of this aggregate (e.g., "cpu_1m").
    pub name: String,
    /// Source collection to read from.
    pub source: String,
    /// Bucket interval in milliseconds (e.g., 60_000 for 1 minute).
    pub bucket_interval_ms: i64,
    /// Retention period in ms (0 = keep forever).
    pub retention_ms: u64,
}

/// Materialized bucket: pre-computed aggregates for one time bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedBucket {
    pub bucket_ts: i64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl MaterializedBucket {
    fn new(bucket_ts: i64) -> Self {
        Self {
            bucket_ts,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn ingest(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    /// Average value in this bucket. Returns 0.0 if empty.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

/// Lite continuous aggregate manager.
///
/// Processes `(timestamp, value)` flush buffers and maintains materialized
/// bucketed aggregates in memory.
pub struct LiteContinuousAggManager {
    definitions: HashMap<String, ContinuousAggDef>,
    /// `agg_name → bucket_ts → MaterializedBucket`.
    materialized: HashMap<String, HashMap<i64, MaterializedBucket>>,
    /// `source_collection → [agg_names]`.
    dependencies: HashMap<String, Vec<String>>,
}

impl LiteContinuousAggManager {
    pub fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            materialized: HashMap::new(),
            dependencies: HashMap::new(),
        }
    }

    /// Register a continuous aggregate definition.
    pub fn register(&mut self, def: ContinuousAggDef) {
        let source = def.source.clone();
        let name = def.name.clone();
        self.materialized.entry(name.clone()).or_default();
        self.dependencies
            .entry(source)
            .or_default()
            .push(name.clone());
        self.definitions.insert(name, def);
    }

    /// Remove a continuous aggregate.
    pub fn unregister(&mut self, name: &str) {
        if let Some(def) = self.definitions.remove(name) {
            self.materialized.remove(name);
            if let Some(deps) = self.dependencies.get_mut(&def.source) {
                deps.retain(|n| n != name);
            }
        }
    }

    /// Process a flush event: compute bucketed aggregates from raw data.
    ///
    /// Called after `TimeseriesEngine::flush()` with the flushed timestamps
    /// and values. Incrementally updates materialized buckets.
    ///
    /// Returns names of aggregates that were refreshed.
    pub fn on_flush(
        &mut self,
        source_collection: &str,
        timestamps: &[i64],
        values: &[f64],
    ) -> Vec<String> {
        let agg_names: Vec<String> = self
            .dependencies
            .get(source_collection)
            .cloned()
            .unwrap_or_default();

        let mut refreshed = Vec::new();

        for agg_name in &agg_names {
            let Some(def) = self.definitions.get(agg_name) else {
                continue;
            };
            let interval = def.bucket_interval_ms;
            if interval <= 0 {
                continue;
            }

            let mat = self.materialized.entry(agg_name.clone()).or_default();

            debug_assert_eq!(
                timestamps.len(),
                values.len(),
                "continuous agg: timestamp/value array length mismatch"
            );
            for (i, &ts) in timestamps.iter().enumerate() {
                let Some(&value) = values.get(i) else {
                    break; // Mismatched lengths — stop rather than corrupt.
                };
                let bucket_ts = ts - (ts.rem_euclid(interval));
                let bucket = mat
                    .entry(bucket_ts)
                    .or_insert_with(|| MaterializedBucket::new(bucket_ts));
                bucket.ingest(value);
            }

            refreshed.push(agg_name.clone());
        }

        refreshed
    }

    /// Query materialized buckets for an aggregate within a time range.
    pub fn query(&self, agg_name: &str, start_ms: i64, end_ms: i64) -> Vec<&MaterializedBucket> {
        let Some(mat) = self.materialized.get(agg_name) else {
            return Vec::new();
        };
        let mut results: Vec<&MaterializedBucket> = mat
            .values()
            .filter(|b| b.bucket_ts >= start_ms && b.bucket_ts <= end_ms)
            .collect();
        results.sort_by_key(|b| b.bucket_ts);
        results
    }

    /// Get all materialized buckets for an aggregate, sorted by time.
    pub fn query_all(&self, agg_name: &str) -> Vec<&MaterializedBucket> {
        let Some(mat) = self.materialized.get(agg_name) else {
            return Vec::new();
        };
        let mut results: Vec<&MaterializedBucket> = mat.values().collect();
        results.sort_by_key(|b| b.bucket_ts);
        results
    }

    /// Apply retention: remove buckets older than each aggregate's retention period.
    pub fn apply_retention(&mut self, now_ms: i64) -> usize {
        let mut total = 0;
        let defs: Vec<(String, u64)> = self
            .definitions
            .values()
            .map(|d| (d.name.clone(), d.retention_ms))
            .collect();
        for (name, retention) in defs {
            if retention == 0 {
                continue;
            }
            let cutoff = now_ms - retention as i64;
            if let Some(mat) = self.materialized.get_mut(&name) {
                let before = mat.len();
                mat.retain(|_, b| b.bucket_ts > cutoff);
                total += before - mat.len();
            }
        }
        total
    }

    /// Number of registered aggregates.
    pub fn count(&self) -> usize {
        self.definitions.len()
    }

    /// List all registered aggregate names.
    pub fn list(&self) -> Vec<&str> {
        self.definitions.keys().map(String::as_str).collect()
    }
}

impl Default for LiteContinuousAggManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_def(name: &str, source: &str, interval_ms: i64) -> ContinuousAggDef {
        ContinuousAggDef {
            name: name.into(),
            source: source.into(),
            bucket_interval_ms: interval_ms,
            retention_ms: 0,
        }
    }

    #[test]
    fn register_and_flush() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));

        // Use a base timestamp aligned to 60s boundary for predictable buckets.
        let base = 1_700_000_040_000_i64; // already aligned to 60s
        let ts: Vec<i64> = (0..120).map(|i| base + i * 1000).collect();
        let vals: Vec<f64> = (0..120).map(|i| 50.0 + i as f64).collect();

        let refreshed = mgr.on_flush("cpu", &ts, &vals);
        assert_eq!(refreshed, vec!["cpu_1m"]);

        let results = mgr.query_all("cpu_1m");
        assert_eq!(results.len(), 2); // 120s / 60s = 2 buckets (aligned)
        let total: u64 = results.iter().map(|b| b.count).sum();
        assert_eq!(total, 120);
    }

    #[test]
    fn incremental_accumulation() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));

        // Use aligned base so all 60 samples fall in one bucket.
        let base = 1_700_000_000_000 - (1_700_000_000_000 % 60_000);
        let ts1: Vec<i64> = (0..60).map(|i| base + i * 1000).collect();
        let vals1: Vec<f64> = (0..60).map(|i| i as f64).collect();
        mgr.on_flush("cpu", &ts1, &vals1);

        // Second flush: same samples again.
        mgr.on_flush("cpu", &ts1, &vals1);

        let results = mgr.query_all("cpu_1m");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].count, 120); // Accumulated.
    }

    #[test]
    fn time_range_query() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));

        let ts: Vec<i64> = (0..300).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let vals: Vec<f64> = vec![1.0; 300];
        mgr.on_flush("cpu", &ts, &vals);

        let start = 1_700_000_060_000;
        let end = 1_700_000_180_000;
        let results = mgr.query("cpu_1m", start, end);
        assert_eq!(results.len(), 2); // Buckets at 60k and 120k.
    }

    #[test]
    fn retention() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(ContinuousAggDef {
            name: "cpu_1m".into(),
            source: "cpu".into(),
            bucket_interval_ms: 60_000,
            retention_ms: 120_000, // 2 minutes
        });

        let ts: Vec<i64> = (0..300).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let vals: Vec<f64> = vec![1.0; 300];
        mgr.on_flush("cpu", &ts, &vals);

        let before = mgr.query_all("cpu_1m").len();
        let now = 1_700_000_000_000 + 5 * 60_000; // 5 minutes later
        let removed = mgr.apply_retention(now);
        assert!(removed > 0);
        assert!(mgr.query_all("cpu_1m").len() < before);
    }

    #[test]
    fn min_max_avg() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("m_1m", "m", 60_000));

        let ts = vec![1_700_000_000_000i64; 4];
        let vals = vec![10.0, 20.0, 30.0, 40.0];
        mgr.on_flush("m", &ts, &vals);

        let results = mgr.query_all("m_1m");
        assert_eq!(results.len(), 1);
        let b = &results[0];
        assert_eq!(b.min, 10.0);
        assert_eq!(b.max, 40.0);
        assert_eq!(b.sum, 100.0);
        assert_eq!(b.count, 4);
        assert!((b.avg() - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn unregister_removes() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));
        assert_eq!(mgr.count(), 1);
        mgr.unregister("cpu_1m");
        assert_eq!(mgr.count(), 0);
    }

    #[test]
    fn no_source_match_no_refresh() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));

        let refreshed = mgr.on_flush("memory", &[1_700_000_000_000], &[50.0]);
        assert!(refreshed.is_empty());
    }
}
