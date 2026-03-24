//! Partition registry — tracks all partitions for a timeseries collection.
//!
//! Maintains a `BTreeMap<i64, PartitionMeta>` keyed by partition start
//! timestamp. Supports O(log P) pruning for time-range queries, partition
//! lifecycle management (create/seal/merge/delete), and the adaptive
//! interval algorithm for AUTO mode.
//!
//! The registry is the partition manifest: all state transitions are
//! recorded here. On crash recovery, replay the manifest to reach a
//! consistent state.

use std::collections::BTreeMap;

use nodedb_types::timeseries::{
    PartitionInterval, PartitionMeta, PartitionState, TieredPartitionConfig, TimeRange,
};

// ---------------------------------------------------------------------------
// Rate estimator
// ---------------------------------------------------------------------------

/// Exponentially weighted moving average rate estimator.
#[derive(Debug, Clone)]
pub struct RateEstimator {
    /// Current estimated rows per second.
    rate: f64,
    /// Smoothing factor (0..1). Higher = more weight to recent samples.
    alpha: f64,
    /// Last update timestamp (ms).
    last_update_ms: i64,
    /// Rows accumulated since last update.
    rows_since_update: u64,
}

impl RateEstimator {
    pub fn new() -> Self {
        Self {
            rate: 0.0,
            alpha: 0.1,
            last_update_ms: 0,
            rows_since_update: 0,
        }
    }

    /// Record `n` rows ingested at timestamp `now_ms`.
    pub fn record(&mut self, n: u64, now_ms: i64) {
        if self.last_update_ms == 0 {
            self.last_update_ms = now_ms;
            self.rows_since_update = n;
            return;
        }

        self.rows_since_update += n;
        let elapsed_ms = now_ms - self.last_update_ms;

        // Update every second.
        if elapsed_ms >= 1000 {
            let elapsed_s = elapsed_ms as f64 / 1000.0;
            let instant_rate = self.rows_since_update as f64 / elapsed_s;
            self.rate = self.alpha * instant_rate + (1.0 - self.alpha) * self.rate;
            self.last_update_ms = now_ms;
            self.rows_since_update = 0;
        }
    }

    /// Current estimated rows/second.
    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// Select an interval based on current rate.
    pub fn suggest_interval(&self) -> PartitionInterval {
        let r = self.rate;
        if r > 100_000.0 {
            PartitionInterval::Duration(3_600_000) // 1h
        } else if r > 1_000.0 {
            PartitionInterval::Duration(86_400_000) // 1d
        } else if r > 10.0 {
            PartitionInterval::Duration(604_800_000) // 1w
        } else if r > 0.1 {
            PartitionInterval::Month
        } else {
            PartitionInterval::Unbounded
        }
    }
}

impl Default for RateEstimator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Partition registry
// ---------------------------------------------------------------------------

/// Registry of all partitions for one timeseries collection.
pub struct PartitionRegistry {
    /// Partitions keyed by start timestamp.
    partitions: BTreeMap<i64, PartitionEntry>,
    /// Current effective interval (may differ from config if AUTO + adapted).
    current_interval: PartitionInterval,
    /// Rate estimator for AUTO mode.
    rate_estimator: RateEstimator,
    /// Collection config.
    config: TieredPartitionConfig,
}

/// A partition entry in the registry.
#[derive(Debug, Clone)]
pub struct PartitionEntry {
    pub meta: PartitionMeta,
    /// Directory name for this partition.
    pub dir_name: String,
}

impl PartitionRegistry {
    pub fn new(config: TieredPartitionConfig) -> Self {
        let current_interval = match &config.partition_by {
            PartitionInterval::Auto => PartitionInterval::Duration(86_400_000), // start at 1d
            other => other.clone(),
        };
        Self {
            partitions: BTreeMap::new(),
            current_interval,
            rate_estimator: RateEstimator::new(),
            config,
        }
    }

    /// Get or create a partition for a given timestamp.
    ///
    /// Returns the partition directory name and whether it's new.
    pub fn get_or_create_partition(&mut self, timestamp_ms: i64) -> (&PartitionEntry, bool) {
        let (start, end) = self.partition_boundaries(timestamp_ms);

        if self.partitions.contains_key(&start) {
            return (self.partitions.get(&start).unwrap(), false);
        }

        let dir_name = format_partition_dir(start, end);
        let entry = PartitionEntry {
            meta: PartitionMeta {
                min_ts: timestamp_ms,
                max_ts: timestamp_ms,
                row_count: 0,
                size_bytes: 0,
                schema_version: 1,
                state: PartitionState::Active,
                interval_ms: self.current_interval.as_millis().unwrap_or(0),
                last_flushed_wal_lsn: 0,
            },
            dir_name,
        };
        self.partitions.insert(start, entry);
        (self.partitions.get(&start).unwrap(), true)
    }

    /// Compute partition boundaries (start_ms, end_ms) for a given timestamp.
    fn partition_boundaries(&self, timestamp_ms: i64) -> (i64, i64) {
        match &self.current_interval {
            PartitionInterval::Duration(ms) => {
                let ms = *ms as i64;
                let start = (timestamp_ms / ms) * ms;
                (start, start + ms)
            }
            PartitionInterval::Month => {
                // Align to calendar month start/end (simplified: 30d).
                let approx_month_ms = 30 * 86_400_000i64;
                let start = (timestamp_ms / approx_month_ms) * approx_month_ms;
                (start, start + approx_month_ms)
            }
            PartitionInterval::Year => {
                let approx_year_ms = 365 * 86_400_000i64;
                let start = (timestamp_ms / approx_year_ms) * approx_year_ms;
                (start, start + approx_year_ms)
            }
            PartitionInterval::Unbounded => {
                // Single partition: start=0, end=MAX.
                (0, i64::MAX)
            }
            PartitionInterval::Auto => {
                // Should have been resolved to a concrete interval in new().
                let ms = 86_400_000i64; // fallback 1d
                let start = (timestamp_ms / ms) * ms;
                (start, start + ms)
            }
        }
    }

    /// Record ingest for adaptive interval algorithm.
    pub fn record_ingest(&mut self, row_count: u64, now_ms: i64) {
        self.rate_estimator.record(row_count, now_ms);
    }

    /// Seal a partition (mark immutable).
    pub fn seal_partition(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts)
            && entry.meta.state == PartitionState::Active
        {
            entry.meta.state = PartitionState::Sealed;

            // AUTO mode: check size floor after sealing.
            if matches!(self.config.partition_by, PartitionInterval::Auto)
                && entry.meta.row_count < 1000
            {
                self.widen_interval();
            }
            return true;
        }
        false
    }

    /// Double the current interval (AUTO mode size floor promotion).
    fn widen_interval(&mut self) {
        self.current_interval = match &self.current_interval {
            PartitionInterval::Duration(ms) => {
                let doubled = ms * 2;
                if doubled >= 30 * 86_400_000 {
                    PartitionInterval::Month
                } else {
                    PartitionInterval::Duration(doubled)
                }
            }
            PartitionInterval::Month => PartitionInterval::Year,
            PartitionInterval::Year | PartitionInterval::Unbounded => PartitionInterval::Unbounded,
            other => other.clone(),
        };
    }

    /// Check if AUTO mode should narrow the interval (rate increased).
    pub fn maybe_narrow_interval(&mut self) {
        if !matches!(self.config.partition_by, PartitionInterval::Auto) {
            return;
        }
        let suggested = self.rate_estimator.suggest_interval();
        if let (Some(suggested_ms), Some(current_ms)) =
            (suggested.as_millis(), self.current_interval.as_millis())
            && suggested_ms < current_ms
        {
            self.current_interval = suggested;
        }
    }

    /// Find partitions that overlap a time range (for queries).
    pub fn query_partitions(&self, range: &TimeRange) -> Vec<&PartitionEntry> {
        self.partitions
            .values()
            .filter(|e| e.meta.is_queryable() && e.meta.overlaps(range))
            .collect()
    }

    /// Find partitions eligible for merging.
    ///
    /// Returns groups of `merge_count` consecutive sealed partitions
    /// that are all older than `merge_after` relative to `now_ms`.
    pub fn find_mergeable(&self, now_ms: i64) -> Vec<Vec<i64>> {
        let merge_after = self.config.merge_after_ms as i64;
        let merge_count = self.config.merge_count as usize;

        let sealed: Vec<i64> = self
            .partitions
            .iter()
            .filter(|(_, e)| {
                e.meta.state == PartitionState::Sealed && (now_ms - e.meta.max_ts) > merge_after
            })
            .map(|(&start, _)| start)
            .collect();

        let mut groups = Vec::new();
        let mut i = 0;
        while i + merge_count <= sealed.len() {
            groups.push(sealed[i..i + merge_count].to_vec());
            i += merge_count;
        }
        groups
    }

    /// Find partitions eligible for retention drop.
    pub fn find_expired(&self, now_ms: i64) -> Vec<i64> {
        if self.config.retention_period_ms == 0 {
            return Vec::new();
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        self.partitions
            .iter()
            .filter(|(_, e)| e.meta.max_ts < cutoff && e.meta.state != PartitionState::Deleted)
            .map(|(&start, _)| start)
            .collect()
    }

    /// Mark a partition as deleted.
    pub fn mark_deleted(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts) {
            entry.meta.state = PartitionState::Deleted;
            true
        } else {
            false
        }
    }

    /// Mark a partition as merging.
    pub fn mark_merging(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts)
            && entry.meta.state == PartitionState::Sealed
        {
            entry.meta.state = PartitionState::Merging;
            return true;
        }
        false
    }

    /// Insert a merged partition and mark sources as deleted.
    pub fn commit_merge(
        &mut self,
        merged_meta: PartitionMeta,
        merged_dir: String,
        source_starts: &[i64],
    ) {
        // Mark sources as deleted first (before inserting merged, in case
        // the merged partition's start_ts overlaps a source key).
        for &src in source_starts {
            self.mark_deleted(src);
        }
        // Insert (or overwrite) the merged partition.
        let start_ts = merged_meta.min_ts;
        self.partitions.insert(
            start_ts,
            PartitionEntry {
                meta: merged_meta,
                dir_name: merged_dir,
            },
        );
    }

    /// Remove deleted partitions from the registry (after physical cleanup).
    pub fn purge_deleted(&mut self) -> Vec<String> {
        let deleted: Vec<(i64, String)> = self
            .partitions
            .iter()
            .filter(|(_, e)| e.meta.state == PartitionState::Deleted)
            .map(|(&start, e)| (start, e.dir_name.clone()))
            .collect();

        let mut dirs = Vec::new();
        for (start, dir) in deleted {
            self.partitions.remove(&start);
            dirs.push(dir);
        }
        dirs
    }

    /// Update a partition's metadata (e.g., after flush updates row_count).
    pub fn update_meta(&mut self, start_ts: i64, meta: PartitionMeta) {
        if let Some(entry) = self.partitions.get_mut(&start_ts) {
            entry.meta = meta;
        }
    }

    /// Change the partition interval (online ALTER).
    /// Only affects new partitions — existing partitions keep their width.
    pub fn set_partition_interval(&mut self, interval: PartitionInterval) {
        self.current_interval = interval.clone();
        self.config.partition_by = interval;
    }

    // -- Accessors --

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn active_count(&self) -> usize {
        self.partitions
            .values()
            .filter(|e| e.meta.state == PartitionState::Active)
            .count()
    }

    pub fn sealed_count(&self) -> usize {
        self.partitions
            .values()
            .filter(|e| e.meta.state == PartitionState::Sealed)
            .count()
    }

    pub fn current_interval(&self) -> &PartitionInterval {
        &self.current_interval
    }

    pub fn rate(&self) -> f64 {
        self.rate_estimator.rate()
    }

    pub fn get(&self, start_ts: i64) -> Option<&PartitionEntry> {
        self.partitions.get(&start_ts)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&i64, &PartitionEntry)> {
        self.partitions.iter()
    }

    /// Export registry state for persistence.
    pub fn export(&self) -> Vec<(i64, PartitionEntry)> {
        self.partitions
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect()
    }

    /// Import persisted registry state.
    pub fn import(&mut self, entries: Vec<(i64, PartitionEntry)>) {
        for (start, entry) in entries {
            self.partitions.insert(start, entry);
        }
    }
}

// ---------------------------------------------------------------------------
// Directory naming
// ---------------------------------------------------------------------------

/// Format a partition directory name from start/end timestamps.
///
/// Uses `YYYYMMDD-HHmmss` format (no colons, cross-platform safe).
pub fn format_partition_dir(start_ms: i64, end_ms: i64) -> String {
    format!(
        "ts-{}_{}",
        format_compact_ts(start_ms),
        format_compact_ts(end_ms)
    )
}

/// Format a timestamp as `YYYYMMDD-HHmmss`.
fn format_compact_ts(ms: i64) -> String {
    if ms == i64::MAX {
        return "unbounded".to_string();
    }
    // Simple epoch-to-date conversion (no external dep).
    let secs = ms / 1000;
    let (year, month, day, hour, min, sec) = epoch_to_datetime(secs);
    format!("{year:04}{month:02}{day:02}-{hour:02}{min:02}{sec:02}")
}

/// Convert epoch seconds to (year, month, day, hour, minute, second).
/// Civil calendar via Hinnant's algorithm.
fn epoch_to_datetime(epoch_secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let secs_in_day = 86400i64;
    let mut days = epoch_secs / secs_in_day;
    let time_of_day = (epoch_secs % secs_in_day + secs_in_day) % secs_in_day;

    let hour = (time_of_day / 3600) as u32;
    let min = ((time_of_day % 3600) / 60) as u32;
    let sec = (time_of_day % 60) as u32;

    // Shift epoch from 1970-01-01 to 0000-03-01.
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as i32, m, d, hour, min, sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TieredPartitionConfig {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
        cfg.merge_after_ms = 7 * 86_400_000;
        cfg.merge_count = 3;
        cfg.retention_period_ms = 30 * 86_400_000;
        cfg
    }

    #[test]
    fn create_partition() {
        let mut reg = PartitionRegistry::new(test_config());
        let (entry, is_new) = reg.get_or_create_partition(86_400_000 * 5 + 1000);
        assert!(is_new);
        assert_eq!(entry.meta.state, PartitionState::Active);
        assert!(entry.dir_name.starts_with("ts-"));

        // Same timestamp range → same partition.
        let (_, is_new2) = reg.get_or_create_partition(86_400_000 * 5 + 2000);
        assert!(!is_new2);
        assert_eq!(reg.partition_count(), 1);
    }

    #[test]
    fn different_days_different_partitions() {
        let mut reg = PartitionRegistry::new(test_config());
        reg.get_or_create_partition(86_400_000); // day 1
        reg.get_or_create_partition(86_400_000 * 2); // day 2
        reg.get_or_create_partition(86_400_000 * 3); // day 3
        assert_eq!(reg.partition_count(), 3);
    }

    #[test]
    fn seal_partition() {
        let mut reg = PartitionRegistry::new(test_config());
        let day1_start = 86_400_000i64;
        reg.get_or_create_partition(day1_start);
        assert_eq!(reg.active_count(), 1);

        assert!(reg.seal_partition(day1_start));
        assert_eq!(reg.active_count(), 0);
        assert_eq!(reg.sealed_count(), 1);
    }

    #[test]
    fn query_partitions_pruning() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;
        for d in 1..=10 {
            let (_, _) = reg.get_or_create_partition(d * day_ms);
        }

        // Query days 3-5.
        let range = TimeRange::new(3 * day_ms, 5 * day_ms + day_ms - 1);
        let results = reg.query_partitions(&range);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn find_mergeable() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        // Create and seal 6 partitions.
        for d in 1..=6 {
            reg.get_or_create_partition(d * day_ms);
            reg.seal_partition(d * day_ms);
        }

        // None mergeable yet (merge_after = 7d, data is "today").
        let now = 7 * day_ms;
        assert!(reg.find_mergeable(now).is_empty());

        // 15 days later, all are old enough. merge_count=3 → 2 groups.
        let now = 22 * day_ms;
        let groups = reg.find_mergeable(now);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 3);
    }

    #[test]
    fn find_expired() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        for d in 1..=5 {
            let start = d * day_ms;
            reg.get_or_create_partition(start);
            // Manually set max_ts so retention check works.
            if let Some(entry) = reg.partitions.get_mut(&start) {
                entry.meta.max_ts = start + day_ms - 1;
            }
        }

        // 40 days later, retention=30d → days 1-9 expired (but only 1-5 exist).
        let now = 40 * day_ms;
        let expired = reg.find_expired(now);
        assert_eq!(expired.len(), 5);
    }

    #[test]
    fn commit_merge_and_purge() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        let starts: Vec<i64> = (1..=3).map(|d| d * day_ms).collect();
        for &s in &starts {
            reg.get_or_create_partition(s);
            reg.seal_partition(s);
        }

        // Merge.
        for &s in &starts {
            reg.mark_merging(s);
        }

        let merged_meta = PartitionMeta {
            min_ts: starts[0],
            max_ts: starts[2] + day_ms - 1,
            row_count: 3000,
            size_bytes: 1024,
            schema_version: 1,
            state: PartitionState::Merged,
            interval_ms: 3 * day_ms as u64,
            last_flushed_wal_lsn: 100,
        };
        reg.commit_merge(merged_meta, "ts-merged".into(), &starts);

        // Sources are deleted, merged exists. The merged partition's min_ts
        // equals starts[0], so it overwrites one deleted entry → 3 total
        // (1 merged at starts[0], 2 deleted at starts[1] and starts[2]).
        assert_eq!(reg.partition_count(), 3);
        let dirs = reg.purge_deleted();
        assert_eq!(dirs.len(), 2); // starts[1] and starts[2]
        assert_eq!(reg.partition_count(), 1); // only the merged partition
    }

    #[test]
    fn auto_mode_widen_on_small_partition() {
        let mut cfg = test_config();
        cfg.partition_by = PartitionInterval::Auto;
        let mut reg = PartitionRegistry::new(cfg);

        // Start at 1d.
        assert_eq!(reg.current_interval().as_millis(), Some(86_400_000));

        // Create and seal a partition with < 1000 rows.
        let start = 86_400_000i64;
        reg.get_or_create_partition(start);
        if let Some(entry) = reg.partitions.get_mut(&start) {
            entry.meta.row_count = 50;
        }
        reg.seal_partition(start);

        // Interval should have doubled to 2d.
        assert_eq!(reg.current_interval().as_millis(), Some(2 * 86_400_000));
    }

    #[test]
    fn set_partition_interval_online() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        // Create some 1d partitions.
        reg.get_or_create_partition(day_ms);
        reg.get_or_create_partition(2 * day_ms);

        // Change to 3d.
        reg.set_partition_interval(PartitionInterval::Duration(3 * day_ms as u64));
        assert_eq!(reg.current_interval().as_millis(), Some(3 * 86_400_000));

        // New partition uses 3d boundaries.
        reg.get_or_create_partition(10 * day_ms);
        assert_eq!(reg.partition_count(), 3);
    }

    #[test]
    fn rate_estimator_basic() {
        let mut est = RateEstimator::new();
        // Simulate 1000 rows/sec for 5 seconds.
        for i in 0..5 {
            est.record(1000, i * 1000);
        }
        // Rate should be approaching 1000.
        assert!(est.rate() > 100.0); // EWMA takes time to converge.
    }

    #[test]
    fn format_partition_dir_test() {
        let dir = format_partition_dir(1_704_067_200_000, 1_704_153_600_000);
        // 2024-01-01 00:00:00 to 2024-01-02 00:00:00
        assert_eq!(dir, "ts-20240101-000000_20240102-000000");
    }

    #[test]
    fn unbounded_partition() {
        let mut cfg = test_config();
        cfg.partition_by = PartitionInterval::Unbounded;
        let mut reg = PartitionRegistry::new(cfg);

        reg.get_or_create_partition(1000);
        reg.get_or_create_partition(999_999_999);
        // All go to the same unbounded partition.
        assert_eq!(reg.partition_count(), 1);
    }
}
