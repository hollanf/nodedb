//! Per-partition watermark tracker.
//!
//! Tracks the minimum LSN below which no more events will arrive for
//! each partition (vShard). Advances monotonically as events are consumed.
//!
//! Used by streaming MVs to determine when a time bucket is "complete"
//! (all partitions have advanced past its boundary).

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks per-partition watermarks across all cores.
///
/// Thread-safe: updated by Event Plane consumer tasks (one per core),
/// read by streaming MV finalization logic.
pub struct WatermarkTracker {
    /// partition_id → minimum LSN below which no more events will arrive.
    partitions: RwLock<HashMap<u16, u64>>,
    /// Global watermark: min(per_partition_watermarks).
    /// An event is "complete" when all partition watermarks exceed its LSN.
    global_watermark: AtomicU64,
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
            global_watermark: AtomicU64::new(0),
        }
    }

    /// Advance the watermark for a specific partition.
    ///
    /// Called after processing each event. The watermark only advances
    /// forward (monotonically increasing).
    pub fn advance(&self, partition_id: u16, lsn: u64) {
        let mut partitions = self.partitions.write().unwrap_or_else(|p| p.into_inner());
        let current = partitions.entry(partition_id).or_insert(0);
        if lsn > *current {
            *current = lsn;
        }

        // Recompute global watermark.
        if !partitions.is_empty() {
            let min_lsn = partitions.values().copied().min().unwrap_or(0);
            self.global_watermark.store(min_lsn, Ordering::Relaxed);
        }
    }

    /// Get the watermark for a specific partition.
    pub fn partition_watermark(&self, partition_id: u16) -> u64 {
        let partitions = self.partitions.read().unwrap_or_else(|p| p.into_inner());
        partitions.get(&partition_id).copied().unwrap_or(0)
    }

    /// Get the global watermark (min across all partitions).
    ///
    /// An event with LSN <= global_watermark has been processed by ALL partitions.
    pub fn global_watermark(&self) -> u64 {
        self.global_watermark.load(Ordering::Relaxed)
    }

    /// Get all partition watermarks as a sorted vec.
    pub fn all_partitions(&self) -> Vec<(u16, u64)> {
        let partitions = self.partitions.read().unwrap_or_else(|p| p.into_inner());
        let mut result: Vec<(u16, u64)> = partitions.iter().map(|(&k, &v)| (k, v)).collect();
        result.sort_by_key(|(pid, _)| *pid);
        result
    }

    /// Number of tracked partitions.
    pub fn partition_count(&self) -> usize {
        let partitions = self.partitions.read().unwrap_or_else(|p| p.into_inner());
        partitions.len()
    }
}

impl Default for WatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_monotonically() {
        let tracker = WatermarkTracker::new();
        tracker.advance(0, 100);
        tracker.advance(0, 200);
        tracker.advance(0, 150); // Should not go backwards.

        assert_eq!(tracker.partition_watermark(0), 200);
    }

    #[test]
    fn global_watermark_is_min() {
        let tracker = WatermarkTracker::new();
        tracker.advance(0, 100);
        tracker.advance(1, 200);
        tracker.advance(2, 50);

        assert_eq!(tracker.global_watermark(), 50);

        // Advance partition 2 past others.
        tracker.advance(2, 300);
        assert_eq!(tracker.global_watermark(), 100); // Min is now partition 0.
    }

    #[test]
    fn unknown_partition_returns_zero() {
        let tracker = WatermarkTracker::new();
        assert_eq!(tracker.partition_watermark(99), 0);
    }

    #[test]
    fn all_partitions_sorted() {
        let tracker = WatermarkTracker::new();
        tracker.advance(2, 200);
        tracker.advance(0, 100);
        tracker.advance(1, 150);

        let all = tracker.all_partitions();
        assert_eq!(all, vec![(0, 100), (1, 150), (2, 200)]);
    }
}
