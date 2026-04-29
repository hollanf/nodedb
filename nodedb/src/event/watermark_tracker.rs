//! Per-partition watermark tracker.
//!
//! Tracks `(LSN, event_time)` pairs per partition (vShard). The global
//! watermark event_time is `min(per_partition_event_times)` — the wall-clock
//! time below which ALL partitions have advanced. This enables streaming MV
//! finalization: groups with `latest_event_time < global_watermark_event_time`
//! are complete and will receive no more events.
//!
//! LSN provides the ordering guarantee. Event_time provides the wall-clock
//! mapping for time-bucket finalization. Both advance monotonically.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-partition watermark state.
#[derive(Debug, Clone, Copy)]
struct PartitionWatermark {
    /// Highest processed LSN for this partition.
    lsn: u64,
    /// Wall-clock event_time (epoch ms) of the event at this LSN.
    event_time_ms: u64,
}

/// Tracks per-partition watermarks across all cores.
///
/// Thread-safe: updated by Event Plane consumer tasks (one per core),
/// read by streaming MV finalization and CDC late-data enforcement.
pub struct WatermarkTracker {
    /// partition_id → watermark state.
    partitions: RwLock<HashMap<u32, PartitionWatermark>>,
    /// Global watermark LSN: min(per_partition_lsns).
    global_watermark_lsn: AtomicU64,
    /// Global watermark event_time: min(per_partition_event_times).
    /// This is the wall-clock time below which ALL partitions have advanced.
    global_watermark_event_time: AtomicU64,
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
            global_watermark_lsn: AtomicU64::new(0),
            global_watermark_event_time: AtomicU64::new(0),
        }
    }

    /// Advance the watermark for a specific partition.
    ///
    /// Called after processing each event. Both LSN and event_time
    /// advance monotonically (later events have higher LSN and
    /// typically higher event_time).
    pub fn advance(&self, partition_id: u32, lsn: u64, event_time_ms: u64) {
        let mut partitions = self.partitions.write().unwrap_or_else(|p| p.into_inner());
        let current = partitions
            .entry(partition_id)
            .or_insert(PartitionWatermark {
                lsn: 0,
                event_time_ms: 0,
            });

        if lsn > current.lsn {
            current.lsn = lsn;
            // Only advance event_time if it's newer (wall clocks can jitter).
            if event_time_ms > current.event_time_ms {
                current.event_time_ms = event_time_ms;
            }
        }

        // Recompute global watermarks.
        if !partitions.is_empty() {
            let min_lsn = partitions.values().map(|w| w.lsn).min().unwrap_or(0);
            let min_event_time = partitions
                .values()
                .map(|w| w.event_time_ms)
                .min()
                .unwrap_or(0);
            self.global_watermark_lsn.store(min_lsn, Ordering::Relaxed);
            self.global_watermark_event_time
                .store(min_event_time, Ordering::Relaxed);
        }
    }

    /// Advance watermark with LSN only (for heartbeats that don't carry event_time).
    /// Uses the partition's existing event_time (heartbeats don't generate new wall-clock data).
    pub fn advance_lsn_only(&self, partition_id: u32, lsn: u64) {
        let mut partitions = self.partitions.write().unwrap_or_else(|p| p.into_inner());
        let current = partitions
            .entry(partition_id)
            .or_insert(PartitionWatermark {
                lsn: 0,
                event_time_ms: 0,
            });

        if lsn > current.lsn {
            current.lsn = lsn;
            // Don't update event_time — heartbeats carry no wall-clock data.
            // If event_time is still 0 (no data events yet), use current wall-clock
            // so this partition doesn't block finalization forever.
            if current.event_time_ms == 0 {
                current.event_time_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
            }
        }

        if !partitions.is_empty() {
            let min_lsn = partitions.values().map(|w| w.lsn).min().unwrap_or(0);
            let min_event_time = partitions
                .values()
                .map(|w| w.event_time_ms)
                .min()
                .unwrap_or(0);
            self.global_watermark_lsn.store(min_lsn, Ordering::Relaxed);
            self.global_watermark_event_time
                .store(min_event_time, Ordering::Relaxed);
        }
    }

    /// Get the LSN watermark for a specific partition.
    pub fn partition_watermark(&self, partition_id: u32) -> u64 {
        let partitions = self.partitions.read().unwrap_or_else(|p| p.into_inner());
        partitions.get(&partition_id).map(|w| w.lsn).unwrap_or(0)
    }

    /// Get the global watermark LSN (min across all partitions).
    pub fn global_watermark(&self) -> u64 {
        self.global_watermark_lsn.load(Ordering::Relaxed)
    }

    /// Get the global watermark event_time (min across all partitions).
    ///
    /// This is the wall-clock time below which ALL partitions have advanced.
    /// Streaming MV groups with `latest_event_time < global_watermark_event_time()`
    /// are finalized — no more events will arrive for them.
    pub fn global_watermark_event_time(&self) -> u64 {
        self.global_watermark_event_time.load(Ordering::Relaxed)
    }

    /// Get all partition watermarks as a sorted vec.
    pub fn all_partitions(&self) -> Vec<(u32, u64)> {
        let partitions = self.partitions.read().unwrap_or_else(|p| p.into_inner());
        let mut result: Vec<(u32, u64)> = partitions.iter().map(|(&k, w)| (k, w.lsn)).collect();
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
        tracker.advance(0, 100, 1000);
        tracker.advance(0, 200, 2000);
        tracker.advance(0, 150, 1500); // LSN doesn't go backwards.

        assert_eq!(tracker.partition_watermark(0), 200);
    }

    #[test]
    fn global_watermark_is_min() {
        let tracker = WatermarkTracker::new();
        tracker.advance(0, 100, 1000);
        tracker.advance(1, 200, 2000);
        tracker.advance(2, 50, 500);

        assert_eq!(tracker.global_watermark(), 50);
        assert_eq!(tracker.global_watermark_event_time(), 500);

        tracker.advance(2, 300, 3000);
        assert_eq!(tracker.global_watermark(), 100);
        assert_eq!(tracker.global_watermark_event_time(), 1000);
    }

    #[test]
    fn unknown_partition_returns_zero() {
        let tracker = WatermarkTracker::new();
        assert_eq!(tracker.partition_watermark(99), 0);
    }

    #[test]
    fn all_partitions_sorted() {
        let tracker = WatermarkTracker::new();
        tracker.advance(2, 200, 2000);
        tracker.advance(0, 100, 1000);
        tracker.advance(1, 150, 1500);

        let all = tracker.all_partitions();
        assert_eq!(all, vec![(0, 100), (1, 150), (2, 200)]);
    }

    #[test]
    fn event_time_tracks_independently() {
        let tracker = WatermarkTracker::new();
        tracker.advance(0, 100, 5000);
        tracker.advance(1, 200, 3000); // Higher LSN but lower event_time.

        // Global event_time is min of per-partition event_times.
        assert_eq!(tracker.global_watermark_event_time(), 3000);
    }

    #[test]
    fn heartbeat_advances_lsn_without_event_time() {
        let tracker = WatermarkTracker::new();
        tracker.advance(0, 100, 5000);
        tracker.advance_lsn_only(0, 200); // Heartbeat — no new event_time.

        assert_eq!(tracker.partition_watermark(0), 200);
        // event_time stays at 5000 (heartbeats don't carry wall-clock data).
    }
}
