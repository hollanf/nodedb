//! Event Plane memory budget: tracks total usage and enforces cap.
//!
//! Prevents unbounded memory growth by tracking memory usage across all
//! Event Plane components (stream buffers, MV state, DLQ, ring buffers).
//! When usage exceeds the cap, reduces stream retention and emits warnings.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::{info, warn};

/// Default Event Plane memory budget: 512 MB.
const DEFAULT_BUDGET_BYTES: u64 = 512 * 1024 * 1024;

/// Estimated memory per pending cross-shard write entry (request struct + queue overhead).
const CROSS_SHARD_ENTRY_BYTES: u64 = 512;

/// Event Plane memory budget tracker.
pub struct EventPlaneBudget {
    /// Maximum allowed memory usage in bytes.
    limit: AtomicU64,
    /// Current estimated memory usage in bytes.
    current: AtomicU64,
    /// Whether the budget is currently exceeded.
    exceeded: AtomicBool,
    /// Number of times the budget was exceeded.
    exceed_count: AtomicU64,
    /// Pending cross-shard writes count (updated externally).
    pending_cross_shard: AtomicU64,
}

impl EventPlaneBudget {
    pub fn new() -> Self {
        Self {
            limit: AtomicU64::new(DEFAULT_BUDGET_BYTES),
            current: AtomicU64::new(0),
            exceeded: AtomicBool::new(false),
            exceed_count: AtomicU64::new(0),
            pending_cross_shard: AtomicU64::new(0),
        }
    }

    pub fn with_limit(limit_bytes: u64) -> Self {
        Self {
            limit: AtomicU64::new(limit_bytes),
            current: AtomicU64::new(0),
            exceeded: AtomicBool::new(false),
            exceed_count: AtomicU64::new(0),
            pending_cross_shard: AtomicU64::new(0),
        }
    }

    /// Update the current memory usage estimate.
    ///
    /// Call this periodically from the Event Plane (e.g., every 30s)
    /// with the sum of all component memory estimates (stream buffers,
    /// MV state, DLQ, etc.). Pending cross-shard writes are automatically
    /// added to the total.
    pub fn update_usage(&self, base_bytes: u64) {
        self.current.store(base_bytes, Ordering::Relaxed);

        let total_bytes = base_bytes + self.pending_cross_shard_bytes();
        let limit = self.limit.load(Ordering::Relaxed);
        let was_exceeded = self.exceeded.load(Ordering::Relaxed);

        if total_bytes > limit {
            if !was_exceeded {
                self.exceeded.store(true, Ordering::Relaxed);
                self.exceed_count.fetch_add(1, Ordering::Relaxed);
                warn!(
                    total_mb = total_bytes / (1024 * 1024),
                    limit_mb = limit / (1024 * 1024),
                    "Event Plane memory budget EXCEEDED — reducing stream retention"
                );
            }
        } else if was_exceeded {
            self.exceeded.store(false, Ordering::Relaxed);
            info!(
                total_mb = total_bytes / (1024 * 1024),
                limit_mb = limit / (1024 * 1024),
                "Event Plane memory budget returned to normal"
            );
        }
    }

    /// Whether the budget is currently exceeded.
    pub fn is_exceeded(&self) -> bool {
        self.exceeded.load(Ordering::Relaxed)
    }

    /// Whether new change stream subscriptions should be rejected.
    ///
    /// Returns true when the budget is exceeded — existing streams continue
    /// with reduced retention, but new ones are blocked.
    pub fn should_reject_new_streams(&self) -> bool {
        self.is_exceeded()
    }

    /// Current estimated usage in bytes.
    pub fn current_usage(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Configured limit in bytes.
    pub fn limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Usage as a percentage (0-100).
    pub fn usage_percent(&self) -> u8 {
        let current = self.current.load(Ordering::Relaxed);
        let limit = self.limit.load(Ordering::Relaxed);
        if limit == 0 {
            return 0;
        }
        ((current * 100) / limit).min(100) as u8
    }

    /// Number of times the budget was exceeded.
    pub fn exceed_count(&self) -> u64 {
        self.exceed_count.load(Ordering::Relaxed)
    }

    /// Update the pending cross-shard write count.
    ///
    /// Called periodically by the cross-shard dispatcher task.
    /// The estimated memory is `count * CROSS_SHARD_ENTRY_BYTES`.
    pub fn update_pending_cross_shard(&self, count: u64) {
        self.pending_cross_shard.store(count, Ordering::Relaxed);
    }

    /// Estimated memory used by pending cross-shard writes.
    pub fn pending_cross_shard_bytes(&self) -> u64 {
        self.pending_cross_shard.load(Ordering::Relaxed) * CROSS_SHARD_ENTRY_BYTES
    }

    /// Total estimated usage including cross-shard pending writes.
    ///
    /// Callers should pass the base usage (stream buffers, MV state, etc.)
    /// to `update_usage()`. The pending cross-shard bytes are automatically
    /// added to the total.
    pub fn effective_usage(&self) -> u64 {
        self.current.load(Ordering::Relaxed) + self.pending_cross_shard_bytes()
    }

    /// Set a new limit (runtime reconfiguration).
    pub fn set_limit(&self, limit_bytes: u64) {
        self.limit.store(limit_bytes, Ordering::Relaxed);
    }
}

impl Default for EventPlaneBudget {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_512mb() {
        let budget = EventPlaneBudget::new();
        assert_eq!(budget.limit(), 512 * 1024 * 1024);
        assert!(!budget.is_exceeded());
    }

    #[test]
    fn exceeds_on_over_limit() {
        let budget = EventPlaneBudget::with_limit(100);
        budget.update_usage(50);
        assert!(!budget.is_exceeded());

        budget.update_usage(150);
        assert!(budget.is_exceeded());
        assert!(budget.should_reject_new_streams());
        assert_eq!(budget.exceed_count(), 1);

        budget.update_usage(80);
        assert!(!budget.is_exceeded());
    }

    #[test]
    fn usage_percent() {
        let budget = EventPlaneBudget::with_limit(1000);
        budget.update_usage(250);
        assert_eq!(budget.usage_percent(), 25);

        budget.update_usage(1500);
        assert_eq!(budget.usage_percent(), 100); // Capped at 100.
    }
}
