//! Bridge throughput and backpressure metrics.
//!
//! All counters are atomic so they can be read from any thread (e.g., a metrics
//! exporter on the Control Plane reading counters owned by the Data Plane).

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared metrics for a single SPSC channel.
pub struct BridgeMetrics {
    /// Total items pushed (written by producer).
    push_count: AtomicU64,

    /// Total items popped (written by consumer).
    pop_count: AtomicU64,

    /// Number of times the producer found the queue full.
    full_count: AtomicU64,
}

impl BridgeMetrics {
    pub(crate) fn new() -> Self {
        Self {
            push_count: AtomicU64::new(0),
            pop_count: AtomicU64::new(0),
            full_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn record_push(&self) {
        self.push_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_pop(&self) {
        self.pop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_pops(&self, n: u64) {
        self.pop_count.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn record_full(&self) {
        self.full_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Total items successfully enqueued.
    pub fn pushes(&self) -> u64 {
        self.push_count.load(Ordering::Relaxed)
    }

    /// Total items successfully dequeued.
    pub fn pops(&self) -> u64 {
        self.pop_count.load(Ordering::Relaxed)
    }

    /// Number of times the producer was blocked by a full queue.
    pub fn full_events(&self) -> u64 {
        self.full_count.load(Ordering::Relaxed)
    }

    /// Items currently in flight (pushed but not yet popped).
    pub fn in_flight(&self) -> u64 {
        let pushed = self.pushes();
        let popped = self.pops();
        pushed.saturating_sub(popped)
    }
}
