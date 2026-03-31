//! Event Plane metrics: per-core atomic counters for observability.
//!
//! All counters use `Relaxed` ordering — they are informational metrics,
//! not synchronization primitives. Exact consistency is not required.
//!
//! Per-tenant metrics track event counts by tenant for multi-tenant
//! observability. The tenant map is bounded (max 1024 tenants tracked)
//! to prevent unbounded memory growth.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum number of tenants tracked per core. Prevents unbounded growth
/// if rogue requests create many tenant IDs.
const MAX_TRACKED_TENANTS: usize = 1024;

/// Per-core metrics for one Event Plane consumer.
pub struct CoreMetrics {
    /// Total events successfully enqueued by the Data Plane producer.
    pub events_emitted: AtomicU64,
    /// Total events processed by the Event Plane consumer.
    pub events_processed: AtomicU64,
    /// Total events lost due to ring buffer overflow (detected via sequence gaps).
    pub events_dropped: AtomicU64,
    /// Last processed sequence number.
    pub last_sequence: AtomicU64,
    /// Last processed LSN (for lag calculation).
    pub last_processed_lsn: AtomicU64,
    /// Number of times WAL catchup mode was entered.
    pub wal_catchup_count: AtomicU64,
    /// Number of events replayed from WAL (startup + catchup combined).
    pub wal_replay_count: AtomicU64,
    /// Current ring buffer utilization (0–100). Updated by the Data Plane
    /// producer via `EventProducer` — the consumer side cannot read utilization.
    pub ring_utilization: AtomicU64,
    /// Number of backpressure transitions (Normal → Throttled or Suspended).
    /// Updated by the Data Plane producer via `EventProducer`.
    pub backpressure_transitions: AtomicU64,
    /// Per-tenant event counts. Bounded to `MAX_TRACKED_TENANTS` to prevent
    /// unbounded growth. Once full, new tenants are silently not tracked.
    tenant_events: RwLock<HashMap<u32, u64>>,
}

impl std::fmt::Debug for CoreMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreMetrics")
            .field("events_emitted", &self.events_emitted)
            .field("events_processed", &self.events_processed)
            .field("events_dropped", &self.events_dropped)
            .finish()
    }
}

impl CoreMetrics {
    pub fn new() -> Self {
        Self {
            events_emitted: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            last_sequence: AtomicU64::new(0),
            last_processed_lsn: AtomicU64::new(0),
            wal_catchup_count: AtomicU64::new(0),
            wal_replay_count: AtomicU64::new(0),
            ring_utilization: AtomicU64::new(0),
            backpressure_transitions: AtomicU64::new(0),
            tenant_events: RwLock::new(HashMap::new()),
        }
    }

    pub fn record_emit(&self) {
        self.events_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_process(&self, lsn: u64, sequence: u64) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.last_processed_lsn.store(lsn, Ordering::Relaxed);
        self.last_sequence.store(sequence, Ordering::Relaxed);
    }

    /// Record event processing for a specific tenant.
    ///
    /// Also increments the global `events_processed` counter and updates
    /// LSN/sequence tracking. Silently skips tenant tracking if the
    /// per-core tenant map is full (bounded to `MAX_TRACKED_TENANTS`).
    pub fn record_process_for_tenant(&self, lsn: u64, sequence: u64, tenant_id: u32) {
        self.record_process(lsn, sequence);

        let mut map = match self.tenant_events.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        if let Some(count) = map.get_mut(&tenant_id) {
            *count += 1;
        } else if map.len() < MAX_TRACKED_TENANTS {
            map.insert(tenant_id, 1);
        }
        // If map is full and tenant not present, silently skip.
    }

    /// Snapshot of per-tenant event counts on this core.
    pub fn tenant_event_counts(&self) -> HashMap<u32, u64> {
        match self.tenant_events.read() {
            Ok(m) => m.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    pub fn record_drop(&self, count: u64) {
        self.events_dropped.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_wal_catchup_enter(&self) {
        self.wal_catchup_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_replay(&self, count: u64) {
        self.wal_replay_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn update_utilization(&self, pct: u8) {
        self.ring_utilization.store(pct as u64, Ordering::Relaxed);
    }

    pub fn record_backpressure_transition(&self) {
        self.backpressure_transitions
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for CoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate metrics across all Event Plane consumers.
pub struct AggregateMetrics {
    pub total_emitted: u64,
    pub total_processed: u64,
    pub total_dropped: u64,
    pub total_wal_replayed: u64,
    pub total_wal_catchups: u64,
    pub total_backpressure_transitions: u64,
    /// Per-tenant event counts aggregated across all cores.
    pub tenant_events: HashMap<u32, u64>,
}

impl AggregateMetrics {
    /// Compute aggregate metrics from per-core metrics.
    pub fn from_cores(cores: &[std::sync::Arc<CoreMetrics>]) -> Self {
        let mut agg = Self {
            total_emitted: 0,
            total_processed: 0,
            total_dropped: 0,
            total_wal_replayed: 0,
            total_wal_catchups: 0,
            total_backpressure_transitions: 0,
            tenant_events: HashMap::new(),
        };
        for m in cores {
            agg.total_emitted += m.events_emitted.load(Ordering::Relaxed);
            agg.total_processed += m.events_processed.load(Ordering::Relaxed);
            agg.total_dropped += m.events_dropped.load(Ordering::Relaxed);
            agg.total_wal_replayed += m.wal_replay_count.load(Ordering::Relaxed);
            agg.total_wal_catchups += m.wal_catchup_count.load(Ordering::Relaxed);
            agg.total_backpressure_transitions +=
                m.backpressure_transitions.load(Ordering::Relaxed);

            for (tenant_id, count) in m.tenant_event_counts() {
                *agg.tenant_events.entry(tenant_id).or_default() += count;
            }
        }
        agg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_metrics_basics() {
        let m = CoreMetrics::new();
        m.record_emit();
        m.record_emit();
        m.record_process(100, 1);
        m.record_process(200, 2);
        m.record_drop(3);

        assert_eq!(m.events_emitted.load(Ordering::Relaxed), 2);
        assert_eq!(m.events_processed.load(Ordering::Relaxed), 2);
        assert_eq!(m.events_dropped.load(Ordering::Relaxed), 3);
        assert_eq!(m.last_processed_lsn.load(Ordering::Relaxed), 200);
        assert_eq!(m.last_sequence.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn per_tenant_tracking() {
        let m = CoreMetrics::new();
        m.record_process_for_tenant(10, 1, 1);
        m.record_process_for_tenant(20, 2, 1);
        m.record_process_for_tenant(30, 3, 2);

        let counts = m.tenant_event_counts();
        assert_eq!(counts.get(&1), Some(&2));
        assert_eq!(counts.get(&2), Some(&1));
        assert_eq!(counts.get(&99), None);

        // Global counter should also reflect all 3 events.
        assert_eq!(m.events_processed.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn tenant_tracking_bounded() {
        let m = CoreMetrics::new();
        // Fill up the tenant map to MAX_TRACKED_TENANTS.
        for i in 0..MAX_TRACKED_TENANTS as u32 {
            m.record_process_for_tenant(i as u64, i as u64, i);
        }
        let counts = m.tenant_event_counts();
        assert_eq!(counts.len(), MAX_TRACKED_TENANTS);

        // One more tenant should be silently skipped.
        m.record_process_for_tenant(9999, 9999, MAX_TRACKED_TENANTS as u32 + 1);
        let counts = m.tenant_event_counts();
        assert_eq!(counts.len(), MAX_TRACKED_TENANTS);
        // But global counter still incremented.
        assert_eq!(
            m.events_processed.load(Ordering::Relaxed),
            MAX_TRACKED_TENANTS as u64 + 1
        );
    }

    #[test]
    fn aggregate_from_cores() {
        let c0 = std::sync::Arc::new(CoreMetrics::new());
        let c1 = std::sync::Arc::new(CoreMetrics::new());
        c0.record_emit();
        c0.record_process_for_tenant(10, 1, 1);
        c1.record_emit();
        c1.record_emit();
        c1.record_process_for_tenant(20, 1, 1);
        c1.record_process_for_tenant(30, 2, 2);
        c1.record_drop(5);

        let agg = AggregateMetrics::from_cores(&[c0, c1]);
        assert_eq!(agg.total_emitted, 3);
        assert_eq!(agg.total_processed, 3);
        assert_eq!(agg.total_dropped, 5);

        // Per-tenant aggregation across cores.
        assert_eq!(agg.tenant_events.get(&1), Some(&2)); // 1 from c0 + 1 from c1
        assert_eq!(agg.tenant_events.get(&2), Some(&1)); // 1 from c1
    }
}
