//! Per-core atomic usage counters with periodic flush.
//!
//! Each Data Plane core has its own set of counters (no contention).
//! Periodically flushed to the Control Plane `_system.usage` store.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// A single metering event with dimension values.
#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub auth_user_id: String,
    pub org_id: String,
    pub tenant_id: u64,
    pub collection: String,
    pub engine: String,
    pub operation: String,
    pub tokens: u64,
    pub timestamp_secs: u64,
}

/// Aggregation key for bucketing usage events.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct BucketKey {
    auth_user_id: String,
    org_id: String,
    tenant_id: u64,
    collection: String,
    engine: String,
    operation: String,
}

/// Per-core usage counter that aggregates events into buckets.
pub struct UsageCounter {
    /// Aggregated token counts per bucket key.
    buckets: RwLock<HashMap<BucketKey, AtomicU64>>,
    /// Total tokens metered since last flush.
    total_tokens: AtomicU64,
}

impl UsageCounter {
    pub fn new() -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
            total_tokens: AtomicU64::new(0),
        }
    }

    /// Record a usage event. Lock-free on hot path (bucket already exists).
    pub fn record(&self, event: &UsageEvent) {
        let key = BucketKey {
            auth_user_id: event.auth_user_id.clone(),
            org_id: event.org_id.clone(),
            tenant_id: event.tenant_id,
            collection: event.collection.clone(),
            engine: event.engine.clone(),
            operation: event.operation.clone(),
        };

        // Fast path: bucket exists, atomic add.
        {
            let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
            if let Some(counter) = buckets.get(&key) {
                counter.fetch_add(event.tokens, Ordering::Relaxed);
                self.total_tokens.fetch_add(event.tokens, Ordering::Relaxed);
                return;
            }
        }

        // Slow path: create bucket.
        let mut buckets = self.buckets.write().unwrap_or_else(|p| p.into_inner());
        let counter = buckets.entry(key).or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(event.tokens, Ordering::Relaxed);
        self.total_tokens.fetch_add(event.tokens, Ordering::Relaxed);
    }

    /// Drain all accumulated counters for flushing to the store.
    /// Returns aggregated events and resets counters to zero.
    pub fn drain(&self) -> Vec<UsageEvent> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
        let mut events = Vec::with_capacity(buckets.len());

        for (key, counter) in buckets.iter() {
            let tokens = counter.swap(0, Ordering::Relaxed);
            if tokens > 0 {
                events.push(UsageEvent {
                    auth_user_id: key.auth_user_id.clone(),
                    org_id: key.org_id.clone(),
                    tenant_id: key.tenant_id,
                    collection: key.collection.clone(),
                    engine: key.engine.clone(),
                    operation: key.operation.clone(),
                    tokens,
                    timestamp_secs: now,
                });
            }
        }

        self.total_tokens.store(0, Ordering::Relaxed);
        events
    }

    /// Total tokens metered since last flush.
    pub fn total_tokens(&self) -> u64 {
        self.total_tokens.load(Ordering::Relaxed)
    }
}

impl Default for UsageCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the periodic usage flush task.
pub fn spawn_flush_task(
    counter: std::sync::Arc<UsageCounter>,
    store: std::sync::Arc<super::store::UsageStore>,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(10)));
        ticker.tick().await; // Skip first immediate tick.
        loop {
            ticker.tick().await;
            let events = counter.drain();
            if !events.is_empty() {
                store.ingest(events);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(user: &str, op: &str, tokens: u64) -> UsageEvent {
        UsageEvent {
            auth_user_id: user.into(),
            org_id: "acme".into(),
            tenant_id: 1,
            collection: "orders".into(),
            engine: "document_schemaless".into(),
            operation: op.into(),
            tokens,
            timestamp_secs: 0,
        }
    }

    #[test]
    fn record_and_drain() {
        let counter = UsageCounter::new();
        counter.record(&test_event("u1", "point_get", 1));
        counter.record(&test_event("u1", "point_get", 1));
        counter.record(&test_event("u1", "vector_search", 20));

        assert_eq!(counter.total_tokens(), 22);

        let events = counter.drain();
        assert_eq!(events.len(), 2); // 2 unique bucket keys.
        assert_eq!(counter.total_tokens(), 0); // Reset after drain.

        let get_tokens: u64 = events
            .iter()
            .filter(|e| e.operation == "point_get")
            .map(|e| e.tokens)
            .sum();
        assert_eq!(get_tokens, 2);
    }

    #[test]
    fn different_users_separate_buckets() {
        let counter = UsageCounter::new();
        counter.record(&test_event("u1", "point_get", 1));
        counter.record(&test_event("u2", "point_get", 1));

        let events = counter.drain();
        assert_eq!(events.len(), 2);
    }
}
