//! Usage store: accumulates flushed usage events for querying.
//!
//! In production, this would write to `_system.usage` as a timeseries
//! collection with rollups and retention. For now, it stores in-memory
//! with a ring buffer for the most recent events.

use std::collections::HashMap;
use std::sync::RwLock;

use super::counter::UsageEvent;

/// In-memory usage store with ring buffer.
pub struct UsageStore {
    /// Recent events (ring buffer, max capacity).
    events: RwLock<Vec<UsageEvent>>,
    /// Maximum events to retain.
    max_events: usize,
    /// Aggregated totals per user for quota checking.
    user_totals: RwLock<HashMap<String, u64>>,
    /// Aggregated totals per org.
    org_totals: RwLock<HashMap<String, u64>>,
    /// Persistent per-tenant usage summaries. Updated on every `ingest()` call.
    /// Serves as the queryable aggregation layer for `SHOW USAGE FOR TENANT`
    /// and `EXPORT USAGE`. Survives ring buffer eviction.
    tenant_totals: RwLock<HashMap<u64, TenantUsageSummary>>,
}

impl UsageStore {
    pub fn new(max_events: usize) -> Self {
        Self {
            events: RwLock::new(Vec::with_capacity(max_events.min(100_000))),
            max_events,
            user_totals: RwLock::new(HashMap::new()),
            org_totals: RwLock::new(HashMap::new()),
            tenant_totals: RwLock::new(HashMap::new()),
        }
    }

    /// Ingest flushed events from the counter.
    ///
    /// Updates three aggregation layers:
    /// 1. Per-user totals (for `SHOW USAGE FOR AUTH USER`)
    /// 2. Per-org totals (for `SHOW USAGE FOR ORG`)
    /// 3. Per-tenant summaries (for `SHOW USAGE FOR TENANT` / `EXPORT USAGE`)
    pub fn ingest(&self, events: Vec<UsageEvent>) {
        // Update per-user and per-org totals.
        {
            let mut user_totals = self.user_totals.write().unwrap_or_else(|p| p.into_inner());
            let mut org_totals = self.org_totals.write().unwrap_or_else(|p| p.into_inner());
            for e in &events {
                *user_totals.entry(e.auth_user_id.clone()).or_insert(0) += e.tokens;
                if !e.org_id.is_empty() {
                    *org_totals.entry(e.org_id.clone()).or_insert(0) += e.tokens;
                }
            }
        }

        // Update per-tenant summaries (persistent aggregation).
        {
            let mut tenant_totals = self
                .tenant_totals
                .write()
                .unwrap_or_else(|p| p.into_inner());
            for e in &events {
                let summary = tenant_totals.entry(e.tenant_id).or_default();
                accumulate_event(summary, e);
            }
        }

        // Store events in ring buffer.
        let mut stored = self.events.write().unwrap_or_else(|p| p.into_inner());
        for e in events {
            if stored.len() >= self.max_events {
                stored.remove(0); // Drop oldest.
            }
            stored.push(e);
        }
    }

    /// Get total tokens used by a user.
    pub fn user_total(&self, user_id: &str) -> u64 {
        let totals = self.user_totals.read().unwrap_or_else(|p| p.into_inner());
        *totals.get(user_id).unwrap_or(&0)
    }

    /// Get total tokens used by an org.
    pub fn org_total(&self, org_id: &str) -> u64 {
        let totals = self.org_totals.read().unwrap_or_else(|p| p.into_inner());
        *totals.get(org_id).unwrap_or(&0)
    }

    /// Query usage events filtered by user and/or time range.
    pub fn query(
        &self,
        user_filter: Option<&str>,
        org_filter: Option<&str>,
        since_secs: u64,
    ) -> Vec<UsageEvent> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events
            .iter()
            .filter(|e| {
                let user_ok = user_filter.is_none_or(|u| e.auth_user_id == u);
                let org_ok = org_filter.is_none_or(|o| e.org_id == o);
                let time_ok = since_secs == 0 || e.timestamp_secs >= since_secs;
                user_ok && org_ok && time_ok
            })
            .cloned()
            .collect()
    }

    /// Export usage as NDJSON (newline-delimited JSON).
    pub fn export_ndjson(&self, user_filter: Option<&str>, since_secs: u64) -> String {
        let events = self.query(user_filter, None, since_secs);
        events
            .iter()
            .map(|e| {
                serde_json::json!({
                    "auth_user_id": e.auth_user_id,
                    "org_id": e.org_id,
                    "tenant_id": e.tenant_id,
                    "collection": e.collection,
                    "engine": e.engine,
                    "operation": e.operation,
                    "tokens": e.tokens,
                    "timestamp": e.timestamp_secs,
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Query usage events filtered by tenant_id and optional time range.
    pub fn query_by_tenant(&self, tenant_id: u64, since_secs: u64) -> Vec<UsageEvent> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events
            .iter()
            .filter(|e| {
                e.tenant_id == tenant_id && (since_secs == 0 || e.timestamp_secs >= since_secs)
            })
            .cloned()
            .collect()
    }

    /// Export usage for a specific tenant as JSON (billing integration format).
    pub fn export_tenant_json(&self, tenant_id: u64, since_secs: u64) -> String {
        let events = self.query_by_tenant(tenant_id, since_secs);

        let mut summary = TenantUsageSummary::default();
        for e in &events {
            accumulate_event(&mut summary, e);
        }

        serde_json::json!({
            "tenant_id": tenant_id,
            "reads": { "count": summary.reads_count, "tokens": summary.reads_tokens },
            "writes": { "count": summary.writes_count, "tokens": summary.writes_tokens },
            "vector_searches": summary.vector_searches,
            "graph_traversals": summary.graph_traversals,
            "total_events": summary.total_events,
        })
        .to_string()
    }

    /// Aggregate usage events by tenant_id.
    ///
    /// Returns a map of `tenant_id → TenantUsageSummary` with rolled-up
    /// read/write counts and engine-specific metrics.
    pub fn aggregate_by_tenant(&self) -> HashMap<u64, TenantUsageSummary> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        let mut summaries: HashMap<u64, TenantUsageSummary> = HashMap::new();

        for e in events.iter() {
            let summary = summaries.entry(e.tenant_id).or_default();
            accumulate_event(summary, e);
        }

        summaries
    }

    /// Get the persistent usage summary for a specific tenant.
    ///
    /// Unlike `aggregate_by_tenant()` which recomputes from the event ring buffer,
    /// this reads from the persistent aggregation layer that survives ring buffer eviction.
    pub fn tenant_summary(&self, tenant_id: u64) -> Option<TenantUsageSummary> {
        let totals = self.tenant_totals.read().unwrap_or_else(|p| p.into_inner());
        totals.get(&tenant_id).cloned()
    }

    /// Total events stored.
    pub fn count(&self) -> usize {
        self.events.read().unwrap_or_else(|p| p.into_inner()).len()
    }
}

/// Accumulate a single usage event into a summary.
fn accumulate_event(summary: &mut TenantUsageSummary, e: &UsageEvent) {
    summary.total_tokens += e.tokens;
    summary.total_events += 1;
    if is_read_operation(&e.operation) {
        summary.reads_count += 1;
        summary.reads_tokens += e.tokens;
    } else {
        summary.writes_count += 1;
        summary.writes_tokens += e.tokens;
    }
    if e.operation == "vector_search" {
        summary.vector_searches += 1;
    }
    if e.engine == "graph" {
        summary.graph_traversals += 1;
    }
}

/// Whether an operation is a read (vs. write) based on the operation name.
fn is_read_operation(operation: &str) -> bool {
    matches!(
        operation,
        "point_get"
            | "range_scan"
            | "kv_get"
            | "kv_scan"
            | "text_search"
            | "vector_search"
            | "timeseries_scan"
            | "columnar_scan"
    )
}

/// Aggregated usage summary for a single tenant.
///
/// Computed by [`UsageStore::aggregate_by_tenant()`] from raw usage events.
/// Used by billing integration, quota enforcement, and the SHOW TENANT USAGE DDL.
#[derive(Debug, Default, Clone)]
pub struct TenantUsageSummary {
    /// Total tokens consumed across all operations.
    pub total_tokens: u64,
    /// Total number of metering events recorded.
    pub total_events: u64,
    /// Number of read operations (point get, scan, search).
    pub reads_count: u64,
    /// Tokens consumed by read operations.
    pub reads_tokens: u64,
    /// Number of write operations (put, delete, bulk).
    pub writes_count: u64,
    /// Tokens consumed by write operations.
    pub writes_tokens: u64,
    /// Number of vector similarity searches.
    pub vector_searches: u64,
    /// Number of graph traversal operations.
    pub graph_traversals: u64,
}

impl Default for UsageStore {
    fn default() -> Self {
        Self::new(100_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(user: &str, tokens: u64) -> UsageEvent {
        UsageEvent {
            auth_user_id: user.into(),
            org_id: "acme".into(),
            tenant_id: 1,
            collection: "orders".into(),
            engine: "document_schemaless".into(),
            operation: "point_get".into(),
            tokens,
            timestamp_secs: 1700000000,
        }
    }

    #[test]
    fn ingest_and_query() {
        let store = UsageStore::new(1000);
        store.ingest(vec![test_event("u1", 10), test_event("u2", 20)]);

        assert_eq!(store.count(), 2);
        assert_eq!(store.user_total("u1"), 10);
        assert_eq!(store.user_total("u2"), 20);
        assert_eq!(store.org_total("acme"), 30);
    }

    #[test]
    fn query_with_filter() {
        let store = UsageStore::new(1000);
        store.ingest(vec![test_event("u1", 10), test_event("u2", 20)]);

        let u1_events = store.query(Some("u1"), None, 0);
        assert_eq!(u1_events.len(), 1);
        assert_eq!(u1_events[0].tokens, 10);
    }

    #[test]
    fn ring_buffer_drops_oldest() {
        let store = UsageStore::new(2);
        store.ingest(vec![
            test_event("u1", 1),
            test_event("u2", 2),
            test_event("u3", 3),
        ]);

        assert_eq!(store.count(), 2); // Only last 2 retained.
    }
}
