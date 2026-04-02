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
}

impl UsageStore {
    pub fn new(max_events: usize) -> Self {
        Self {
            events: RwLock::new(Vec::with_capacity(max_events.min(100_000))),
            max_events,
            user_totals: RwLock::new(HashMap::new()),
            org_totals: RwLock::new(HashMap::new()),
        }
    }

    /// Ingest flushed events from the counter.
    pub fn ingest(&self, events: Vec<UsageEvent>) {
        // Update aggregated totals.
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
    pub fn query_by_tenant(&self, tenant_id: u32, since_secs: u64) -> Vec<UsageEvent> {
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
    pub fn export_tenant_json(&self, tenant_id: u32, since_secs: u64) -> String {
        let events = self.query_by_tenant(tenant_id, since_secs);

        let mut reads_count: u64 = 0;
        let mut reads_tokens: u64 = 0;
        let mut writes_count: u64 = 0;
        let mut writes_tokens: u64 = 0;
        let mut vector_searches: u64 = 0;
        let mut graph_traversals: u64 = 0;

        for e in &events {
            match e.operation.as_str() {
                "point_get" | "range_scan" | "kv_get" | "kv_scan" | "text_search"
                | "vector_search" | "timeseries_scan" | "columnar_scan" => {
                    reads_count += 1;
                    reads_tokens += e.tokens;
                }
                _ => {
                    writes_count += 1;
                    writes_tokens += e.tokens;
                }
            }
            if e.operation == "vector_search" {
                vector_searches += 1;
            }
            if e.engine == "graph" {
                graph_traversals += 1;
            }
        }

        serde_json::json!({
            "tenant_id": tenant_id,
            "reads": { "count": reads_count, "tokens": reads_tokens },
            "writes": { "count": writes_count, "tokens": writes_tokens },
            "vector_searches": vector_searches,
            "graph_traversals": graph_traversals,
            "total_events": events.len(),
        })
        .to_string()
    }

    /// Total events stored.
    pub fn count(&self) -> usize {
        self.events.read().unwrap_or_else(|p| p.into_inner()).len()
    }
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
            engine: "document".into(),
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
