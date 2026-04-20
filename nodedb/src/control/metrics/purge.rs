//! Collection hard-delete ("purge") metrics.
//!
//! These track the Event Plane retention-GC sweeper: how many
//! soft-deleted collections are still sitting in the retention window,
//! how long their hard-delete actually takes, how many bytes get
//! reclaimed, and the depth of the L2 (S3) delete backlog.
//!
//! All metrics carry labels operators need for sizing retention
//! windows and diagnosing stuck purges:
//! - `tenant` — numeric tenant id, rendered as a string label.
//! - `engine` — storage engine slug from
//!   `StoredCollection::collection_type.as_str()` (e.g. `"document"`,
//!   `"columnar"`, `"timeseries"`).
//! - `tier` — `"l1"` or `"l2"` for reclaim accounting.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use super::histogram::AtomicHistogram;

/// Purge sweeper + reclaim metrics.
#[derive(Debug, Default)]
pub struct PurgeMetrics {
    /// `nodedb_deactivated_collections_pending_purge{tenant}` — gauge of
    /// soft-deleted collections still inside the retention window.
    /// Refreshed by the sweeper each pass.
    pub pending_by_tenant: RwLock<HashMap<u32, u64>>,

    /// `nodedb_collection_purge_duration_seconds{tenant,engine}` —
    /// histogram of how long a single collection's hard-delete takes,
    /// end-to-end (reclaim + catalog cleanup + WAL tombstone emit).
    /// Key: `(tenant_id, engine_slug)`.
    pub purge_duration_us: RwLock<HashMap<(u32, String), AtomicHistogram>>,

    /// `nodedb_collection_purge_bytes_reclaimed_total{tenant,engine,tier}` —
    /// counter of bytes reclaimed during hard-delete, broken out by
    /// storage tier so operators can tell L1 (NVMe) reclaim apart
    /// from L2 (S3) reclaim.
    /// Key: `(tenant_id, engine_slug, tier)`.
    pub bytes_reclaimed: RwLock<HashMap<(u32, String, &'static str), AtomicU64>>,

    /// `nodedb_l2_cleanup_queue_depth{tenant}` — gauge of pending S3
    /// delete operations per tenant. Refreshed by the L2 cleanup
    /// worker. High and persistent values mean S3 delete is falling
    /// behind and storage cost is growing silently.
    pub l2_cleanup_queue_depth: RwLock<HashMap<u32, u64>>,
}

impl PurgeMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the per-tenant pending-purge snapshot. The sweeper
    /// should pass the complete map each pass so tenants whose
    /// dropped-collection count went to zero drop out of the gauge.
    pub fn set_pending_by_tenant(&self, snapshot: HashMap<u32, u64>) {
        let mut m = self
            .pending_by_tenant
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m = snapshot;
    }

    /// Record one end-to-end hard-delete duration (microseconds).
    pub fn record_purge_duration(&self, tenant: u32, engine: &str, duration_us: u64) {
        let key = (tenant, engine.to_string());
        let mut m = self
            .purge_duration_us
            .write()
            .unwrap_or_else(|p| p.into_inner());
        m.entry(key).or_default().observe(duration_us);
    }

    /// Add bytes reclaimed for a tenant+engine+tier.
    /// `tier` must be `"l1"` or `"l2"`.
    pub fn add_bytes_reclaimed(&self, tenant: u32, engine: &str, tier: &'static str, bytes: u64) {
        let key = (tenant, engine.to_string(), tier);
        let mut m = self
            .bytes_reclaimed
            .write()
            .unwrap_or_else(|p| p.into_inner());
        m.entry(key)
            .or_default()
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Replace the L2 cleanup queue depth snapshot.
    pub fn set_l2_cleanup_queue_depth(&self, snapshot: HashMap<u32, u64>) {
        let mut m = self
            .l2_cleanup_queue_depth
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m = snapshot;
    }

    /// Serialize all purge metrics in Prometheus text format.
    pub fn write_prometheus(&self, out: &mut String) {
        self.write_pending(out);
        self.write_durations(out);
        self.write_bytes_reclaimed(out);
        self.write_l2_queue(out);
    }

    fn write_pending(&self, out: &mut String) {
        let m = self
            .pending_by_tenant
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_deactivated_collections_pending_purge Soft-deleted collections inside the retention window\n\
             # TYPE nodedb_deactivated_collections_pending_purge gauge\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by_key(|(t, _)| *t);
        for (tenant, count) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_deactivated_collections_pending_purge{{tenant="{tenant}"}} {count}"#
            );
        }
    }

    fn write_durations(&self, out: &mut String) {
        let m = self
            .purge_duration_us
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_collection_purge_duration_seconds End-to-end hard-delete duration per collection\n\
             # TYPE nodedb_collection_purge_duration_seconds histogram\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for ((tenant, engine), hist) in pairs {
            let name = format!(
                r#"nodedb_collection_purge_duration_seconds{{tenant="{tenant}",engine="{engine}"}}"#
            );
            hist.write_prometheus(out, &name, "");
        }
    }

    fn write_bytes_reclaimed(&self, out: &mut String) {
        let m = self
            .bytes_reclaimed
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_collection_purge_bytes_reclaimed_total Bytes reclaimed during hard-delete per tier\n\
             # TYPE nodedb_collection_purge_bytes_reclaimed_total counter\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for ((tenant, engine, tier), counter) in pairs {
            let v = counter.load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                r#"nodedb_collection_purge_bytes_reclaimed_total{{tenant="{tenant}",engine="{engine}",tier="{tier}"}} {v}"#
            );
        }
    }

    fn write_l2_queue(&self, out: &mut String) {
        let m = self
            .l2_cleanup_queue_depth
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_l2_cleanup_queue_depth Pending S3 delete operations per tenant\n\
             # TYPE nodedb_l2_cleanup_queue_depth gauge\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by_key(|(t, _)| *t);
        for (tenant, depth) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_l2_cleanup_queue_depth{{tenant="{tenant}"}} {depth}"#
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_renders_per_tenant() {
        let m = PurgeMetrics::new();
        let mut snap = HashMap::new();
        snap.insert(1_u32, 3);
        snap.insert(7_u32, 0);
        m.set_pending_by_tenant(snap);
        let mut out = String::new();
        m.write_prometheus(&mut out);
        assert!(out.contains(r#"nodedb_deactivated_collections_pending_purge{tenant="1"} 3"#));
        assert!(out.contains(r#"nodedb_deactivated_collections_pending_purge{tenant="7"} 0"#));
    }

    #[test]
    fn bytes_reclaimed_accumulates_per_tier() {
        let m = PurgeMetrics::new();
        m.add_bytes_reclaimed(2, "columnar", "l1", 1_000);
        m.add_bytes_reclaimed(2, "columnar", "l1", 500);
        m.add_bytes_reclaimed(2, "columnar", "l2", 9_000);
        let mut out = String::new();
        m.write_prometheus(&mut out);
        assert!(out.contains(
            r#"nodedb_collection_purge_bytes_reclaimed_total{tenant="2",engine="columnar",tier="l1"} 1500"#
        ));
        assert!(out.contains(
            r#"nodedb_collection_purge_bytes_reclaimed_total{tenant="2",engine="columnar",tier="l2"} 9000"#
        ));
    }

    #[test]
    fn l2_queue_gauge_renders() {
        let m = PurgeMetrics::new();
        let mut snap = HashMap::new();
        snap.insert(4_u32, 17);
        m.set_l2_cleanup_queue_depth(snap);
        let mut out = String::new();
        m.write_prometheus(&mut out);
        assert!(out.contains(r#"nodedb_l2_cleanup_queue_depth{tenant="4"} 17"#));
    }

    #[test]
    fn empty_renders_nothing() {
        let m = PurgeMetrics::new();
        let mut out = String::new();
        m.write_prometheus(&mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn duration_histogram_emits() {
        let m = PurgeMetrics::new();
        m.record_purge_duration(1, "document", 12_000);
        m.record_purge_duration(1, "document", 34_000);
        let mut out = String::new();
        m.write_prometheus(&mut out);
        assert!(out.contains(r#"tenant="1""#));
        assert!(out.contains(r#"engine="document""#));
    }
}
