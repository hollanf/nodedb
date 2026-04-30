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
//!   `StoredCollection::collection_type.as_str()` (e.g. `"document_schemaless"`,
//!   `"document_strict"`, `"columnar"`, `"timeseries"`, `"spatial"`, `"kv"`).
//! - `tier` — `"l1"` or `"l2"` for reclaim accounting.
//!
//! # Cardinality bound
//!
//! All per-tenant label sets are capped at `MAX_PROM_TENANTS` (256)
//! distinct values at emission time. Tenants beyond the cap (ranked by
//! descending metric value) are aggregated into a single
//! `tenant="__overflow__"` series. This prevents unbounded label
//! cardinality in SaaS deployments where tenant count can reach
//! hundreds of thousands.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use super::histogram::AtomicHistogram;

/// Maximum number of distinct tenant label values emitted per metric.
/// Tenants beyond this cap are aggregated into `TENANT_OVERFLOW_BUCKET`.
/// Cardinality bound for SaaS deployments where tenant count is unbounded.
const MAX_PROM_TENANTS: usize = 256;

/// Prometheus label value used for all tenants that exceed the cap.
const TENANT_OVERFLOW_BUCKET: &str = "__overflow__";

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
        let rows = cap_simple_tenant_map(&m);
        for (label, count) in rows {
            let _ = writeln!(
                out,
                r#"nodedb_deactivated_collections_pending_purge{{tenant="{label}"}} {count}"#
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
        // Collect (tenant_id, engine) -> sum-of-counts for ranking, then
        // apply the cap.  We keep the top-256 tenants (by total histogram
        // count) individually and merge the rest into __overflow__.
        let rows = cap_duration_tenant_map(&m);
        for (label, engine, hist) in rows {
            let name = format!(
                r#"nodedb_collection_purge_duration_seconds{{tenant="{label}",engine="{engine}"}}"#
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
        let rows = cap_bytes_tenant_map(&m);
        for (label, engine, tier, v) in rows {
            let _ = writeln!(
                out,
                r#"nodedb_collection_purge_bytes_reclaimed_total{{tenant="{label}",engine="{engine}",tier="{tier}"}} {v}"#
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
        let rows = cap_simple_tenant_map(&m);
        for (label, depth) in rows {
            let _ = writeln!(
                out,
                r#"nodedb_l2_cleanup_queue_depth{{tenant="{label}"}} {depth}"#
            );
        }
    }
}

// ── Tenant-cap helpers ───────────────────────────────────────────────────────

/// Cap a `tenant_id -> u64` map to `MAX_PROM_TENANTS` individual rows.
///
/// Returns rows sorted by tenant label ascending (individual tenants use
/// their numeric id as the label; the overflow bucket, if present, is last).
/// Tenants beyond the cap are ranked by descending value; the bottom ones
/// are summed into a single `__overflow__` row.
fn cap_simple_tenant_map(map: &HashMap<u32, u64>) -> Vec<(String, u64)> {
    if map.len() <= MAX_PROM_TENANTS {
        let mut rows: Vec<(String, u64)> = map.iter().map(|(t, v)| (t.to_string(), *v)).collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        return rows;
    }

    // Sort by descending value so the most-significant tenants are kept.
    let mut sorted: Vec<(u32, u64)> = map.iter().map(|(t, v)| (*t, *v)).collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    let mut rows: Vec<(String, u64)> = sorted[..MAX_PROM_TENANTS]
        .iter()
        .map(|(t, v)| (t.to_string(), *v))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let overflow: u64 = sorted[MAX_PROM_TENANTS..].iter().map(|(_, v)| v).sum();
    rows.push((TENANT_OVERFLOW_BUCKET.to_string(), overflow));
    rows
}

/// Cap a `(tenant_id, engine) -> AtomicHistogram` map to `MAX_PROM_TENANTS`
/// distinct tenants.
///
/// Tenants beyond the cap (ranked by descending total histogram count) are
/// merged into a single `__overflow__` histogram per engine.
/// Returns `(tenant_label, engine, merged_histogram)` triples, sorted by
/// `(tenant_label, engine)`.
fn cap_duration_tenant_map(
    map: &HashMap<(u32, String), AtomicHistogram>,
) -> Vec<(String, String, AtomicHistogram)> {
    // Collect distinct tenant ids and their aggregate counts for ranking.
    let mut tenant_counts: HashMap<u32, u64> = HashMap::new();
    for ((tenant, _), hist) in map.iter() {
        *tenant_counts.entry(*tenant).or_default() += hist.count();
    }

    let kept_tenants: std::collections::HashSet<u32> = if tenant_counts.len() <= MAX_PROM_TENANTS {
        tenant_counts.keys().copied().collect()
    } else {
        let mut ranked: Vec<(u32, u64)> = tenant_counts.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        ranked[..MAX_PROM_TENANTS].iter().map(|(t, _)| *t).collect()
    };

    // Gather kept rows and overflow rows (per engine).
    let mut kept: Vec<(String, String, AtomicHistogram)> = Vec::new();
    let mut overflow_by_engine: HashMap<String, AtomicHistogram> = HashMap::new();

    for ((tenant, engine), hist) in map.iter() {
        if kept_tenants.contains(tenant) {
            kept.push((tenant.to_string(), engine.clone(), hist.snapshot()));
        } else {
            overflow_by_engine
                .entry(engine.clone())
                .or_default()
                .merge(hist);
        }
    }

    for (engine, hist) in overflow_by_engine {
        kept.push((TENANT_OVERFLOW_BUCKET.to_string(), engine, hist));
    }

    kept.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    kept
}

/// Cap a `(tenant_id, engine, tier) -> AtomicU64` map to `MAX_PROM_TENANTS`
/// distinct tenants.
///
/// Tenants beyond the cap are aggregated into `__overflow__` per
/// `(engine, tier)` combination. Returns `(label, engine, tier, value)`
/// quads sorted by `(label, engine, tier)`.
fn cap_bytes_tenant_map(
    map: &HashMap<(u32, String, &'static str), AtomicU64>,
) -> Vec<(String, String, &'static str, u64)> {
    // Rank tenants by their total bytes across all (engine, tier) combos.
    let mut tenant_totals: HashMap<u32, u64> = HashMap::new();
    for ((tenant, _, _), counter) in map.iter() {
        *tenant_totals.entry(*tenant).or_default() += counter.load(Ordering::Relaxed);
    }

    let kept_tenants: std::collections::HashSet<u32> = if tenant_totals.len() <= MAX_PROM_TENANTS {
        tenant_totals.keys().copied().collect()
    } else {
        let mut ranked: Vec<(u32, u64)> = tenant_totals.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        ranked[..MAX_PROM_TENANTS].iter().map(|(t, _)| *t).collect()
    };

    let mut kept: Vec<(String, String, &'static str, u64)> = Vec::new();
    let mut overflow: HashMap<(String, &'static str), u64> = HashMap::new();

    for ((tenant, engine, tier), counter) in map.iter() {
        let v = counter.load(Ordering::Relaxed);
        if kept_tenants.contains(tenant) {
            kept.push((tenant.to_string(), engine.clone(), tier, v));
        } else {
            *overflow.entry((engine.clone(), tier)).or_default() += v;
        }
    }

    for ((engine, tier), v) in overflow {
        kept.push((TENANT_OVERFLOW_BUCKET.to_string(), engine, tier, v));
    }

    kept.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(b.2)));
    kept
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
        m.record_purge_duration(1, "document_schemaless", 12_000);
        m.record_purge_duration(1, "document_schemaless", 34_000);
        let mut out = String::new();
        m.write_prometheus(&mut out);
        assert!(out.contains(r#"tenant="1""#));
        assert!(out.contains(r#"engine="document_schemaless""#));
    }

    // ── Cardinality-cap tests ────────────────────────────────────────────────

    #[test]
    fn pending_under_cap_emits_all_rows() {
        let m = PurgeMetrics::new();
        // Insert exactly MAX_PROM_TENANTS tenants — all should appear.
        let snap: HashMap<u32, u64> = (0..MAX_PROM_TENANTS as u32)
            .map(|i| (i, i as u64))
            .collect();
        m.set_pending_by_tenant(snap);
        let mut out = String::new();
        m.write_prometheus(&mut out);
        // Count individual tenant rows (not the overflow bucket).
        let individual = out
            .lines()
            .filter(|l| {
                l.starts_with("nodedb_deactivated_collections_pending_purge")
                    && !l.contains(TENANT_OVERFLOW_BUCKET)
                    && !l.starts_with('#')
            })
            .count();
        assert_eq!(individual, MAX_PROM_TENANTS);
        assert!(!out.contains(TENANT_OVERFLOW_BUCKET));
    }

    #[test]
    fn pending_over_cap_produces_overflow_row() {
        let m = PurgeMetrics::new();
        // 300 tenants: tenant 0..299 each with value equal to their id + 1
        // so that value sums are predictable.
        let total = 300_u32;
        let snap: HashMap<u32, u64> = (0..total).map(|i| (i, i as u64 + 1)).collect();
        m.set_pending_by_tenant(snap);
        let mut out = String::new();
        m.write_prometheus(&mut out);

        // Count individual rows.
        let individual: Vec<&str> = out
            .lines()
            .filter(|l| {
                l.starts_with("nodedb_deactivated_collections_pending_purge")
                    && !l.contains(TENANT_OVERFLOW_BUCKET)
                    && !l.starts_with('#')
            })
            .collect();
        assert_eq!(
            individual.len(),
            MAX_PROM_TENANTS,
            "expected 256 individual rows"
        );

        // Overflow row must exist.
        let overflow_line = out
            .lines()
            .find(|l| {
                l.starts_with("nodedb_deactivated_collections_pending_purge")
                    && l.contains(TENANT_OVERFLOW_BUCKET)
            })
            .expect("overflow row missing");

        // The top 256 tenants by descending value are tenants 44..299 (values 45..300).
        // The 44 dropped tenants are 0..43 (values 1..44). Their sum = 44*45/2 = 990.
        let expected_overflow: u64 = (1..=44).sum();
        assert!(
            overflow_line.ends_with(&format!(" {expected_overflow}")),
            "overflow value wrong: {overflow_line}"
        );
    }

    #[test]
    fn overflow_value_equals_sum_of_dropped_tenants() {
        // Verify via cap_simple_tenant_map directly with known data.
        // 10 tenants, cap = 3 for this logic check (we test the real cap
        // via the PurgeMetrics path above; here we check the arithmetic).
        // We can't change MAX_PROM_TENANTS, but we can pick 300 tenants
        // with known values and check the overflow value matches exactly.
        let total: u32 = 300;
        let map: HashMap<u32, u64> = (0..total).map(|i| (i, i as u64 + 1)).collect();
        let rows = cap_simple_tenant_map(&map);

        // Exactly 256 individual + 1 overflow.
        assert_eq!(rows.len(), MAX_PROM_TENANTS + 1);

        let overflow_row = rows
            .iter()
            .find(|(label, _)| label == TENANT_OVERFLOW_BUCKET)
            .expect("no overflow row");

        // Top 256 tenants by value are tenants 43..299 (values 44..300).
        // Dropped: tenants 0..43 with values 1..44. Sum = 44*45/2 = 990.
        let expected: u64 = (1..=44).sum();
        assert_eq!(overflow_row.1, expected);
    }
}
