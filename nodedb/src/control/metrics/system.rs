//! Core system metrics collected across all NodeDB subsystems.
//!
//! All fields are atomic — safe for concurrent reads/writes from
//! Control Plane, Data Plane handlers, and the HTTP metrics endpoint.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::histogram::{AtomicHistogram, WAL_FSYNC_BUCKETS_US};
use super::purge::PurgeMetrics;
use crate::data::io::IoMetrics;

/// Core metrics collected across the system.
#[derive(Debug, Default)]
pub struct SystemMetrics {
    // ── WAL ──
    pub wal_fsync_seconds: AtomicHistogram,
    pub wal_fsync_count: AtomicU64,
    pub wal_segment_count: AtomicU64,
    pub wal_segment_bytes: AtomicU64,

    // ── Raft / replication ──
    pub raft_apply_lag: AtomicU64,
    pub raft_commit_index: AtomicU64,
    pub raft_applied_index: AtomicU64,
    pub raft_leader_term: AtomicU64,
    pub raft_snapshot_count: AtomicU64,
    pub vshard_migrations_active: AtomicU64,

    // ── Bridge ──
    pub bridge_utilization: AtomicU64,

    // ── Compaction ──
    pub compaction_debt: AtomicU64,
    pub compaction_cycles: AtomicU64,
    pub compaction_bytes_total: AtomicU64,

    // ── Auth ──
    pub auth_failures: AtomicU64,
    pub auth_successes: AtomicU64,

    // ── Connections ──
    pub active_connections: AtomicU64,
    pub pgwire_connections: AtomicU64,
    pub http_connections: AtomicU64,
    pub native_connections: AtomicU64,
    pub websocket_connections: AtomicU64,
    pub ilp_connections: AtomicU64,

    // ── Queries ──
    pub queries_total: AtomicU64,
    pub query_errors: AtomicU64,
    pub slow_queries_total: AtomicU64,
    pub query_planning_seconds: AtomicHistogram,
    pub query_execution_seconds: AtomicHistogram,
    pub query_latency: AtomicHistogram,

    // ── Per-engine operations ──
    pub vector_searches: AtomicU64,
    pub vector_collections: AtomicU64,
    pub vector_vectors_stored: AtomicU64,
    pub vector_query_seconds: AtomicHistogram,

    pub graph_traversals: AtomicU64,
    pub graph_nodes: AtomicU64,
    pub graph_edges: AtomicU64,

    pub document_inserts: AtomicU64,
    pub document_reads: AtomicU64,
    pub document_collections: AtomicU64,
    /// Count of `DocumentOp::BackfillIndex` handler invocations on this
    /// node's Data Plane. Per-node: every node increments its own
    /// counter exactly when its local core runs the backfill primitive.
    /// Tests assert per-node fan-out for distributed CREATE INDEX by
    /// reading this counter on every node after DDL completion.
    pub document_index_backfills: AtomicU64,

    pub columnar_segments: AtomicU64,
    pub columnar_compaction_queue: AtomicU64,
    pub columnar_compression_ratio: AtomicU64, // stored as ratio × 100

    pub fts_searches: AtomicU64,
    pub fts_indexes: AtomicU64,
    pub fts_query_seconds: AtomicHistogram,

    // ── KV engine ──
    pub kv_gets_total: AtomicU64,
    pub kv_puts_total: AtomicU64,
    pub kv_deletes_total: AtomicU64,
    pub kv_scans_total: AtomicU64,
    pub kv_expiries_total: AtomicU64,
    pub kv_memory_bytes: AtomicU64,
    pub kv_total_keys: AtomicU64,

    // ── Per-engine query counts ──
    pub queries_vector: AtomicU64,
    pub queries_graph: AtomicU64,
    pub queries_document: AtomicU64,
    pub queries_columnar: AtomicU64,
    pub queries_kv: AtomicU64,
    pub queries_fts: AtomicU64,

    // ── Data Plane (aggregate across all cores) ──
    pub io_uring_submissions: AtomicU64,
    pub io_uring_completions: AtomicU64,
    pub tpc_utilization_ratio: AtomicU64,
    pub arena_memory_bytes: AtomicU64,

    // ── Contention ──
    pub mmap_major_faults: AtomicU64,
    pub nvme_queue_depth: AtomicU64,
    pub throttle_activations: AtomicU64,
    pub cache_contention_events: AtomicU64,

    // ── Storage tiers ──
    pub storage_l0_bytes: AtomicU64,
    pub storage_l1_bytes: AtomicU64,
    pub storage_l2_bytes: AtomicU64,
    pub mmap_rss_bytes: AtomicU64,

    // ── Subscriptions ──
    pub active_subscriptions: AtomicU64,
    pub active_listen_channels: AtomicU64,
    pub change_events_delivered: AtomicU64,
    /// Global CDC drop counter (sum across all streams). Kept for backward
    /// compatibility with existing dashboards that query this name without labels.
    pub change_events_dropped: AtomicU64,
    /// Per-stream CDC drop counters. Key: `(tenant_id, stream_name)`.
    /// Rendered as `nodedb_cdc_events_dropped_total{tenant="<id>",stream="<name>"}`.
    pub cdc_events_dropped_by_stream: RwLock<HashMap<(u64, String), u64>>,

    // ── Backpressure ──
    /// Per-engine Critical-pressure fire count.
    /// Key: engine name string (e.g. "vector", "columnar").
    /// Rendered as `nodedb_backpressure_critical_total{engine="..."}`.
    pub backpressure_critical_by_engine: RwLock<HashMap<String, u64>>,
    /// Per-engine Emergency-pressure fire count.
    /// Key: engine name string.
    /// Rendered as `nodedb_backpressure_emergency_total{engine="..."}`.
    pub backpressure_emergency_by_engine: RwLock<HashMap<String, u64>>,

    // ── Checkpoints ──
    pub checkpoints: AtomicU64,

    // ── Catalog sanity check ──
    /// Labeled counter: (registry, outcome) → total.
    /// `outcome` is one of "ok", "warning", "error".
    pub catalog_sanity_check_totals: RwLock<HashMap<(String, String), u64>>,

    // ── Collection hard-delete (purge) ──
    pub purge: PurgeMetrics,

    // ── Shutdown ──
    /// Gauge: phase name → last observed drain duration in milliseconds.
    /// Updated once per phase transition during graceful shutdown.
    pub shutdown_phase_durations_ms: RwLock<HashMap<String, u64>>,

    // ── IO priority scheduler ──
    /// Per-priority IO queue-depth and wait-latency metrics.
    ///
    /// Shared `Arc` is cloned into each `CoreLoop` at startup so the Data
    /// Plane can update counters without crossing the plane boundary.
    /// The Prometheus handler reads from here.
    pub io_metrics: Arc<IoMetrics>,
}

impl SystemMetrics {
    pub fn new() -> Self {
        // WAL fsync latency uses sub-millisecond buckets (100µs–1s range).
        Self {
            wal_fsync_seconds: AtomicHistogram::with_buckets(WAL_FSYNC_BUCKETS_US),
            ..Self::default()
        }
    }

    // ── WAL ──

    pub fn record_wal_fsync(&self, duration_us: u64) {
        self.wal_fsync_seconds.observe(duration_us);
        self.wal_fsync_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_wal_segments(&self, count: u64, bytes: u64) {
        self.wal_segment_count.store(count, Ordering::Relaxed);
        self.wal_segment_bytes.store(bytes, Ordering::Relaxed);
    }

    // ── Replication ──

    pub fn record_raft_lag(&self, lag: u64) {
        self.raft_apply_lag.store(lag, Ordering::Relaxed);
    }

    pub fn update_raft_state(&self, commit_idx: u64, applied_idx: u64, term: u64) {
        self.raft_commit_index.store(commit_idx, Ordering::Relaxed);
        self.raft_applied_index
            .store(applied_idx, Ordering::Relaxed);
        self.raft_leader_term.store(term, Ordering::Relaxed);
    }

    pub fn record_raft_snapshot(&self) {
        self.raft_snapshot_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_vshard_migrations(&self, active: u64) {
        self.vshard_migrations_active
            .store(active, Ordering::Relaxed);
    }

    // ── Bridge ──

    pub fn record_bridge_utilization(&self, pct: u64) {
        self.bridge_utilization.store(pct, Ordering::Relaxed);
    }

    // ── Compaction ──

    pub fn update_compaction(&self, debt: u64, bytes_written: u64) {
        self.compaction_debt.store(debt, Ordering::Relaxed);
        self.compaction_bytes_total
            .fetch_add(bytes_written, Ordering::Relaxed);
    }

    pub fn record_compaction_cycle(&self) {
        self.compaction_cycles.fetch_add(1, Ordering::Relaxed);
    }

    // ── Auth ──

    pub fn record_auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_auth_success(&self) {
        self.auth_successes.fetch_add(1, Ordering::Relaxed);
    }

    // ── Connections ──

    pub fn update_connections(
        &self,
        pgwire: u64,
        http: u64,
        native: u64,
        websocket: u64,
        ilp: u64,
    ) {
        self.pgwire_connections.store(pgwire, Ordering::Relaxed);
        self.http_connections.store(http, Ordering::Relaxed);
        self.native_connections.store(native, Ordering::Relaxed);
        self.websocket_connections
            .store(websocket, Ordering::Relaxed);
        self.ilp_connections.store(ilp, Ordering::Relaxed);
        self.active_connections
            .store(pgwire + http + native + websocket + ilp, Ordering::Relaxed);
    }

    pub fn inc_pgwire_connections(&self) {
        self.pgwire_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_pgwire_connections(&self) {
        self.pgwire_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_http_connections(&self) {
        self.http_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_http_connections(&self) {
        self.http_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_websocket_connections(&self) {
        self.websocket_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_websocket_connections(&self) {
        self.websocket_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_ilp_connections(&self) {
        self.ilp_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ilp_connections(&self) {
        self.ilp_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    // ── Queries ──

    pub fn record_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_error(&self) {
        self.query_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_latency(&self, latency_us: u64) {
        self.query_latency.observe(latency_us);
        if latency_us > 100_000 {
            self.slow_queries_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_query_timing(&self, planning_us: u64, execution_us: u64) {
        self.query_planning_seconds.observe(planning_us);
        self.query_execution_seconds.observe(execution_us);
    }

    pub fn record_query_by_engine(&self, engine: &str) {
        match engine {
            "vector" => self.queries_vector.fetch_add(1, Ordering::Relaxed),
            "graph" => self.queries_graph.fetch_add(1, Ordering::Relaxed),
            "document_schemaless" | "document_strict" => {
                self.queries_document.fetch_add(1, Ordering::Relaxed)
            }
            "columnar" => self.queries_columnar.fetch_add(1, Ordering::Relaxed),
            "kv" => self.queries_kv.fetch_add(1, Ordering::Relaxed),
            "fts" => self.queries_fts.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    // ── Vector engine ──

    pub fn record_vector_search(&self, latency_us: u64) {
        self.vector_searches.fetch_add(1, Ordering::Relaxed);
        self.vector_query_seconds.observe(latency_us);
    }

    pub fn update_vector_stats(&self, collections: u64, vectors: u64) {
        self.vector_collections
            .store(collections, Ordering::Relaxed);
        self.vector_vectors_stored.store(vectors, Ordering::Relaxed);
    }

    // ── Graph engine ──

    pub fn record_graph_traversal(&self) {
        self.graph_traversals.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_graph_stats(&self, nodes: u64, edges: u64) {
        self.graph_nodes.store(nodes, Ordering::Relaxed);
        self.graph_edges.store(edges, Ordering::Relaxed);
    }

    // ── Document engine ──

    pub fn record_document_insert(&self) {
        self.document_inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_document_read(&self) {
        self.document_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_document_index_backfill(&self) {
        self.document_index_backfills
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_document_collections(&self, count: u64) {
        self.document_collections.store(count, Ordering::Relaxed);
    }

    // ── Columnar engine ──

    pub fn update_columnar_stats(&self, segments: u64, compaction_queue: u64, ratio_x100: u64) {
        self.columnar_segments.store(segments, Ordering::Relaxed);
        self.columnar_compaction_queue
            .store(compaction_queue, Ordering::Relaxed);
        self.columnar_compression_ratio
            .store(ratio_x100, Ordering::Relaxed);
    }

    // ── FTS engine ──

    pub fn record_fts_search(&self, latency_us: u64) {
        self.fts_searches.fetch_add(1, Ordering::Relaxed);
        self.fts_query_seconds.observe(latency_us);
    }

    pub fn update_fts_indexes(&self, count: u64) {
        self.fts_indexes.store(count, Ordering::Relaxed);
    }

    // ── KV engine ──

    pub fn record_kv_get(&self) {
        self.kv_gets_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_put(&self) {
        self.kv_puts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_delete(&self) {
        self.kv_deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_scan(&self) {
        self.kv_scans_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_expiries(&self, count: u64) {
        self.kv_expiries_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn update_kv_memory(&self, bytes: u64) {
        self.kv_memory_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn update_kv_keys(&self, count: u64) {
        self.kv_total_keys.store(count, Ordering::Relaxed);
    }

    // ── Data Plane ──

    pub fn record_io_uring_submission(&self) {
        self.io_uring_submissions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_io_uring_completion(&self) {
        self.io_uring_completions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_tpc_utilization(&self, pct: u64) {
        self.tpc_utilization_ratio.store(pct, Ordering::Relaxed);
    }

    pub fn update_arena_memory(&self, bytes: u64) {
        self.arena_memory_bytes.store(bytes, Ordering::Relaxed);
    }

    // ── Contention ──

    pub fn record_mmap_fault(&self) {
        self.mmap_major_faults.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_throttle(&self) {
        self.throttle_activations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_contention(&self) {
        self.cache_contention_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_nvme_queue_depth(&self, depth: u64) {
        self.nvme_queue_depth.store(depth, Ordering::Relaxed);
    }

    // ── Storage tiers ──

    pub fn update_storage_tiers(&self, l0: u64, l1: u64, l2: u64) {
        self.storage_l0_bytes.store(l0, Ordering::Relaxed);
        self.storage_l1_bytes.store(l1, Ordering::Relaxed);
        self.storage_l2_bytes.store(l2, Ordering::Relaxed);
    }

    pub fn update_mmap_rss(&self, bytes: u64) {
        self.mmap_rss_bytes.store(bytes, Ordering::Relaxed);
    }

    // ── CDC per-stream drops ──

    /// Record `count` events evicted from a specific CDC stream buffer.
    ///
    /// Increments both the global `change_events_dropped` counter (backward
    /// compatibility) and the per-stream labelled map used for Prometheus
    /// `nodedb_cdc_events_dropped_total{tenant, stream}`.
    pub fn record_cdc_stream_drop(&self, tenant_id: u64, stream_name: &str, count: u64) {
        self.change_events_dropped
            .fetch_add(count, Ordering::Relaxed);
        let mut m = self
            .cdc_events_dropped_by_stream
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry((tenant_id, stream_name.to_string())).or_insert(0) += count;
    }

    // ── Catalog sanity check ──

    /// Record the outcome of one registry's catalog sanity check.
    ///
    /// `outcome` must be `"ok"`, `"warning"`, or `"error"`.
    pub fn record_catalog_sanity_check(&self, registry: &str, outcome: &str) {
        let mut m = self
            .catalog_sanity_check_totals
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry((registry.to_string(), outcome.to_string()))
            .or_insert(0) += 1;
    }

    /// Record the duration of a single shutdown phase.
    ///
    /// Called by `ShutdownBus::initiate()` after each phase drains.
    /// The value is overwritten on each shutdown so `/metrics` always
    /// shows the most recent run.
    pub fn record_shutdown_phase_duration(&self, phase: &str, duration_ms: u64) {
        let mut m = self
            .shutdown_phase_durations_ms
            .write()
            .unwrap_or_else(|p| p.into_inner());
        m.insert(phase.to_string(), duration_ms);
    }

    /// Increment the Critical-pressure counter for the given engine.
    ///
    /// Called by the Data Plane pressure check on every Critical-branch fire.
    pub fn record_backpressure_critical(&self, engine: &str) {
        let mut m = self
            .backpressure_critical_by_engine
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry(engine.to_string()).or_insert(0) += 1;
    }

    /// Increment the Emergency-pressure counter for the given engine.
    ///
    /// Called by the Data Plane pressure check on every Emergency-branch fire.
    pub fn record_backpressure_emergency(&self, engine: &str) {
        let mut m = self
            .backpressure_emergency_by_engine
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry(engine.to_string()).or_insert(0) += 1;
    }

    /// Emit `nodedb_backpressure_critical_total{engine}` and
    /// `nodedb_backpressure_emergency_total{engine}` counters.
    fn prometheus_backpressure(&self, out: &mut String) {
        use std::fmt::Write as _;
        let critical = self
            .backpressure_critical_by_engine
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let emergency = self
            .backpressure_emergency_by_engine
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if !critical.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_backpressure_critical_total Write handlers that entered Critical-pressure flush path\n\
                 # TYPE nodedb_backpressure_critical_total counter\n",
            );
            let mut pairs: Vec<_> = critical.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (engine, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_backpressure_critical_total{{engine="{engine}"}} {count}"#
                );
            }
        }
        if !emergency.is_empty() {
            let _ = out.write_str(
                "# HELP nodedb_backpressure_emergency_total Write handlers rejected by Emergency-pressure\n\
                 # TYPE nodedb_backpressure_emergency_total counter\n",
            );
            let mut pairs: Vec<_> = emergency.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            for (engine, count) in pairs {
                let _ = writeln!(
                    out,
                    r#"nodedb_backpressure_emergency_total{{engine="{engine}"}} {count}"#
                );
            }
        }
    }

    /// Serialize all metrics as Prometheus text format 0.0.4.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(8192);
        self.prometheus_core(&mut out);
        self.prometheus_engines(&mut out);
        self.prometheus_catalog_sanity(&mut out);
        self.prometheus_shutdown_phases(&mut out);
        self.prometheus_cdc_stream_drops(&mut out);
        self.prometheus_backpressure(&mut out);
        self.purge.write_prometheus(&mut out);
        self.io_metrics.write_prometheus(&mut out);
        out
    }

    /// Emit `nodedb_cdc_events_dropped_total{tenant,stream}` labelled counters.
    fn prometheus_cdc_stream_drops(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .cdc_events_dropped_by_stream
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_cdc_events_dropped_total CDC events dropped from stream buffers due to overflow\n\
             # TYPE nodedb_cdc_events_dropped_total counter\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for ((tenant_id, stream_name), count) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_cdc_events_dropped_total{{tenant="{tenant_id}",stream="{stream_name}"}} {count}"#
            );
        }
    }

    /// Emit `nodedb_shutdown_phase_duration_seconds{phase}` gauges.
    fn prometheus_shutdown_phases(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .shutdown_phase_durations_ms
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_shutdown_phase_duration_seconds Duration of each shutdown phase in the last graceful shutdown\n\
             # TYPE nodedb_shutdown_phase_duration_seconds gauge\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (phase, ms) in pairs {
            let secs = *ms as f64 / 1_000.0;
            let _ = writeln!(
                out,
                r#"nodedb_shutdown_phase_duration_seconds{{phase="{phase}"}} {secs}"#
            );
        }
    }

    /// Emit `nodedb_catalog_sanity_check_total{registry,outcome}` labeled counters.
    fn prometheus_catalog_sanity(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .catalog_sanity_check_totals
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP nodedb_catalog_sanity_check_total Catalog sanity check outcomes per registry\n\
             # TYPE nodedb_catalog_sanity_check_total counter\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for ((registry, outcome), count) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_catalog_sanity_check_total{{registry="{registry}",outcome="{outcome}"}} {count}"#
            );
        }
    }

    /// Emit `nodedb_segments_quarantined_total{engine,collection}` counters and
    /// `nodedb_segments_quarantined_active{engine,collection}` gauges from a
    /// live registry snapshot.
    ///
    /// Called from the `/metrics` HTTP handler which has direct access to
    /// `SharedState::quarantine_registry`. The `SystemMetrics` struct does not
    /// hold a quarantine counter to avoid requiring a notification path between
    /// the registry and the metrics store — the registry is the source of truth.
    pub fn prometheus_segment_quarantine_active(
        out: &mut String,
        active_counts: &std::collections::HashMap<(String, String), u64>,
    ) {
        use std::fmt::Write as _;
        if active_counts.is_empty() {
            return;
        }
        let mut pairs: Vec<_> = active_counts.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let _ = out.write_str(
            "# HELP nodedb_segments_quarantined_active Currently-quarantined segment count per engine and collection\n\
             # TYPE nodedb_segments_quarantined_active gauge\n",
        );
        for ((engine, collection), count) in &pairs {
            let _ = writeln!(
                out,
                r#"nodedb_segments_quarantined_active{{engine="{engine}",collection="{collection}"}} {count}"#
            );
        }
        // Emit total (same value per process run — quarantines are permanent within a run).
        let _ = out.write_str(
            "# HELP nodedb_segments_quarantined_total Cumulative segments quarantined due to repeated CRC failures\n\
             # TYPE nodedb_segments_quarantined_total counter\n",
        );
        for ((engine, collection), count) in pairs {
            let _ = writeln!(
                out,
                r#"nodedb_segments_quarantined_total{{engine="{engine}",collection="{collection}"}} {count}"#
            );
        }
    }
}
