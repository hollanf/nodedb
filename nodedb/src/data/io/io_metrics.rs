//! Per-priority IO metrics for the io_uring submission path.
//!
//! Tracks queue depth and submission-to-completion latency separately for
//! each priority tier: Low (Background+Normal), High, and Critical.
//!
//! Surfaced as:
//! - `nodedb_io_queue_depth{priority}` — gauge, pending task count per tier
//! - `nodedb_io_wait_ns{priority}` — histogram, submission-to-completion
//!   latency in nanoseconds (buckets in seconds per Prometheus convention)
//!
//! All fields are atomic — safe to read from the Control Plane (Prometheus
//! handler) while the Data Plane writes them.

use std::sync::atomic::{AtomicU64, Ordering};

/// Priority tier index constants (used as array indices throughout).
pub const TIER_LOW: usize = 0;
pub const TIER_HIGH: usize = 1;
pub const TIER_CRITICAL: usize = 2;

/// Human-readable label for each tier (matches Prometheus `priority=` label).
pub const TIER_LABELS: [&str; 3] = ["low", "high", "critical"];

/// IO wait bucket upper bounds in **nanoseconds**.
///
/// Covers 1µs–1s — wide enough for NVMe fast-path (10–100µs) through
/// degraded paths seen under sustained write load.
const IO_WAIT_BUCKETS_NS: [u64; 13] = [
    1_000,         // 1µs
    5_000,         // 5µs
    10_000,        // 10µs
    50_000,        // 50µs
    100_000,       // 100µs
    500_000,       // 500µs
    1_000_000,     // 1ms
    5_000_000,     // 5ms
    10_000_000,    // 10ms
    50_000_000,    // 50ms
    100_000_000,   // 100ms
    500_000_000,   // 500ms
    1_000_000_000, // 1s
];

const BUCKET_COUNT: usize = IO_WAIT_BUCKETS_NS.len();

/// Per-priority IO metrics.
///
/// `Send + Sync` — all fields are atomics.
#[derive(Debug)]
pub struct IoMetrics {
    /// Pending-task queue depth per priority tier (gauge).
    /// Updated each tick after the priority queues are drained.
    pub queue_depth: [AtomicU64; 3],

    /// Per-bucket observation counts, indexed `[tier][bucket]`.
    /// `buckets[tier][i]` counts observations `<= IO_WAIT_BUCKETS_NS[i]`.
    buckets: [[AtomicU64; BUCKET_COUNT]; 3],

    /// Total observation count per tier.
    count: [AtomicU64; 3],

    /// Sum of all observed wait values (ns) per tier.
    sum_ns: [AtomicU64; 3],
}

impl IoMetrics {
    /// Create a zeroed `IoMetrics`.
    pub fn new() -> Self {
        Self {
            queue_depth: std::array::from_fn(|_| AtomicU64::new(0)),
            buckets: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            count: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ns: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Update the queue-depth gauge for a single tier.
    ///
    /// Call this each `tick()` after draining all priority queues.
    #[inline]
    pub fn record_queue_depth(&self, tier: usize, depth: u64) {
        self.queue_depth[tier].store(depth, Ordering::Relaxed);
    }

    /// Record one completed IO submission for `tier` with wait time `wait_ns`.
    ///
    /// `wait_ns` is elapsed nanoseconds from task-enqueue to read_files return.
    #[inline]
    pub fn record_wait(&self, tier: usize, wait_ns: u64) {
        self.count[tier].fetch_add(1, Ordering::Relaxed);
        self.sum_ns[tier].fetch_add(wait_ns, Ordering::Relaxed);
        for (i, &boundary) in IO_WAIT_BUCKETS_NS.iter().enumerate() {
            if wait_ns <= boundary {
                self.buckets[tier][i].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // Overflow: beyond all buckets — counted in `count` but no bucket.
    }

    /// Emit Prometheus text for all IO metrics.
    ///
    /// Produces:
    /// - `nodedb_io_queue_depth{priority=...}` gauges
    /// - `nodedb_io_wait_ns{priority=...}` histograms
    pub fn write_prometheus(&self, out: &mut String) {
        use std::fmt::Write as _;

        // ── Queue depth gauges ──
        let _ = out.write_str(
            "# HELP nodedb_io_queue_depth Pending IO task count per priority tier\n\
             # TYPE nodedb_io_queue_depth gauge\n",
        );
        for (tier, label) in TIER_LABELS.iter().enumerate() {
            let depth = self.queue_depth[tier].load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                r#"nodedb_io_queue_depth{{priority="{label}"}} {depth}"#
            );
        }

        // ── IO wait histograms ──
        // One histogram block per tier, le= values in seconds (Prometheus convention).
        let _ = out.write_str(
            "# HELP nodedb_io_wait_ns IO submission-to-completion latency per priority tier\n\
             # TYPE nodedb_io_wait_ns histogram\n",
        );
        for (tier, label) in TIER_LABELS.iter().enumerate() {
            let mut cumulative = 0u64;
            for (i, &boundary_ns) in IO_WAIT_BUCKETS_NS.iter().enumerate() {
                cumulative += self.buckets[tier][i].load(Ordering::Relaxed);
                let le_s = boundary_ns as f64 / 1_000_000_000.0;
                let _ = writeln!(
                    out,
                    r#"nodedb_io_wait_ns_bucket{{priority="{label}",le="{le_s:.9}"}} {cumulative}"#
                );
            }
            let total = self.count[tier].load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                r#"nodedb_io_wait_ns_bucket{{priority="{label}",le="+Inf"}} {total}"#
            );
            let sum_s = self.sum_ns[tier].load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
            let _ = writeln!(
                out,
                r#"nodedb_io_wait_ns_sum{{priority="{label}"}} {sum_s:.9}"#
            );
            let _ = writeln!(
                out,
                r#"nodedb_io_wait_ns_count{{priority="{label}"}} {total}"#
            );
        }
    }
}

impl Default for IoMetrics {
    fn default() -> Self {
        Self::new()
    }
}
