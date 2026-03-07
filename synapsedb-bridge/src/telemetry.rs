//! Lock-free per-core telemetry buffer.
//!
//! Data Plane cores MUST NOT serve HTTP endpoints or accept Prometheus scrapes.
//! Instead, each core writes metric samples into a lock-free ring buffer.
//! The Control Plane (Tokio) reads these buffers and serves aggregated metrics.
//!
//! This ensures observability never introduces jitter on the Data Plane.
//!
//! ## Design
//!
//! - One `TelemetryRing` per Data Plane core.
//! - Producer: the Data Plane core (single writer, no contention).
//! - Consumer: the Control Plane metrics exporter (periodic drain).
//! - Overflow policy: drop oldest samples (metrics are ephemeral).

use std::sync::atomic::{AtomicU64, Ordering};

/// A single metric sample written by a Data Plane core.
#[derive(Debug, Clone, Copy)]
pub struct MetricSample {
    /// Metric identifier (interned string index, not a heap String).
    pub metric_id: u32,

    /// Timestamp in nanoseconds since epoch (from `CLOCK_MONOTONIC`).
    pub timestamp_ns: u64,

    /// The metric value.
    pub value: MetricValue,
}

/// Metric value types.
#[derive(Debug, Clone, Copy)]
pub enum MetricValue {
    /// Monotonic counter increment.
    Counter(u64),
    /// Point-in-time gauge reading.
    Gauge(f64),
    /// Histogram observation (value to bucket).
    Histogram(f64),
}

/// Fixed-capacity ring buffer for metric samples from a single Data Plane core.
///
/// Single-producer (Data Plane core), single-consumer (Control Plane exporter).
/// On overflow, the oldest samples are silently dropped — this is acceptable
/// for metrics because they are ephemeral and the exporter scrapes frequently.
pub struct TelemetryRing {
    /// Slot array.
    slots: Box<[MetricSample]>,

    /// Write cursor (Data Plane only). Monotonically increasing.
    write_pos: AtomicU64,

    /// Read cursor (Control Plane only). Chases write_pos.
    read_pos: AtomicU64,

    /// Capacity (power of two).
    capacity: usize,

    /// Bitmask for fast modulo.
    mask: usize,

    /// Count of samples dropped due to overflow.
    dropped: AtomicU64,
}

// SAFETY: Same SPSC argument as the bridge buffer — single producer, single consumer,
// disjoint slots. The atomics are the only shared state.
unsafe impl Send for TelemetryRing {}
unsafe impl Sync for TelemetryRing {}

impl TelemetryRing {
    /// Create a new telemetry ring with the given capacity (rounded to power of two).
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        let mask = capacity - 1;

        let default_sample = MetricSample {
            metric_id: 0,
            timestamp_ns: 0,
            value: MetricValue::Counter(0),
        };

        Self {
            slots: vec![default_sample; capacity].into_boxed_slice(),
            write_pos: AtomicU64::new(0),
            read_pos: AtomicU64::new(0),
            capacity,
            mask,
            dropped: AtomicU64::new(0),
        }
    }

    /// Record a metric sample (called from Data Plane core).
    ///
    /// Lock-free, allocation-free. If the ring is full, the oldest sample
    /// is overwritten and the drop counter incremented.
    pub fn record(&mut self, sample: MetricSample) {
        let pos = self.write_pos.load(Ordering::Relaxed);
        let read = self.read_pos.load(Ordering::Relaxed);

        // If we've lapped the reader, advance the reader (drop oldest).
        if pos.wrapping_sub(read) >= self.capacity as u64 {
            self.read_pos.store(
                pos.wrapping_sub(self.capacity as u64 - 1),
                Ordering::Relaxed,
            );
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }

        let idx = (pos as usize) & self.mask;
        self.slots[idx] = sample;
        self.write_pos.store(pos.wrapping_add(1), Ordering::Release);
    }

    /// Drain all available samples into the provided buffer (called from Control Plane).
    ///
    /// Returns the number of samples drained.
    pub fn drain_into(&self, buf: &mut Vec<MetricSample>) -> usize {
        let write = self.write_pos.load(Ordering::Acquire);
        let read = self.read_pos.load(Ordering::Relaxed);

        let available = write.wrapping_sub(read) as usize;
        if available == 0 {
            return 0;
        }

        for i in 0..available {
            let idx = ((read.wrapping_add(i as u64)) as usize) & self.mask;
            buf.push(self.slots[idx]);
        }

        self.read_pos.store(write, Ordering::Release);
        available
    }

    /// Number of samples dropped due to ring overflow.
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: u32, val: u64) -> MetricSample {
        MetricSample {
            metric_id: id,
            timestamp_ns: val,
            value: MetricValue::Counter(val),
        }
    }

    #[test]
    fn basic_record_and_drain() {
        let mut ring = TelemetryRing::new(8);

        ring.record(sample(1, 100));
        ring.record(sample(2, 200));
        ring.record(sample(3, 300));

        let mut buf = Vec::new();
        let count = ring.drain_into(&mut buf);
        assert_eq!(count, 3);
        assert_eq!(buf[0].metric_id, 1);
        assert_eq!(buf[2].metric_id, 3);
    }

    #[test]
    fn overflow_drops_oldest() {
        let mut ring = TelemetryRing::new(4);

        // Write 6 samples into a capacity-4 ring.
        for i in 0..6 {
            ring.record(sample(i, i as u64));
        }

        assert!(ring.dropped_count() > 0);

        let mut buf = Vec::new();
        ring.drain_into(&mut buf);

        // Should have the most recent samples, not the oldest.
        assert!(!buf.is_empty());
        let last = buf.last().unwrap();
        assert_eq!(last.metric_id, 5);
    }

    #[test]
    fn empty_drain_returns_zero() {
        let ring = TelemetryRing::new(8);
        let mut buf = Vec::new();
        assert_eq!(ring.drain_into(&mut buf), 0);
    }
}
