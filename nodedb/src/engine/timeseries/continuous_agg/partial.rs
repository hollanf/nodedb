//! Partial aggregate state for incremental merging.
//!
//! Stores enough state per (bucket, group_key) to merge incrementally:
//! count, sum, min, max, first/last timestamps and values, plus optional
//! sketch state for approximate aggregations.

use super::definition::AggFunction;
use nodedb_types::approx::{HyperLogLog, SpaceSaving, TDigest};

/// Partial aggregate state for a single (bucket, group_key) combination.
pub struct PartialAggregate {
    pub bucket_ts: i64,
    /// Symbol IDs for GROUP BY columns.
    pub group_key: Vec<u32>,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first_ts: i64,
    pub first_val: f64,
    pub last_ts: i64,
    pub last_val: f64,

    // ── Sketch state (lazily initialized only when needed) ──
    pub hll: Option<HyperLogLog>,
    pub tdigest: Option<TDigest>,
    pub topk: Option<SpaceSaving>,
}

impl PartialAggregate {
    /// Create from a single sample.
    pub fn new(bucket_ts: i64, group_key: Vec<u32>, ts: i64, val: f64) -> Self {
        Self {
            bucket_ts,
            group_key,
            count: 1,
            sum: val,
            min: val,
            max: val,
            first_ts: ts,
            first_val: val,
            last_ts: ts,
            last_val: val,
            hll: None,
            tdigest: None,
            topk: None,
        }
    }

    /// Ensure sketch state is initialized for the given function.
    pub fn ensure_sketch(&mut self, function: &AggFunction) {
        match function {
            AggFunction::CountDistinct if self.hll.is_none() => {
                self.hll = Some(HyperLogLog::new());
            }
            AggFunction::Percentile(_) if self.tdigest.is_none() => {
                self.tdigest = Some(TDigest::new());
            }
            AggFunction::TopK(k) if self.topk.is_none() => {
                self.topk = Some(SpaceSaving::new(*k));
            }
            _ => {}
        }
    }

    /// Merge another sample into this partial aggregate.
    pub fn merge_sample(&mut self, ts: i64, val: f64) {
        self.count += 1;
        self.sum += val;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
        if ts < self.first_ts {
            self.first_ts = ts;
            self.first_val = val;
        }
        if ts > self.last_ts {
            self.last_ts = ts;
            self.last_val = val;
        }

        // Feed into active sketches.
        if let Some(hll) = &mut self.hll {
            hll.add(val.to_bits());
        }
        if let Some(td) = &mut self.tdigest {
            td.add(val);
        }
        if let Some(ss) = &mut self.topk {
            ss.add(val.to_bits());
        }
    }

    /// Merge another partial aggregate (for cross-shard or incremental merge).
    pub fn merge_partial(&mut self, other: &PartialAggregate) {
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        if other.first_ts < self.first_ts {
            self.first_ts = other.first_ts;
            self.first_val = other.first_val;
        }
        if other.last_ts > self.last_ts {
            self.last_ts = other.last_ts;
            self.last_val = other.last_val;
        }

        // Merge sketch state.
        if let Some(other_hll) = &other.hll {
            self.hll
                .get_or_insert_with(HyperLogLog::new)
                .merge(other_hll);
        }
        if let Some(other_td) = &other.tdigest {
            self.tdigest
                .get_or_insert_with(TDigest::new)
                .merge(other_td);
        }
        if let Some(other_ss) = &other.topk {
            let k = other_ss.top_k().len().max(10);
            self.topk
                .get_or_insert_with(|| SpaceSaving::new(k))
                .merge(other_ss);
        }
    }

    /// Compute a final aggregate value from the partial state.
    pub fn finalize(&self, function: &AggFunction) -> f64 {
        match function {
            AggFunction::Sum => self.sum,
            AggFunction::Count => self.count as f64,
            AggFunction::Min => self.min,
            AggFunction::Max => self.max,
            AggFunction::Avg => {
                if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                }
            }
            AggFunction::First => self.first_val,
            AggFunction::Last => self.last_val,
            AggFunction::CountDistinct => self.hll.as_ref().map_or(0.0, |h| h.estimate()),
            AggFunction::Percentile(q) => {
                self.tdigest.as_ref().map_or(f64::NAN, |td| td.quantile(*q))
            }
            AggFunction::TopK(_) => {
                // TopK returns structured data; finalize as count of tracked items.
                self.topk.as_ref().map_or(0.0, |ss| ss.top_k().len() as f64)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_sample() {
        let pa = PartialAggregate::new(0, vec![], 100, 42.0);
        assert_eq!(pa.count, 1);
        assert_eq!(pa.finalize(&AggFunction::Sum), 42.0);
        assert_eq!(pa.finalize(&AggFunction::Avg), 42.0);
    }

    #[test]
    fn merge_samples() {
        let mut pa = PartialAggregate::new(0, vec![], 100, 10.0);
        pa.merge_sample(200, 20.0);
        pa.merge_sample(300, 30.0);

        assert_eq!(pa.finalize(&AggFunction::Count), 3.0);
        assert_eq!(pa.finalize(&AggFunction::Sum), 60.0);
        assert_eq!(pa.finalize(&AggFunction::Min), 10.0);
        assert_eq!(pa.finalize(&AggFunction::Max), 30.0);
        assert!((pa.finalize(&AggFunction::Avg) - 20.0).abs() < f64::EPSILON);
        assert_eq!(pa.finalize(&AggFunction::First), 10.0);
        assert_eq!(pa.finalize(&AggFunction::Last), 30.0);
    }

    #[test]
    fn sketch_count_distinct() {
        let mut pa = PartialAggregate::new(0, vec![], 100, 1.0);
        pa.ensure_sketch(&AggFunction::CountDistinct);
        for i in 1..100 {
            pa.merge_sample(100 + i, i as f64);
        }
        let est = pa.finalize(&AggFunction::CountDistinct);
        assert!(est > 80.0 && est < 120.0, "expected ~100, got {est}");
    }

    #[test]
    fn sketch_percentile() {
        let mut pa = PartialAggregate::new(0, vec![], 0, 0.0);
        pa.ensure_sketch(&AggFunction::Percentile(0.5));
        for i in 1..1000 {
            pa.merge_sample(i, i as f64);
        }
        let p50 = pa.finalize(&AggFunction::Percentile(0.5));
        assert!(p50 > 400.0 && p50 < 600.0, "expected ~500, got {p50}");
    }

    #[test]
    fn merge_partials() {
        let mut a = PartialAggregate::new(0, vec![], 100, 10.0);
        a.merge_sample(200, 20.0);

        let mut b = PartialAggregate::new(0, vec![], 50, 5.0);
        b.merge_sample(300, 30.0);

        a.merge_partial(&b);
        assert_eq!(a.count, 4);
        assert_eq!(a.sum, 65.0);
        assert_eq!(a.min, 5.0);
        assert_eq!(a.max, 30.0);
        assert_eq!(a.first_ts, 50);
        assert_eq!(a.first_val, 5.0);
        assert_eq!(a.last_ts, 300);
        assert_eq!(a.last_val, 30.0);
    }
}
