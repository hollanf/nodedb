//! WorkloadAnalyzer — tracks historical query predicates and exposes a 3D cost
//! model (memory × latency × recall) to guide SIEVE subindex build decisions.

use std::collections::HashMap;

use super::collection::PredicateSignature;

/// A single query event recorded in the workload log.
pub struct QueryRecord {
    /// Predicate signature observed in this query (e.g. `"tenant_id=42"`).
    pub predicate_signature: PredicateSignature,
    /// Unix timestamp (seconds) at which the query arrived.
    pub timestamp_secs: u64,
}

/// Tracks historical query logs and exposes heuristics for deciding which
/// predicates warrant a dedicated SIEVE subindex.
pub struct WorkloadAnalyzer {
    log: Vec<QueryRecord>,
    /// A predicate is considered "stable" when its share of total queries is at
    /// or above this fraction.  Typical value: `0.05` (5%).
    stable_fraction_threshold: f32,
}

impl WorkloadAnalyzer {
    /// Create a new analyzer.  `stable_fraction_threshold` is the minimum
    /// fraction of total queries a predicate must represent to be considered
    /// stable.
    pub fn new(stable_fraction_threshold: f32) -> Self {
        Self {
            log: Vec::new(),
            stable_fraction_threshold,
        }
    }

    /// Record a query that used the given predicate signature.
    pub fn record(&mut self, signature: PredicateSignature, timestamp_secs: u64) {
        self.log.push(QueryRecord {
            predicate_signature: signature,
            timestamp_secs,
        });
    }

    /// Returns `(signature, frequency)` pairs sorted by frequency descending,
    /// filtered to those whose frequency exceeds `stable_fraction_threshold`.
    ///
    /// Frequency is the fraction of total log entries contributed by that
    /// predicate.  Returns an empty Vec if the log is empty.
    pub fn stable_predicates(&self) -> Vec<(PredicateSignature, f32)> {
        if self.log.is_empty() {
            return Vec::new();
        }

        let total = self.log.len() as f32;
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for record in &self.log {
            *counts
                .entry(record.predicate_signature.as_str())
                .or_insert(0) += 1;
        }

        let mut result: Vec<(PredicateSignature, f32)> = counts
            .into_iter()
            .filter_map(|(sig, count)| {
                let freq = count as f32 / total;
                if freq >= self.stable_fraction_threshold {
                    Some((sig.to_owned(), freq))
                } else {
                    None
                }
            })
            .collect();

        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        result
    }

    /// Rough 3D cost model for a candidate subindex.
    ///
    /// Returns `(memory_bytes, latency_ms, recall)` estimates:
    ///
    /// - `memory_bytes` ≈ `n * (dim * 4 + sub_m * 4 * avg_layers)` where
    ///   `avg_layers ≈ 2`.
    /// - `latency_ms`   ≈ `log2(n) * 0.01`.
    /// - `recall`       = `0.95` (assumed for a correctly built HNSW index).
    ///
    /// These are intentionally coarse: they guide the build/no-build decision
    /// for a planner, not a precision benchmark.
    pub fn estimate_subindex_cost(
        &self,
        vectors_in_subindex: usize,
        dim: usize,
    ) -> (usize, f32, f32) {
        const AVG_LAYERS: usize = 2;
        const SUB_M: usize = 16; // representative default
        const BYTES_PER_FLOAT: usize = 4;
        const ASSUMED_RECALL: f32 = 0.95;

        if vectors_in_subindex == 0 {
            return (0, 0.0, ASSUMED_RECALL);
        }

        let memory_bytes =
            vectors_in_subindex * (dim * BYTES_PER_FLOAT + SUB_M * BYTES_PER_FLOAT * AVG_LAYERS);

        let latency_ms = (vectors_in_subindex as f64).log2() as f32 * 0.01;

        (memory_bytes, latency_ms, ASSUMED_RECALL)
    }

    /// Drop all log entries older than `retention_secs` seconds before `now`.
    ///
    /// Entries with `timestamp_secs < now - retention_secs` are removed.
    /// Entries at or after that boundary are kept.
    pub fn compact(&mut self, now_secs: u64, retention_secs: u64) {
        let cutoff = now_secs.saturating_sub(retention_secs);
        self.log.retain(|r| r.timestamp_secs >= cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Record 100 queries: 80 with signature "A", 20 with "B".
    /// `stable_predicates(0.5)` must return only "A".
    #[test]
    fn stable_predicates_returns_dominant_signature() {
        let mut analyzer = WorkloadAnalyzer::new(0.5);
        for i in 0u64..80 {
            analyzer.record("A".to_string(), i);
        }
        for i in 80u64..100 {
            analyzer.record("B".to_string(), i);
        }

        let stable = analyzer.stable_predicates();
        assert_eq!(stable.len(), 1, "only A should exceed 50% threshold");
        assert_eq!(stable[0].0, "A");
        let freq = stable[0].1;
        assert!(
            (freq - 0.8).abs() < 1e-5,
            "frequency should be ~0.80, got {freq}"
        );
    }

    /// With a 0.1 threshold both A (80%) and B (20%) are stable.
    #[test]
    fn stable_predicates_multiple_signatures() {
        let mut analyzer = WorkloadAnalyzer::new(0.1);
        for i in 0u64..80 {
            analyzer.record("A".to_string(), i);
        }
        for i in 80u64..100 {
            analyzer.record("B".to_string(), i);
        }

        let stable = analyzer.stable_predicates();
        assert_eq!(stable.len(), 2);
        // Sorted descending by frequency: A first.
        assert_eq!(stable[0].0, "A");
        assert_eq!(stable[1].0, "B");
    }

    /// Empty log returns an empty result.
    #[test]
    fn empty_log_returns_empty() {
        let analyzer = WorkloadAnalyzer::new(0.05);
        assert!(analyzer.stable_predicates().is_empty());
    }

    /// `compact` drops entries older than the retention window.
    #[test]
    fn compact_drops_old_records() {
        let mut analyzer = WorkloadAnalyzer::new(0.05);
        // Timestamps: 0..50 old, 150..200 recent.
        for i in 0u64..50 {
            analyzer.record("old".to_string(), i);
        }
        for i in 150u64..200 {
            analyzer.record("recent".to_string(), i);
        }

        // now=200, retention=100 → cutoff=100; entries with ts < 100 removed.
        analyzer.compact(200, 100);

        let stable = analyzer.stable_predicates();
        // Only "recent" entries remain.
        assert_eq!(stable.len(), 1);
        assert_eq!(stable[0].0, "recent");
    }

    /// After compacting everything, log is empty.
    #[test]
    fn compact_all_yields_empty() {
        let mut analyzer = WorkloadAnalyzer::new(0.05);
        for i in 0u64..10 {
            analyzer.record("X".to_string(), i);
        }
        // cutoff = 1000 - 0 = 1000; all ts < 1000 dropped.
        analyzer.compact(1000, 0);
        assert!(analyzer.stable_predicates().is_empty());
    }

    /// `estimate_subindex_cost` returns plausible values.
    #[test]
    fn estimate_subindex_cost_plausible() {
        let analyzer = WorkloadAnalyzer::new(0.05);
        let (mem, lat, recall) = analyzer.estimate_subindex_cost(1000, 128);

        // memory_bytes = 1000 * (128*4 + 16*4*2) = 1000 * (512 + 128) = 640_000
        assert_eq!(mem, 640_000);

        // latency_ms ≈ log2(1000) * 0.01 ≈ 9.97 * 0.01 ≈ 0.0997
        assert!(lat > 0.0 && lat < 1.0, "latency_ms={lat} should be sub-ms");

        assert!((recall - 0.95).abs() < 1e-6);
    }

    #[test]
    fn estimate_subindex_cost_zero_vectors() {
        let analyzer = WorkloadAnalyzer::new(0.05);
        let (mem, lat, recall) = analyzer.estimate_subindex_cost(0, 128);
        assert_eq!(mem, 0);
        assert_eq!(lat, 0.0);
        assert!((recall - 0.95).abs() < 1e-6);
    }
}
