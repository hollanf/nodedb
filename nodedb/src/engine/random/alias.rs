//! Alias method for O(N) setup, O(1) weighted random selection.
//!
//! The alias method (Vose's algorithm) preprocesses a weight distribution
//! into two parallel arrays such that each pick requires exactly one random
//! number and one comparison — O(1) per pick regardless of distribution size.
//!
//! Reference: M. D. Vose, "A Linear Algorithm for Generating Random Numbers
//! with a Given Distribution" (IEEE Trans. Software Eng., 1991).

use super::csprng::SeedableRng;

/// Precomputed alias table for O(1) weighted random sampling.
///
/// After `new()` (O(N) setup), each `sample()` call is O(1).
/// Supports sampling with or without replacement.
#[derive(Debug)]
pub struct AliasTable {
    /// Probability threshold for each slot: if random < prob[i], pick i, else pick alias[i].
    prob: Vec<f64>,
    /// Alias index for each slot.
    alias: Vec<usize>,
    /// Number of items.
    n: usize,
}

impl AliasTable {
    /// Build an alias table from non-negative weights.
    ///
    /// Weights do not need to sum to 1 — they are normalized internally.
    /// Zero-weight items are never selected. All weights must be non-negative.
    ///
    /// Returns `None` if weights are empty or all zero.
    pub fn new(weights: &[f64]) -> Option<Self> {
        let n = weights.len();
        if n == 0 {
            return None;
        }

        let total: f64 = weights.iter().sum();
        if total <= 0.0 {
            return None;
        }

        // Normalize so that each probability is scaled to n * (w_i / total).
        let scale = n as f64 / total;
        let mut scaled: Vec<f64> = weights.iter().map(|w| w * scale).collect();

        let mut prob = vec![0.0f64; n];
        let mut alias = vec![0usize; n];

        // Partition into small (< 1) and large (>= 1) groups.
        let mut small: Vec<usize> = Vec::new();
        let mut large: Vec<usize> = Vec::new();

        for (i, &s) in scaled.iter().enumerate() {
            if s < 1.0 {
                small.push(i);
            } else {
                large.push(i);
            }
        }

        // Vose's algorithm: pair small items with large items.
        while let (Some(s), Some(&l)) = (small.pop(), large.last()) {
            prob[s] = scaled[s];
            alias[s] = l;
            scaled[l] -= 1.0 - scaled[s];

            if scaled[l] < 1.0 {
                large.pop();
                small.push(l);
            }
        }

        // Remaining items get probability 1.0 (numerical precision cleanup).
        for &l in &large {
            prob[l] = 1.0;
        }
        for &s in &small {
            prob[s] = 1.0;
        }

        Some(Self { prob, alias, n })
    }

    /// Sample one index (O(1) per call).
    pub fn sample(&self, rng: &mut SeedableRng) -> usize {
        let i = rng.gen_range(self.n as u64) as usize;
        let u = rng.gen_f64();
        if u < self.prob[i] { i } else { self.alias[i] }
    }

    /// Sample `count` indices without replacement.
    ///
    /// If `count >= n`, returns all indices (shuffled).
    /// Uses rejection sampling: resample if we pick an already-selected index.
    /// For count << n this is efficient. For count close to n, falls back to shuffle.
    pub fn sample_without_replacement(&self, rng: &mut SeedableRng, count: usize) -> Vec<usize> {
        let count = count.min(self.n);

        if count == self.n {
            // Return all indices.
            return (0..self.n).collect();
        }

        // For small count/n ratio, use rejection sampling.
        if count <= self.n / 2 {
            let mut selected = std::collections::HashSet::with_capacity(count);
            let mut result = Vec::with_capacity(count);
            let max_attempts = count * 20; // Prevent infinite loop on degenerate distributions.
            let mut attempts = 0;

            while result.len() < count && attempts < max_attempts {
                let idx = self.sample(rng);
                if selected.insert(idx) {
                    result.push(idx);
                }
                attempts += 1;
            }
            result
        } else {
            // For large count, shuffle all indices and take first `count`.
            let mut indices: Vec<usize> = (0..self.n).collect();
            // Fisher-Yates shuffle.
            for i in (1..self.n).rev() {
                let j = rng.gen_range((i + 1) as u64) as usize;
                indices.swap(i, j);
            }
            indices.truncate(count);
            indices
        }
    }

    /// Sample `count` indices with replacement (same index can appear multiple times).
    pub fn sample_with_replacement(&self, rng: &mut SeedableRng, count: usize) -> Vec<usize> {
        (0..count).map(|_| self.sample(rng)).collect()
    }

    /// Number of items in the distribution.
    pub fn len(&self) -> usize {
        self.n
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uniform_distribution() {
        let weights = vec![1.0, 1.0, 1.0, 1.0];
        let table = AliasTable::new(&weights).unwrap();
        assert_eq!(table.len(), 4);

        let mut rng = SeedableRng::from_seed_str("uniform-test");
        let mut counts = [0u32; 4];
        let n = 10_000;
        for _ in 0..n {
            counts[table.sample(&mut rng)] += 1;
        }

        // Each should be roughly 2500 ± 300.
        for (i, &c) in counts.iter().enumerate() {
            assert!(
                c > 2000 && c < 3000,
                "item {i} sampled {c} times, expected ~2500"
            );
        }
    }

    #[test]
    fn skewed_distribution() {
        // Item 0 has 90% weight, item 1 has 10%.
        let weights = vec![9.0, 1.0];
        let table = AliasTable::new(&weights).unwrap();

        let mut rng = SeedableRng::from_seed_str("skewed-test");
        let mut counts = [0u32; 2];
        let n = 10_000;
        for _ in 0..n {
            counts[table.sample(&mut rng)] += 1;
        }

        // Item 0 should be ~9000, item 1 ~1000.
        assert!(counts[0] > 8000, "item 0: {}", counts[0]);
        assert!(counts[1] > 500, "item 1: {}", counts[1]);
    }

    #[test]
    fn zero_weight_never_selected() {
        let weights = vec![0.0, 1.0, 0.0, 1.0];
        let table = AliasTable::new(&weights).unwrap();

        let mut rng = SeedableRng::from_seed_str("zero-test");
        for _ in 0..1000 {
            let idx = table.sample(&mut rng);
            assert!(idx == 1 || idx == 3, "got zero-weight index {idx}");
        }
    }

    #[test]
    fn all_zero_returns_none() {
        assert!(AliasTable::new(&[0.0, 0.0]).is_none());
        assert!(AliasTable::new(&[]).is_none());
    }

    #[test]
    fn without_replacement() {
        let weights = vec![1.0, 1.0, 1.0, 1.0, 1.0];
        let table = AliasTable::new(&weights).unwrap();
        let mut rng = SeedableRng::from_seed_str("no-replace");

        let selected = table.sample_without_replacement(&mut rng, 3);
        assert_eq!(selected.len(), 3);

        // No duplicates.
        let set: std::collections::HashSet<usize> = selected.iter().copied().collect();
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn without_replacement_all() {
        let weights = vec![1.0, 1.0, 1.0];
        let table = AliasTable::new(&weights).unwrap();
        let mut rng = SeedableRng::from_seed_str("all");

        let selected = table.sample_without_replacement(&mut rng, 10); // More than n.
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn with_replacement_allows_duplicates() {
        let weights = vec![1.0]; // Only one item.
        let table = AliasTable::new(&weights).unwrap();
        let mut rng = SeedableRng::from_seed_str("replace");

        let selected = table.sample_with_replacement(&mut rng, 5);
        assert_eq!(selected.len(), 5);
        assert!(selected.iter().all(|&i| i == 0));
    }

    #[test]
    fn deterministic_with_seed() {
        let weights = vec![1.0, 2.0, 3.0, 4.0];
        let table = AliasTable::new(&weights).unwrap();

        let mut rng1 = SeedableRng::from_seed_str("deterministic");
        let mut rng2 = SeedableRng::from_seed_str("deterministic");

        let seq1: Vec<usize> = (0..20).map(|_| table.sample(&mut rng1)).collect();
        let seq2: Vec<usize> = (0..20).map(|_| table.sample(&mut rng2)).collect();

        assert_eq!(seq1, seq2);
    }
}
