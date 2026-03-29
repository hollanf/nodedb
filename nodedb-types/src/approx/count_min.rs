//! Count-Min Sketch — approximate frequency estimation for high-cardinality streams.
//!
//! Fixed memory: `width × depth × 8` bytes. Default (1024 × 4) = 32 KB.
//! Error guarantee: over-estimates by at most `ε·N` with probability `1 − δ`
//! where `ε = e/width`, `δ = e^(−depth)`.

/// Count-Min Sketch for approximate frequency queries.
///
/// Answers "how many times did item X appear?" with bounded over-estimation.
/// Mergeable across shards: element-wise addition of tables.
#[derive(Debug)]
pub struct CountMinSketch {
    table: Vec<Vec<u64>>,
    width: usize,
    depth: usize,
    total: u64,
    seeds: Vec<u64>,
}

impl CountMinSketch {
    /// Create a sketch with default parameters (width=1024, depth=4).
    ///
    /// Error ≤ `e/1024 ≈ 0.27%` of total count, confidence ≥ `1 − e^(−4) ≈ 98.2%`.
    pub fn new() -> Self {
        Self::with_params(1024, 4)
    }

    /// Create with custom width and depth.
    ///
    /// * `width` — number of counters per row (controls accuracy; larger = less error)
    /// * `depth` — number of hash functions/rows (controls confidence; larger = higher)
    pub fn with_params(width: usize, depth: usize) -> Self {
        let width = width.max(16);
        let depth = depth.max(2);
        let seeds: Vec<u64> = (0..depth as u64)
            .map(|i| 0x517cc1b727220a95u64.wrapping_add(i.wrapping_mul(0x6c62272e07bb0142)))
            .collect();
        Self {
            table: vec![vec![0u64; width]; depth],
            width,
            depth,
            total: 0,
            seeds,
        }
    }

    /// Add an item occurrence.
    pub fn add(&mut self, item: u64) {
        self.add_count(item, 1);
    }

    /// Add an item with a specified count.
    pub fn add_count(&mut self, item: u64, count: u64) {
        self.total += count;
        for d in 0..self.depth {
            let idx = self.hash(d, item);
            self.table[d][idx] += count;
        }
    }

    /// Add a batch of items (each with count 1).
    pub fn add_batch(&mut self, items: &[u64]) {
        for &item in items {
            self.add(item);
        }
    }

    /// Estimate the frequency of an item.
    ///
    /// Returns the minimum count across all hash rows (point query).
    /// This is always ≥ the true count and ≤ `true_count + ε·N`.
    pub fn estimate(&self, item: u64) -> u64 {
        let mut min_count = u64::MAX;
        for d in 0..self.depth {
            let idx = self.hash(d, item);
            min_count = min_count.min(self.table[d][idx]);
        }
        min_count
    }

    /// Total number of items added.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Merge another sketch (element-wise addition).
    ///
    /// Both sketches must have the same width and depth.
    pub fn merge(&mut self, other: &CountMinSketch) {
        debug_assert_eq!(self.width, other.width);
        debug_assert_eq!(self.depth, other.depth);
        self.total += other.total;
        for d in 0..self.depth {
            for w in 0..self.width {
                self.table[d][w] += other.table[d][w];
            }
        }
    }

    /// Memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.width * self.depth * std::mem::size_of::<u64>()
    }

    /// Serialize the table as a flat byte array (row-major, little-endian u64).
    pub fn table_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.width * self.depth * 8);
        for row in &self.table {
            for &val in row {
                bytes.extend_from_slice(&val.to_le_bytes());
            }
        }
        bytes
    }

    /// Reconstruct from serialized table bytes.
    pub fn from_table_bytes(data: &[u8], width: usize, depth: usize) -> Self {
        let mut sketch = Self::with_params(width, depth);
        let mut offset = 0;
        for d in 0..depth {
            for w in 0..width {
                if offset + 8 <= data.len() {
                    sketch.table[d][w] =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap_or([0; 8]));
                    sketch.total += sketch.table[d][w];
                    offset += 8;
                }
            }
        }
        // Total is over-counted (each item is in `depth` rows).
        // Approximate: use row 0's sum.
        sketch.total = sketch.table[0].iter().sum();
        sketch
    }

    #[inline]
    fn hash(&self, depth_idx: usize, item: u64) -> usize {
        let h = splitmix64(item ^ self.seeds[depth_idx]);
        (h as usize) % self.width
    }
}

impl Default for CountMinSketch {
    fn default() -> Self {
        Self::new()
    }
}

/// Splitmix64 hash — same as HyperLogLog for consistency.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sketch() {
        let cms = CountMinSketch::new();
        assert_eq!(cms.estimate(42), 0);
        assert_eq!(cms.total(), 0);
    }

    #[test]
    fn exact_for_single_item() {
        let mut cms = CountMinSketch::new();
        for _ in 0..1000 {
            cms.add(42);
        }
        assert_eq!(cms.estimate(42), 1000);
    }

    #[test]
    fn overestimate_bounded() {
        let mut cms = CountMinSketch::new();
        // Add 100K items with zipf-like distribution.
        for i in 0..100_000u64 {
            cms.add(i % 1000);
        }
        // Each of the 1000 items appears exactly 100 times.
        // CMS should return ≥ 100 for any item.
        for i in 0..1000u64 {
            let est = cms.estimate(i);
            assert!(est >= 100, "item {i}: expected ≥100, got {est}");
        }
        // Over-estimation should be bounded by ~ε·N = (e/1024)*100000 ≈ 265.
        for i in 0..1000u64 {
            let est = cms.estimate(i);
            assert!(est <= 400, "item {i}: expected ≤400, got {est}");
        }
    }

    #[test]
    fn absent_item_bounded() {
        let mut cms = CountMinSketch::new();
        for i in 0..10_000u64 {
            cms.add(i);
        }
        // Item 99999 was never added. Estimate should be low.
        let est = cms.estimate(99999);
        // Bounded by ε·N ≈ (e/1024)*10000 ≈ 26.5
        assert!(est <= 50, "absent item: expected ≤50, got {est}");
    }

    #[test]
    fn merge() {
        let mut a = CountMinSketch::new();
        let mut b = CountMinSketch::new();
        for _ in 0..500 {
            a.add(1);
        }
        for _ in 0..300 {
            b.add(1);
        }
        for _ in 0..200 {
            b.add(2);
        }
        a.merge(&b);
        assert_eq!(a.estimate(1), 800);
        assert_eq!(a.total(), 1000);
    }

    #[test]
    fn batch_add() {
        let mut cms = CountMinSketch::new();
        cms.add_batch(&[1, 1, 2, 3, 3, 3]);
        assert_eq!(cms.estimate(1), 2);
        assert_eq!(cms.estimate(3), 3);
        assert_eq!(cms.total(), 6);
    }

    #[test]
    fn memory() {
        let cms = CountMinSketch::new();
        assert_eq!(cms.memory_bytes(), 1024 * 4 * 8); // 32 KB
    }
}
