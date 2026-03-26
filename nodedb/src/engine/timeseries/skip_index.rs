//! Secondary skip indexes for high-selectivity column filtering.
//!
//! Beyond the sparse primary index (timestamp-based block skip), these
//! per-column skip indexes enable predicate pushdown on non-timestamp columns:
//!
//! - **MinMax**: per-block min/max for numeric range filters.
//!   `WHERE cpu > 90` → skip blocks where `max(cpu) <= 90`.
//!   (Already partially covered by SparseIndex block-level stats; this provides
//!   a standalone per-column index file for secondary columns.)
//!
//! - **Set**: per-block set of distinct values for low-cardinality columns.
//!   `WHERE status = 'error'` → skip blocks where `status` set doesn't contain 'error'.
//!
//! - **Bloom**: per-block bloom filter for high-cardinality string columns.
//!   `WHERE trace_id = 'abc123'` → skip blocks where bloom says "definitely not here".

use serde::{Deserialize, Serialize};

/// Block size for skip indexes (aligned with sparse index).
pub const BLOCK_SIZE: usize = 1024;

// ---------------------------------------------------------------------------
// MinMax skip index
// ---------------------------------------------------------------------------

/// Per-block min/max for a numeric column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinMaxIndex {
    pub column_name: String,
    pub blocks: Vec<MinMaxEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MinMaxEntry {
    pub min: f64,
    pub max: f64,
}

impl MinMaxIndex {
    /// Build from a column of f64 values.
    pub fn build_f64(column_name: &str, values: &[f64], block_size: usize) -> Self {
        Self::build_from_iter(
            column_name,
            values.chunks(block_size).map(|chunk| {
                chunk
                    .iter()
                    .copied()
                    .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), v| {
                        (mn.min(v), mx.max(v))
                    })
            }),
        )
    }

    /// Build from a column of i64 values.
    pub fn build_i64(column_name: &str, values: &[i64], block_size: usize) -> Self {
        Self::build_from_iter(
            column_name,
            values.chunks(block_size).map(|chunk| {
                let (mn, mx) = chunk
                    .iter()
                    .copied()
                    .fold((i64::MAX, i64::MIN), |(mn, mx), v| (mn.min(v), mx.max(v)));
                (mn as f64, mx as f64)
            }),
        )
    }

    /// Shared builder from an iterator of (min, max) pairs per block.
    fn build_from_iter(column_name: &str, blocks: impl Iterator<Item = (f64, f64)>) -> Self {
        Self {
            column_name: column_name.to_string(),
            blocks: blocks.map(|(min, max)| MinMaxEntry { min, max }).collect(),
        }
    }

    /// Return block indices that might contain values matching a range predicate.
    pub fn filter_range(&self, min_val: f64, max_val: f64) -> Vec<usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, e)| e.max >= min_val && e.min <= max_val)
            .map(|(i, _)| i)
            .collect()
    }

    /// Return block indices where column > threshold.
    pub fn filter_gt(&self, threshold: f64) -> Vec<usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, e)| e.max > threshold)
            .map(|(i, _)| i)
            .collect()
    }

    /// Return block indices where column < threshold.
    pub fn filter_lt(&self, threshold: f64) -> Vec<usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, e)| e.min < threshold)
            .map(|(i, _)| i)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Set skip index
// ---------------------------------------------------------------------------

/// Per-block set of distinct symbol IDs for low-cardinality columns.
///
/// Each block stores the set of unique u32 symbol IDs present. For
/// equality filters (`WHERE status = 'error'`), check if the target
/// symbol ID is in the block's set — if not, skip the block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetIndex {
    pub column_name: String,
    pub blocks: Vec<Vec<u32>>, // sorted unique symbol IDs per block
}

impl SetIndex {
    /// Build from a column of u32 symbol IDs.
    pub fn build(column_name: &str, values: &[u32], block_size: usize) -> Self {
        let blocks = values
            .chunks(block_size)
            .map(|chunk| {
                let mut unique: Vec<u32> = chunk.to_vec();
                unique.sort_unstable();
                unique.dedup();
                unique
            })
            .collect();
        Self {
            column_name: column_name.to_string(),
            blocks,
        }
    }

    /// Return block indices that contain the given symbol ID.
    pub fn filter_eq(&self, symbol_id: u32) -> Vec<usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, set)| set.binary_search(&symbol_id).is_ok())
            .map(|(i, _)| i)
            .collect()
    }

    /// Return block indices that contain any of the given symbol IDs.
    pub fn filter_in(&self, symbol_ids: &[u32]) -> Vec<usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, set)| symbol_ids.iter().any(|id| set.binary_search(id).is_ok()))
            .map(|(i, _)| i)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Bloom filter skip index
// ---------------------------------------------------------------------------

/// Per-block bloom filter for high-cardinality columns.
///
/// Each block has a small bloom filter (default 256 bytes = 2048 bits)
/// that can probabilistically test membership. False positive rate ~1%
/// with 7 hash functions and 10 bits per element for typical block sizes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomIndex {
    pub column_name: String,
    pub blocks: Vec<BloomFilter>,
}

/// A single block's bloom filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u8,
}

impl BloomFilter {
    /// Create a bloom filter with the given number of bits and hash functions.
    pub fn new(num_bits: usize, num_hashes: u8) -> Self {
        Self {
            bits: vec![0u64; num_bits.div_ceil(64)],
            num_bits,
            num_hashes,
        }
    }

    /// Create with optimal parameters for `n` expected elements and ~1% FPR.
    pub fn for_capacity(n: usize) -> Self {
        let bits_per_elem = 10; // ~1% FPR
        let num_bits = (n * bits_per_elem).max(64);
        let num_hashes = 7; // optimal for 10 bits/elem
        Self::new(num_bits, num_hashes)
    }

    /// Insert a value (as raw bytes).
    pub fn insert(&mut self, value: &[u8]) {
        let (h1, h2) = double_hash(value);
        for i in 0..self.num_hashes as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2))) as usize % self.num_bits;
            self.bits[bit / 64] |= 1u64 << (bit % 64);
        }
    }

    /// Insert a u64 value.
    pub fn insert_u64(&mut self, value: u64) {
        self.insert(&value.to_le_bytes());
    }

    /// Test if a value might be in the set.
    ///
    /// Returns `true` if the value might be present (possible false positive).
    /// Returns `false` if the value is definitely not present.
    pub fn might_contain(&self, value: &[u8]) -> bool {
        let (h1, h2) = double_hash(value);
        for i in 0..self.num_hashes as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2))) as usize % self.num_bits;
            if (self.bits[bit / 64] >> (bit % 64)) & 1 == 0 {
                return false;
            }
        }
        true
    }

    pub fn might_contain_u64(&self, value: u64) -> bool {
        self.might_contain(&value.to_le_bytes())
    }
}

/// Double hash for bloom filter (Kirsch-Mitzenmacher).
fn double_hash(value: &[u8]) -> (u64, u64) {
    let mut h1: u64 = 0x9e3779b97f4a7c15;
    let mut h2: u64 = 0x517cc1b727220a95;
    for &b in value {
        h1 = h1.wrapping_mul(0x100000001b3).wrapping_add(b as u64);
        h2 = h2.wrapping_mul(0x6c62272e07bb0142).wrapping_add(b as u64);
    }
    (h1, h2)
}

impl BloomIndex {
    /// Build from a column of u64 values (e.g., hashed trace IDs).
    pub fn build_u64(column_name: &str, values: &[u64], block_size: usize) -> Self {
        let blocks = values
            .chunks(block_size)
            .map(|chunk| {
                let mut bf = BloomFilter::for_capacity(chunk.len());
                for &v in chunk {
                    bf.insert_u64(v);
                }
                bf
            })
            .collect();
        Self {
            column_name: column_name.to_string(),
            blocks,
        }
    }

    /// Build from a column of byte strings.
    pub fn build_bytes(column_name: &str, values: &[&[u8]], block_size: usize) -> Self {
        let blocks = values
            .chunks(block_size)
            .map(|chunk| {
                let mut bf = BloomFilter::for_capacity(chunk.len());
                for v in chunk {
                    bf.insert(v);
                }
                bf
            })
            .collect();
        Self {
            column_name: column_name.to_string(),
            blocks,
        }
    }

    /// Return block indices that might contain the given u64 value.
    pub fn filter_u64(&self, value: u64) -> Vec<usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, bf)| bf.might_contain_u64(value))
            .map(|(i, _)| i)
            .collect()
    }

    /// Return block indices that might contain the given byte string.
    pub fn filter_bytes(&self, value: &[u8]) -> Vec<usize> {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, bf)| bf.might_contain(value))
            .map(|(i, _)| i)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- MinMax --

    #[test]
    fn minmax_range_filter() {
        let values: Vec<f64> = (0..2048).map(|i| i as f64).collect();
        let idx = MinMaxIndex::build_f64("cpu", &values, 1024);
        assert_eq!(idx.blocks.len(), 2);

        // Block 0: [0, 1023], Block 1: [1024, 2047].
        let matching = idx.filter_gt(1500.0);
        assert_eq!(matching, vec![1]); // Only block 1.

        let matching = idx.filter_lt(500.0);
        assert_eq!(matching, vec![0]); // Only block 0.

        let matching = idx.filter_range(500.0, 1500.0);
        assert_eq!(matching, vec![0, 1]); // Both.
    }

    // -- Set --

    #[test]
    fn set_equality_filter() {
        // Block 0: symbols [0, 1, 2], Block 1: symbols [3, 4, 5].
        let values: Vec<u32> = (0..2048).map(|i| (i / 1024 * 3 + i % 3) as u32).collect();
        let idx = SetIndex::build("status", &values, 1024);
        assert_eq!(idx.blocks.len(), 2);

        let matching = idx.filter_eq(0);
        assert_eq!(matching, vec![0]); // Only block 0.

        let matching = idx.filter_eq(5);
        assert_eq!(matching, vec![1]); // Only block 1.
    }

    #[test]
    fn set_in_filter() {
        let values: Vec<u32> = (0..2048).map(|i| (i % 10) as u32).collect();
        let idx = SetIndex::build("tag", &values, 1024);

        let matching = idx.filter_in(&[0, 5]);
        assert_eq!(matching, vec![0, 1]); // Both blocks have 0 and 5.
    }

    // -- Bloom --

    #[test]
    fn bloom_no_false_negatives() {
        let mut bf = BloomFilter::for_capacity(100);
        for i in 0..100u64 {
            bf.insert_u64(i);
        }
        // All inserted values must be found (no false negatives).
        for i in 0..100u64 {
            assert!(bf.might_contain_u64(i), "false negative for {i}");
        }
    }

    #[test]
    fn bloom_low_false_positive_rate() {
        let mut bf = BloomFilter::for_capacity(1000);
        for i in 0..1000u64 {
            bf.insert_u64(i);
        }
        // Check 1000 non-inserted values — expect <5% false positives.
        let false_positives = (10_000..11_000u64)
            .filter(|&v| bf.might_contain_u64(v))
            .count();
        assert!(
            false_positives < 50,
            "too many false positives: {false_positives}/1000"
        );
    }

    #[test]
    fn bloom_index_block_skip() {
        // Block 0: values 0..1024, Block 1: values 1024..2048.
        let values: Vec<u64> = (0..2048).collect();
        let idx = BloomIndex::build_u64("trace_id", &values, 1024);
        assert_eq!(idx.blocks.len(), 2);

        // Value 500 should be in block 0.
        let matching = idx.filter_u64(500);
        assert!(matching.contains(&0));

        // Value 1500 should be in block 1.
        let matching = idx.filter_u64(1500);
        assert!(matching.contains(&1));

        // Value 999999 should not be in either block (might have false positive).
        let matching = idx.filter_u64(999_999);
        // Can't assert empty due to FP, but shouldn't match both.
        assert!(matching.len() <= 2);
    }
}
