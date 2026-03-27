//! Hash table helpers: FxHash hasher and value storage operations.
//!
//! Separated from `hash_table.rs` to keep per-file code under the 500-line limit.

use std::hash::{BuildHasher, Hasher};

use super::entry::{KvValue, OverflowPool};

// ---------------------------------------------------------------------------
// FxHash — fast non-cryptographic hasher for KV keys
// ---------------------------------------------------------------------------

/// Default hasher: FxHash-style fast non-cryptographic hash.
/// Good distribution for typical KV keys (strings, UUIDs, integers).
#[derive(Clone)]
struct FxBuildHasher;

impl BuildHasher for FxBuildHasher {
    type Hasher = FxHasher;
    fn build_hasher(&self) -> FxHasher {
        FxHasher { hash: 0 }
    }
}

struct FxHasher {
    hash: u64,
}

const FX_SEED: u64 = 0x517c_c1b7_2722_0a95;

impl Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.hash = self.hash.rotate_left(5) ^ (b as u64).wrapping_mul(FX_SEED);
        }
    }
}

/// Compute the hash for a key using the FxHash algorithm.
pub(super) fn hash_key(key: &[u8]) -> u64 {
    let hasher_builder = FxBuildHasher;
    let mut h = hasher_builder.build_hasher();
    h.write(key);
    h.finish()
}

// ---------------------------------------------------------------------------
// Value storage operations (avoids borrow conflicts in hash table methods)
// ---------------------------------------------------------------------------

use super::entry::KvEntry;

/// Read value bytes from a KvEntry, resolving overflow from the pool.
pub(super) fn read_value_from<'a>(entry: &'a KvEntry, overflow: &'a OverflowPool) -> &'a [u8] {
    match &entry.value {
        KvValue::Inline(data) => data,
        KvValue::Overflow { index, len } => overflow.get(*index, *len),
    }
}

/// Extract a copy of the value bytes.
pub(super) fn extract_value_from(value: &KvValue, overflow: &OverflowPool) -> Vec<u8> {
    match value {
        KvValue::Inline(data) => data.clone(),
        KvValue::Overflow { index, len } => overflow.get(*index, *len).to_vec(),
    }
}

/// Store a value, choosing inline or overflow based on the threshold.
pub(super) fn store_value_in(
    overflow: &mut OverflowPool,
    value: &[u8],
    inline_threshold: usize,
) -> KvValue {
    if value.len() <= inline_threshold {
        KvValue::Inline(value.to_vec())
    } else {
        let (index, len) = overflow.alloc(value);
        KvValue::Overflow { index, len }
    }
}

/// Free overflow storage if the value is an overflow entry.
pub(super) fn free_value_from(value: &KvValue, overflow: &mut OverflowPool) {
    if let KvValue::Overflow { index, len } = value {
        overflow.free(*index, *len);
    }
}
