//! KV hash table entry: key + inline/overflow value + TTL metadata.
//!
//! Each slot in the Robin Hood hash table stores a `KvEntry`. Small values
//! (≤ inline threshold) are embedded directly to avoid a pointer chase.
//! Larger values are stored in an overflow buffer and referenced by index.

/// Sentinel value indicating no TTL (key never expires).
pub const NO_EXPIRY: u64 = 0;

/// A single entry in the KV hash table.
///
/// Layout is optimized for cache-line-friendly access:
/// - `hash` is stored to avoid rehashing during probes and incremental rehash.
/// - `key` is the primary key bytes (serialized per the collection's key column type).
/// - Value is either inline (small, ≤ threshold) or overflow (index into separate buffer).
/// - `expire_at_ms` is the absolute wall-clock millisecond timestamp for expiry,
///   or [`NO_EXPIRY`] if the key has no TTL.
#[derive(Debug, Clone)]
pub struct KvEntry {
    /// Cached hash of the key (avoids rehashing during Robin Hood probing).
    pub hash: u64,
    /// Primary key bytes.
    pub key: Vec<u8>,
    /// Value storage: inline for small values, overflow index for large.
    pub value: KvValue,
    /// Absolute expiry timestamp in milliseconds since Unix epoch.
    /// `0` = no expiry.
    pub expire_at_ms: u64,
}

impl KvEntry {
    /// Create an entry with an inline value and optional TTL.
    pub fn inline(hash: u64, key: Vec<u8>, data: Vec<u8>, expire_at_ms: u64) -> Self {
        Self {
            hash,
            key,
            value: KvValue::Inline(data),
            expire_at_ms,
        }
    }

    /// Create an entry with an overflow value and optional TTL.
    pub fn overflow(hash: u64, key: Vec<u8>, index: u32, len: u32, expire_at_ms: u64) -> Self {
        Self {
            hash,
            key,
            value: KvValue::Overflow { index, len },
            expire_at_ms,
        }
    }

    /// Whether this key has a TTL set (regardless of whether it has already expired).
    ///
    /// To check if a key is still accessible, use [`is_expired`](Self::is_expired).
    pub fn has_ttl(&self) -> bool {
        self.expire_at_ms != NO_EXPIRY
    }

    /// Whether this key is expired relative to the given wall-clock time.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_at_ms != NO_EXPIRY && now_ms >= self.expire_at_ms
    }

    /// Get the inline value bytes, if stored inline.
    pub fn inline_value(&self) -> Option<&[u8]> {
        match &self.value {
            KvValue::Inline(data) => Some(data),
            KvValue::Overflow { .. } => None,
        }
    }

    /// Approximate memory usage in bytes (for budget accounting).
    pub fn mem_size(&self) -> usize {
        // Fixed overhead: hash(8) + expire(8) + enum discriminant(8) + vec overhead(24)
        let fixed = 48;
        let key_size = self.key.len();
        let value_size = match &self.value {
            KvValue::Inline(data) => data.len(),
            // Overflow stores index+len (8 bytes), actual data is in the overflow pool.
            KvValue::Overflow { .. } => 8,
        };
        fixed + key_size + value_size
    }
}

/// Value storage mode for a KV entry.
///
/// Inline: value bytes embedded directly in the hash entry. No pointer chase.
/// Overflow: value stored in a separate pool, referenced by index.
#[derive(Debug, Clone)]
pub enum KvValue {
    /// Small value stored directly in the hash entry.
    Inline(Vec<u8>),
    /// Large value stored in the overflow pool.
    Overflow {
        /// Index into the per-collection overflow buffer.
        index: u32,
        /// Length of the value in bytes.
        len: u32,
    },
}

/// Overflow buffer: stores large values that don't fit inline.
///
/// Simple append-only buffer with free-list reuse. Values are stored
/// contiguously and referenced by `(index, len)` from `KvValue::Overflow`.
///
/// Not a slab allocator yet — that optimization comes when the KV engine
/// needs size-class segregation to prevent arena fragmentation with columnar.
/// For now, a Vec<u8> with explicit free ranges is correct and sufficient.
#[derive(Debug)]
pub struct OverflowPool {
    /// Contiguous backing buffer.
    buf: Vec<u8>,
    /// Free ranges available for reuse: `(start, len)`.
    free: Vec<(u32, u32)>,
}

impl OverflowPool {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            free: Vec::new(),
        }
    }

    /// Allocate space for a value. Returns the index into the buffer.
    pub fn alloc(&mut self, data: &[u8]) -> (u32, u32) {
        let len = data.len() as u32;

        // Try to reuse a free range (first-fit).
        for i in 0..self.free.len() {
            let (start, free_len) = self.free[i];
            if free_len >= len {
                // Copy data into the reused range.
                let start_usize = start as usize;
                self.buf[start_usize..start_usize + data.len()].copy_from_slice(data);

                // Shrink or remove the free range.
                if free_len == len {
                    self.free.swap_remove(i);
                } else {
                    self.free[i] = (start + len, free_len - len);
                }
                return (start, len);
            }
        }

        // Append to the end.
        let start = self.buf.len() as u32;
        self.buf.extend_from_slice(data);
        (start, len)
    }

    /// Read a value by index and length.
    pub fn get(&self, index: u32, len: u32) -> &[u8] {
        let start = index as usize;
        let end = start + len as usize;
        &self.buf[start..end]
    }

    /// Free a value, making its range available for reuse.
    pub fn free(&mut self, index: u32, len: u32) {
        self.free.push((index, len));
    }

    /// Total bytes allocated (including free gaps).
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Total bytes in active use (capacity minus free ranges).
    pub fn used_bytes(&self) -> usize {
        let free_bytes: u32 = self.free.iter().map(|(_, len)| *len).sum();
        self.buf.len().saturating_sub(free_bytes as usize)
    }
}

impl Default for OverflowPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_inline_creation() {
        let entry = KvEntry::inline(12345, b"mykey".to_vec(), b"myvalue".to_vec(), NO_EXPIRY);
        assert_eq!(entry.hash, 12345);
        assert_eq!(entry.key, b"mykey");
        assert_eq!(entry.inline_value(), Some(b"myvalue".as_slice()));
        assert!(!entry.has_ttl());
        assert!(!entry.is_expired(1_000_000));
    }

    #[test]
    fn entry_with_ttl() {
        let entry = KvEntry::inline(1, b"k".to_vec(), b"v".to_vec(), 5000);
        assert!(entry.has_ttl());
        assert!(!entry.is_expired(4999));
        assert!(entry.is_expired(5000));
        assert!(entry.is_expired(5001));
    }

    #[test]
    fn entry_overflow() {
        let entry = KvEntry::overflow(1, b"k".to_vec(), 0, 1024, NO_EXPIRY);
        assert!(entry.inline_value().is_none());
        assert!(!entry.has_ttl());
        match &entry.value {
            KvValue::Overflow { index, len } => {
                assert_eq!(*index, 0);
                assert_eq!(*len, 1024);
            }
            _ => panic!("expected overflow"),
        }
    }

    #[test]
    fn overflow_pool_alloc_and_read() {
        let mut pool = OverflowPool::new();
        let (idx1, len1) = pool.alloc(b"hello world");
        assert_eq!(pool.get(idx1, len1), b"hello world");

        let (idx2, len2) = pool.alloc(b"second value");
        assert_eq!(pool.get(idx2, len2), b"second value");
        // First value still readable.
        assert_eq!(pool.get(idx1, len1), b"hello world");
    }

    #[test]
    fn overflow_pool_free_and_reuse() {
        let mut pool = OverflowPool::new();
        let (idx, len) = pool.alloc(b"12345678");
        pool.free(idx, len);

        // Realloc same size should reuse the freed range.
        let (idx2, len2) = pool.alloc(b"abcdefgh");
        assert_eq!(idx2, idx);
        assert_eq!(len2, len);
        assert_eq!(pool.get(idx2, len2), b"abcdefgh");
    }

    #[test]
    fn overflow_pool_partial_reuse() {
        let mut pool = OverflowPool::new();
        let (idx, len) = pool.alloc(b"1234567890"); // 10 bytes
        pool.free(idx, len);

        // Alloc smaller: reuses front of the freed range.
        let (idx2, len2) = pool.alloc(b"abc"); // 3 bytes
        assert_eq!(idx2, idx);
        assert_eq!(len2, 3);
        assert_eq!(pool.get(idx2, len2), b"abc");
    }

    #[test]
    fn mem_size_is_reasonable() {
        let entry = KvEntry::inline(1, b"key".to_vec(), b"val".to_vec(), NO_EXPIRY);
        let size = entry.mem_size();
        // 48 fixed + 3 key + 3 value = 54
        assert_eq!(size, 54);
    }
}
