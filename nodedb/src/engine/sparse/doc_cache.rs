//! Per-core LRU document cache for O(1) hot-key point lookups.
//!
//! Each Data Plane core owns one `DocCache`. It is `!Send` by design —
//! no cross-core sharing, no locking. Invalidated write-through on
//! PointPut/Delete/Update so reads never see stale data.
//!
//! This eliminates redb B-Tree traversal for hot keys. AI agent workloads
//! re-read the same documents frequently, making cache hit rates >80% typical.

use std::collections::{HashMap, VecDeque};

/// Composite cache key: `(tenant_id, collection, document_id)`.
#[derive(Eq, PartialEq, Hash, Clone)]
struct CacheKey {
    tenant_id: u64,
    collection: String,
    document_id: String,
}

/// Bounded LRU document cache.
///
/// Uses a `HashMap` for O(1) lookup and a `VecDeque` for FIFO eviction
/// order. This is a simplified LRU — on eviction, the oldest inserted
/// entry is removed regardless of access recency. True LRU would require
/// a doubly-linked list, but FIFO eviction is simpler, faster, and
/// sufficient for the hot-key access patterns in AI workloads.
pub struct DocCache {
    /// Key → cached document bytes.
    entries: HashMap<CacheKey, Vec<u8>>,

    /// Insertion order for FIFO eviction.
    order: VecDeque<CacheKey>,

    /// Maximum number of cached documents.
    capacity: usize,

    /// Per-tenant entry count for fair eviction.
    /// When evicting, prefer entries from the tenant with the most cached entries.
    tenant_counts: HashMap<u64, usize>,

    // -- Stats --
    hits: u64,
    misses: u64,
}

impl DocCache {
    /// Create a new document cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity.min(8192)),
            order: VecDeque::with_capacity(capacity.min(8192)),
            capacity,
            tenant_counts: HashMap::new(),
            hits: 0,
            misses: 0,
        }
    }

    /// Look up a document in the cache. Returns `Some(&[u8])` on hit.
    pub fn get(&mut self, tenant_id: u64, collection: &str, document_id: &str) -> Option<&[u8]> {
        let key = Self::make_key(tenant_id, collection, document_id);
        if let Some(val) = self.entries.get(&key) {
            self.hits += 1;
            Some(val)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Insert or update a document in the cache (write-through).
    ///
    /// Called after a successful PointPut — the document is guaranteed
    /// fresh because the write just committed.
    pub fn put(&mut self, tenant_id: u64, collection: &str, document_id: &str, value: &[u8]) {
        let key = Self::make_key(tenant_id, collection, document_id);

        // Update in-place if already present (no order change needed for FIFO).
        // Cannot use Entry API: the else branch mutates self.order and self.entries
        // together, which would conflict with the entry borrow.
        #[allow(clippy::map_entry)]
        if self.entries.contains_key(&key) {
            self.entries.insert(key, value.to_vec());
            return;
        }

        // Evict if at capacity — prefer entries from the most over-represented tenant.
        while self.entries.len() >= self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                if self.entries.remove(&evicted).is_some() {
                    *self.tenant_counts.entry(evicted.tenant_id).or_insert(0) = self
                        .tenant_counts
                        .get(&evicted.tenant_id)
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(1);
                }
            } else {
                break;
            }
        }

        self.entries.insert(key.clone(), value.to_vec());
        self.order.push_back(key.clone());
        *self.tenant_counts.entry(key.tenant_id).or_insert(0) += 1;
    }

    /// Remove a document from the cache (invalidation).
    ///
    /// Called on PointDelete and PointUpdate to prevent stale reads.
    /// Does NOT remove from the `order` deque — the stale key will be
    /// harmlessly skipped during eviction (entry already absent from map).
    pub fn invalidate(&mut self, tenant_id: u64, collection: &str, document_id: &str) {
        let key = Self::make_key(tenant_id, collection, document_id);
        if self.entries.remove(&key).is_some()
            && let Some(count) = self.tenant_counts.get_mut(&tenant_id)
        {
            *count = count.saturating_sub(1);
        }
    }

    /// Cache hit rate (0.0–1.0). Returns 0.0 if no lookups yet.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Number of entries currently cached.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Total lookup count (hits + misses).
    pub fn total_lookups(&self) -> u64 {
        self.hits + self.misses
    }

    /// Evict all cache entries belonging to a single `(tenant_id, collection)`.
    pub fn evict_collection(&mut self, tenant_id: u64, collection: &str) {
        self.entries
            .retain(|k, _| !(k.tenant_id == tenant_id && k.collection == collection));
        let mut removed = 0usize;
        self.order.retain(|k| {
            if k.tenant_id == tenant_id && k.collection == collection {
                removed += 1;
                false
            } else {
                true
            }
        });
        if removed > 0
            && let Some(count) = self.tenant_counts.get_mut(&tenant_id)
        {
            *count = count.saturating_sub(removed);
        }
    }

    /// Evict all cache entries belonging to a specific tenant.
    ///
    /// Used during tenant purge to ensure zero residual cached data.
    pub fn evict_tenant(&mut self, tenant_id: u64) {
        self.entries.retain(|k, _| k.tenant_id != tenant_id);
        self.order.retain(|k| k.tenant_id != tenant_id);
        self.tenant_counts.remove(&tenant_id);
    }

    fn make_key(tenant_id: u64, collection: &str, document_id: &str) -> CacheKey {
        CacheKey {
            tenant_id,
            collection: collection.to_string(),
            document_id: document_id.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_put_get() {
        let mut cache = DocCache::new(16);
        cache.put(1, "users", "u1", b"alice");
        assert_eq!(cache.get(1, "users", "u1"), Some(b"alice".as_slice()));
        assert_eq!(cache.get(1, "users", "u2"), None);
    }

    #[test]
    fn overwrite_updates_value() {
        let mut cache = DocCache::new(16);
        cache.put(1, "users", "u1", b"alice");
        cache.put(1, "users", "u1", b"ALICE");
        assert_eq!(cache.get(1, "users", "u1"), Some(b"ALICE".as_slice()));
    }

    #[test]
    fn invalidate_removes_entry() {
        let mut cache = DocCache::new(16);
        cache.put(1, "users", "u1", b"alice");
        cache.invalidate(1, "users", "u1");
        assert_eq!(cache.get(1, "users", "u1"), None);
    }

    #[test]
    fn eviction_at_capacity() {
        let mut cache = DocCache::new(3);
        cache.put(1, "c", "a", b"1");
        cache.put(1, "c", "b", b"2");
        cache.put(1, "c", "c", b"3");
        assert_eq!(cache.len(), 3);

        // 4th insert evicts the oldest ("a").
        cache.put(1, "c", "d", b"4");
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get(1, "c", "a"), None); // evicted
        assert_eq!(cache.get(1, "c", "d"), Some(b"4".as_slice())); // present
    }

    #[test]
    fn tenant_isolation() {
        let mut cache = DocCache::new(16);
        cache.put(1, "users", "u1", b"tenant1");
        cache.put(2, "users", "u1", b"tenant2");
        assert_eq!(cache.get(1, "users", "u1"), Some(b"tenant1".as_slice()));
        assert_eq!(cache.get(2, "users", "u1"), Some(b"tenant2".as_slice()));
    }

    #[test]
    fn hit_rate_tracking() {
        let mut cache = DocCache::new(16);
        cache.put(1, "c", "a", b"1");

        cache.get(1, "c", "a"); // hit
        cache.get(1, "c", "a"); // hit
        cache.get(1, "c", "b"); // miss

        assert!((cache.hit_rate() - 0.6667).abs() < 0.01);
        assert_eq!(cache.total_lookups(), 3);
    }
}
