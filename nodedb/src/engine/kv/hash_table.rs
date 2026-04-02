//! Robin Hood open-addressing hash table with incremental rehash.
//!
//! `!Send` — owned by a single TPC core. No cross-core sharing.
//!
//! Key properties:
//! - O(1) amortized GET/PUT/DELETE via Robin Hood probing.
//! - Cache-friendly: slots are contiguous in memory, probe sequences are short.
//! - Incremental rehash: when load factor > threshold, the table doubles and
//!   migrates entries progressively (a few per PUT) to avoid stalling the reactor.
//! - Lazy expiry fallback: GET checks expiry and returns None for expired keys.

use super::entry::{KvEntry, NO_EXPIRY};
use super::hash_helpers::{
    extract_value_from, free_value_from, hash_key, read_value_from, store_value_in,
};
use super::slab::SlabAllocator;

/// Metadata about a KV entry, returned by [`KvHashTable::get_entry_meta`].
///
/// Used by [`super::engine::KvEngine`] to retrieve the original `expire_at_ms`
/// when cancelling old expiry entries in the timing wheel.
#[derive(Debug, Clone, Copy)]
pub struct EntryMeta {
    /// Whether this key has a TTL set (`expire_at_ms != NO_EXPIRY`).
    pub has_ttl: bool,
    /// Absolute expiry timestamp in milliseconds, or [`NO_EXPIRY`] if persistent.
    pub expire_at_ms: u64,
}

/// Robin Hood hash table with incremental rehash.
///
/// Uses two internal tables during rehash: `primary` (new, larger) and
/// `rehash_source` (old, being migrated). All lookups check both tables.
/// PUTs go to the primary. Each PUT migrates `rehash_batch_size` entries
/// from old to new.
pub struct KvHashTable {
    /// Primary slot array.
    slots: Vec<Option<KvEntry>>,
    /// Number of occupied slots in the primary table.
    len: usize,
    /// Capacity (number of slots) — always a power of two.
    capacity: usize,
    /// Load factor threshold that triggers rehash (0.0–1.0).
    load_factor_threshold: f32,
    /// Entries migrated per PUT during incremental rehash.
    rehash_batch_size: usize,

    /// Old table being migrated (during incremental rehash).
    rehash_source: Option<Vec<Option<KvEntry>>>,
    /// Next index to scan in the old table during incremental rehash.
    rehash_cursor: usize,

    /// Slab allocator for overflow values (fixed-size tiers, O(1) alloc/free).
    overflow: SlabAllocator,

    /// Inline value threshold in bytes.
    inline_threshold: usize,
}

impl KvHashTable {
    /// Create a new hash table with the given initial capacity.
    ///
    /// `capacity` is rounded up to the next power of two.
    /// `inline_threshold` determines whether values are stored inline or in the overflow pool.
    pub fn new(
        capacity: usize,
        load_factor_threshold: f32,
        rehash_batch_size: usize,
        inline_threshold: usize,
    ) -> Self {
        let capacity = capacity.max(16).next_power_of_two();
        Self {
            slots: vec![None; capacity],
            len: 0,
            capacity,
            load_factor_threshold,
            rehash_batch_size,
            rehash_source: None,
            rehash_cursor: 0,
            overflow: SlabAllocator::new(),
            inline_threshold,
        }
    }

    /// Number of entries in the table (including entries still in rehash source).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Export all entries for snapshot/backup.
    ///
    /// Returns `(key_bytes, value_bytes, expire_at_ms)` for every live entry.
    pub fn export_entries(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        let mut result = Vec::with_capacity(self.len);
        for entry in self.slots.iter().flatten() {
            let value_bytes = super::hash_helpers::extract_value_from(&entry.value, &self.overflow);
            result.push((entry.key.clone(), value_bytes, entry.expire_at_ms));
        }
        result
    }

    /// Current load factor of the primary table.
    pub fn load_factor(&self) -> f32 {
        self.len as f32 / self.capacity as f32
    }

    /// Whether an incremental rehash is in progress.
    pub fn is_rehashing(&self) -> bool {
        self.rehash_source.is_some()
    }

    /// Capacity (number of slots) of the primary table.
    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of slots in the rehash source (0 if not rehashing).
    pub(super) fn rehash_source_len(&self) -> usize {
        self.rehash_source.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    /// Access a slot in the primary table by index.
    pub(super) fn primary_slot(&self, idx: usize) -> Option<&KvEntry> {
        self.slots[idx].as_ref()
    }

    /// Access a slot in the rehash source by index.
    pub(super) fn rehash_slot(&self, idx: usize) -> Option<&KvEntry> {
        self.rehash_source.as_ref().and_then(|s| s[idx].as_ref())
    }

    /// Read value bytes from an entry, resolving overflow from the pool.
    pub(super) fn read_value<'a>(&'a self, entry: &'a KvEntry) -> &'a [u8] {
        read_value_from(entry, &self.overflow)
    }

    /// Get a value by key. Returns None if not found or expired.
    ///
    /// Checks the primary table first, then the rehash source (if active).
    /// Expired keys return None (lazy expiry fallback).
    pub fn get(&self, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        let h = hash_key(key);

        // Check primary table.
        if let Some(entry) = self.probe_find(&self.slots, h, key) {
            if entry.is_expired(now_ms) {
                return None;
            }
            return Some(read_value_from(entry, &self.overflow));
        }

        // Check rehash source if active.
        if let Some(old) = &self.rehash_source
            && let Some(entry) = self.probe_find(old, h, key)
        {
            if entry.is_expired(now_ms) {
                return None;
            }
            return Some(read_value_from(entry, &self.overflow));
        }

        None
    }

    /// Get entry metadata without returning the value.
    ///
    /// Returns the TTL state and expiry timestamp for a key, used by
    /// the KV engine to cancel old expiry entries before updates.
    /// Does NOT check expiry — returns metadata even for expired keys,
    /// since the caller needs the original `expire_at_ms` for cancellation.
    pub fn get_entry_meta(&self, key: &[u8]) -> Option<EntryMeta> {
        let h = hash_key(key);

        if let Some(entry) = self.probe_find(&self.slots, h, key) {
            return Some(EntryMeta {
                has_ttl: entry.has_ttl(),
                expire_at_ms: entry.expire_at_ms,
            });
        }

        if let Some(old) = &self.rehash_source
            && let Some(entry) = self.probe_find(old, h, key)
        {
            return Some(EntryMeta {
                has_ttl: entry.has_ttl(),
                expire_at_ms: entry.expire_at_ms,
            });
        }

        None
    }

    /// Insert or update a key-value pair. Returns the old value bytes if overwritten.
    ///
    /// Triggers incremental rehash migration if a rehash is in progress.
    /// Triggers a new rehash if the load factor exceeds the threshold.
    pub fn put(&mut self, key: &[u8], value: &[u8], expire_at_ms: u64) -> Option<Vec<u8>> {
        // Progress incremental rehash.
        self.rehash_step();

        let h = hash_key(key);

        // Check if key exists in primary — update in place (no key copy needed).
        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            let old_value =
                extract_value_from(&self.slots[idx].as_ref().unwrap().value, &self.overflow);
            free_value_from(&self.slots[idx].as_ref().unwrap().value, &mut self.overflow);
            let new_kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
            let entry = self.slots[idx].as_mut().unwrap();
            entry.value = new_kv_value;
            entry.expire_at_ms = expire_at_ms;
            return Some(old_value);
        }

        // Check rehash source — if found, remove from old and insert into primary.
        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            let old_entry = old_slots[idx].take().unwrap();
            let old_value = extract_value_from(&old_entry.value, &self.overflow);
            free_value_from(&old_entry.value, &mut self.overflow);
            let new_kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
            let new_entry = KvEntry {
                hash: h,
                key: key.to_vec(), // Only copy key when migrating from rehash source.
                value: new_kv_value,
                expire_at_ms,
            };
            Self::robin_hood_insert(&mut self.slots, new_entry);
            return Some(old_value);
        }

        // New key — insert into primary. Single key copy here (unavoidable — entry owns key).
        let kv_value = store_value_in(&mut self.overflow, value, self.inline_threshold);
        let entry = KvEntry {
            hash: h,
            key: key.to_vec(),
            value: kv_value,
            expire_at_ms,
        };
        Self::robin_hood_insert(&mut self.slots, entry);
        self.len += 1;

        // Check if we need to start a rehash.
        self.maybe_start_rehash();

        None
    }

    /// Delete a key. Returns true if the key existed and was removed.
    pub fn delete(&mut self, key: &[u8], now_ms: u64) -> bool {
        let h = hash_key(key);

        // Try primary table.
        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            let entry = self.slots[idx].take().unwrap();
            free_value_from(&entry.value, &mut self.overflow);
            Self::repair_after_delete_static(&mut self.slots, idx);
            self.len -= 1;
            return true;
        }

        // Try rehash source.
        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            let entry = old_slots[idx].take().unwrap();
            free_value_from(&entry.value, &mut self.overflow);
            Self::repair_after_delete_static(old_slots, idx);
            self.len -= 1;
            return true;
        }

        // If key doesn't exist at all, nothing to do.
        let _ = now_ms;
        false
    }

    /// Remove an expired entry by key (called by the expiry wheel reaper).
    /// Only removes if the entry still exists and its expire_at_ms matches.
    pub fn reap_expired(&mut self, key: &[u8], expected_expire_ms: u64) -> bool {
        let h = hash_key(key);

        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key)
            && self.slots[idx].as_ref().unwrap().expire_at_ms == expected_expire_ms
        {
            let entry = self.slots[idx].take().unwrap();
            free_value_from(&entry.value, &mut self.overflow);
            Self::repair_after_delete_static(&mut self.slots, idx);
            self.len -= 1;
            return true;
        }

        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
            && old_slots[idx].as_ref().unwrap().expire_at_ms == expected_expire_ms
        {
            let entry = old_slots[idx].take().unwrap();
            free_value_from(&entry.value, &mut self.overflow);
            Self::repair_after_delete_static(old_slots, idx);
            self.len -= 1;
            return true;
        }

        false
    }

    /// Update the TTL of an existing key. Returns true if the key was found.
    pub fn set_expire(&mut self, key: &[u8], expire_at_ms: u64) -> bool {
        let h = hash_key(key);

        if let Some(idx) = Self::probe_find_index_static(&self.slots, h, key) {
            self.slots[idx].as_mut().unwrap().expire_at_ms = expire_at_ms;
            return true;
        }

        if let Some(old_slots) = self.rehash_source.as_mut()
            && let Some(idx) = Self::probe_find_index_static(old_slots, h, key)
        {
            old_slots[idx].as_mut().unwrap().expire_at_ms = expire_at_ms;
            return true;
        }

        false
    }

    /// Remove TTL from a key (make it persistent). Returns true if found.
    pub fn persist(&mut self, key: &[u8]) -> bool {
        self.set_expire(key, NO_EXPIRY)
    }

    /// Approximate memory usage in bytes.
    pub fn mem_usage(&self) -> usize {
        let slot_size = std::mem::size_of::<Option<KvEntry>>();
        let primary = self.capacity * slot_size;
        let rehash = self
            .rehash_source
            .as_ref()
            .map(|s| s.len() * slot_size)
            .unwrap_or(0);
        let overflow = self.overflow.capacity();
        // Entry heap allocations (keys + inline values).
        let entry_heap: usize = self
            .slots
            .iter()
            .filter_map(|s| s.as_ref())
            .map(|e| e.mem_size())
            .sum();
        primary + rehash + overflow + entry_heap
    }

    // -----------------------------------------------------------------------
    // Internal: Robin Hood probing
    // -----------------------------------------------------------------------

    /// Probe distance (PSL): how far an entry is from its ideal slot.
    fn probe_distance(capacity: usize, hash: u64, current_idx: usize) -> usize {
        let ideal = (hash as usize) & (capacity - 1);
        current_idx.wrapping_sub(ideal) & (capacity - 1)
    }

    /// Find an entry by key in a slot array. Returns a reference.
    fn probe_find<'a>(
        &self,
        slots: &'a [Option<KvEntry>],
        hash: u64,
        key: &[u8],
    ) -> Option<&'a KvEntry> {
        let cap = slots.len();
        let mut idx = (hash as usize) & (cap - 1);
        let mut dist = 0;

        loop {
            match &slots[idx] {
                None => return None,
                Some(entry) => {
                    if entry.hash == hash && entry.key == key {
                        return Some(entry);
                    }
                    let entry_dist = Self::probe_distance(cap, entry.hash, idx);
                    if dist > entry_dist {
                        return None; // Robin Hood invariant: key can't be further.
                    }
                }
            }
            idx = (idx + 1) & (cap - 1);
            dist += 1;
        }
    }

    fn probe_find_index_static(slots: &[Option<KvEntry>], hash: u64, key: &[u8]) -> Option<usize> {
        let cap = slots.len();
        let mut idx = (hash as usize) & (cap - 1);
        let mut dist = 0;

        loop {
            match &slots[idx] {
                None => return None,
                Some(entry) => {
                    if entry.hash == hash && entry.key == key {
                        return Some(idx);
                    }
                    let entry_dist = Self::probe_distance(cap, entry.hash, idx);
                    if dist > entry_dist {
                        return None;
                    }
                }
            }
            idx = (idx + 1) & (cap - 1);
            dist += 1;
        }
    }

    /// Robin Hood insertion: insert an entry, swapping with entries that have
    /// shorter probe distances to maintain the Robin Hood invariant.
    fn robin_hood_insert(slots: &mut [Option<KvEntry>], mut entry: KvEntry) {
        let cap = slots.len();
        let mut idx = (entry.hash as usize) & (cap - 1);
        let mut dist = 0;

        loop {
            match &slots[idx] {
                None => {
                    slots[idx] = Some(entry);
                    return;
                }
                Some(existing) => {
                    let existing_dist = Self::probe_distance(cap, existing.hash, idx);
                    if dist > existing_dist {
                        // Steal this slot (Robin Hood: take from the rich).
                        let displaced = slots[idx].take().unwrap();
                        slots[idx] = Some(entry);
                        entry = displaced;
                        dist = existing_dist;
                    }
                }
            }
            idx = (idx + 1) & (cap - 1);
            dist += 1;
        }
    }

    /// Backward-shift deletion: after removing a slot, shift subsequent entries
    /// back to fill the gap, maintaining Robin Hood probe distance invariant.
    fn repair_after_delete_static(slots: &mut [Option<KvEntry>], deleted_idx: usize) {
        let cap = slots.len();
        let mut idx = deleted_idx;
        loop {
            let next = (idx + 1) & (cap - 1);
            match &slots[next] {
                None => break,
                Some(entry) => {
                    let d = Self::probe_distance(cap, entry.hash, next);
                    if d == 0 {
                        break; // Entry is at its ideal slot, no shift needed.
                    }
                }
            }
            slots.swap(idx, next);
            idx = next;
        }
    }

    // -----------------------------------------------------------------------
    // Internal: incremental rehash
    // -----------------------------------------------------------------------

    /// Start a rehash if load factor exceeds the threshold.
    fn maybe_start_rehash(&mut self) {
        if self.rehash_source.is_some() {
            return; // Already rehashing.
        }
        if self.load_factor() <= self.load_factor_threshold {
            return;
        }
        let new_capacity = self.capacity * 2;
        let old_slots = std::mem::replace(&mut self.slots, vec![None; new_capacity]);
        self.rehash_source = Some(old_slots);
        self.rehash_cursor = 0;
        self.capacity = new_capacity;
    }

    /// Migrate `rehash_batch_size` entries from the old table to the new one.
    fn rehash_step(&mut self) {
        let batch = self.rehash_batch_size;
        let Some(old) = &mut self.rehash_source else {
            return;
        };

        let old_len = old.len();
        let mut migrated = 0;

        while migrated < batch && self.rehash_cursor < old_len {
            if let Some(entry) = old[self.rehash_cursor].take() {
                Self::robin_hood_insert(&mut self.slots, entry);
                migrated += 1;
            }
            self.rehash_cursor += 1;
        }

        // If we've scanned the entire old table, rehash is complete.
        if self.rehash_cursor >= old_len {
            self.rehash_source = None;
            self.rehash_cursor = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table() -> KvHashTable {
        KvHashTable::new(16, 0.75, 4, 64)
    }

    #[test]
    fn basic_put_get_delete() {
        let mut t = make_table();
        assert!(t.is_empty());

        t.put(b"key1", b"value1", NO_EXPIRY);
        assert_eq!(t.len(), 1);
        assert_eq!(t.get(b"key1", 0), Some(b"value1".as_slice()));

        t.put(b"key2", b"value2", NO_EXPIRY);
        assert_eq!(t.len(), 2);

        assert!(t.delete(b"key1", 0));
        assert_eq!(t.len(), 1);
        assert!(t.get(b"key1", 0).is_none());
        assert_eq!(t.get(b"key2", 0), Some(b"value2".as_slice()));
    }

    #[test]
    fn overwrite_returns_old_value() {
        let mut t = make_table();
        assert!(t.put(b"k", b"v1", NO_EXPIRY).is_none());
        let old = t.put(b"k", b"v2", NO_EXPIRY);
        assert_eq!(old, Some(b"v1".to_vec()));
        assert_eq!(t.get(b"k", 0), Some(b"v2".as_slice()));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn delete_nonexistent_returns_false() {
        let mut t = make_table();
        assert!(!t.delete(b"nope", 0));
    }

    #[test]
    fn lazy_expiry_on_get() {
        let mut t = make_table();
        t.put(b"k", b"v", 1000);

        assert_eq!(t.get(b"k", 999), Some(b"v".as_slice()));
        assert!(t.get(b"k", 1000).is_none()); // Expired.
        assert!(t.get(b"k", 2000).is_none());
    }

    #[test]
    fn set_expire_and_persist() {
        let mut t = make_table();
        t.put(b"k", b"v", NO_EXPIRY);

        assert!(t.set_expire(b"k", 5000));
        assert!(t.get(b"k", 4999).is_some());
        assert!(t.get(b"k", 5000).is_none());

        // Reset expiry to force it to be visible again — need to re-put.
        t.put(b"k", b"v", 10000);
        assert!(t.persist(b"k"));
        assert!(t.get(b"k", u64::MAX).is_some()); // Never expires.
    }

    #[test]
    fn reap_expired_removes_matching() {
        let mut t = make_table();
        t.put(b"k", b"v", 5000);

        // Wrong expire_at_ms — should not reap.
        assert!(!t.reap_expired(b"k", 9999));
        assert_eq!(t.len(), 1);

        // Correct expire_at_ms — should reap.
        assert!(t.reap_expired(b"k", 5000));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn incremental_rehash() {
        let mut t = KvHashTable::new(16, 0.5, 2, 64);

        // Fill to trigger rehash (>50% of 16 = >8 entries).
        for i in 0..10 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            t.put(key.as_bytes(), val.as_bytes(), NO_EXPIRY);
        }

        // Rehash should have been triggered.
        // Continue inserting to drive incremental migration.
        for i in 10..20 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            t.put(key.as_bytes(), val.as_bytes(), NO_EXPIRY);
        }

        // All entries should be findable.
        for i in 0..20 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            assert_eq!(
                t.get(key.as_bytes(), 0),
                Some(val.as_bytes()),
                "missing key{i:03}"
            );
        }
        assert_eq!(t.len(), 20);
    }

    #[test]
    fn overflow_values() {
        let mut t = KvHashTable::new(16, 0.75, 4, 8); // 8-byte inline threshold.
        let small = b"tiny".to_vec(); // 4 bytes — inline.
        let large = vec![0xAB; 100]; // 100 bytes — overflow.

        t.put(b"s", &small, NO_EXPIRY);
        t.put(b"l", &large, NO_EXPIRY);

        assert_eq!(t.get(b"s", 0), Some(small.as_slice()));
        assert_eq!(t.get(b"l", 0), Some(large.as_slice()));
    }

    #[test]
    fn many_inserts_and_deletes_no_corruption() {
        let mut t = KvHashTable::new(32, 0.75, 8, 64);

        // Insert 500 keys.
        for i in 0u32..500 {
            t.put(&i.to_be_bytes(), &(i * 7).to_be_bytes(), NO_EXPIRY);
        }
        assert_eq!(t.len(), 500);

        // Delete even keys.
        for i in (0u32..500).step_by(2) {
            let key = i.to_be_bytes().to_vec();
            assert!(t.delete(&key, 0), "failed to delete key {i}");
        }
        assert_eq!(t.len(), 250);

        // Verify odd keys are still present.
        for i in (1u32..500).step_by(2) {
            let key = i.to_be_bytes();
            let expected = (i * 7).to_be_bytes();
            assert_eq!(
                t.get(&key, 0),
                Some(expected.as_slice()),
                "missing odd key {i}"
            );
        }

        // Verify even keys are gone.
        for i in (0u32..500).step_by(2) {
            let key = i.to_be_bytes();
            assert!(t.get(&key, 0).is_none(), "even key {i} should be deleted");
        }
    }

    #[test]
    fn get_entry_meta_returns_ttl_info() {
        let mut t = make_table();
        // Key without TTL.
        t.put(b"persistent", b"v", NO_EXPIRY);
        let meta = t.get_entry_meta(b"persistent").unwrap();
        assert!(!meta.has_ttl);
        assert_eq!(meta.expire_at_ms, NO_EXPIRY);

        // Key with TTL.
        t.put(b"ephemeral", b"v", 5000);
        let meta = t.get_entry_meta(b"ephemeral").unwrap();
        assert!(meta.has_ttl);
        assert_eq!(meta.expire_at_ms, 5000);

        // Non-existent key.
        assert!(t.get_entry_meta(b"nope").is_none());
    }

    #[test]
    fn mem_usage_grows_with_entries() {
        let mut t = make_table();
        let base = t.mem_usage();

        for i in 0..100u32 {
            t.put(&i.to_be_bytes(), &[0u8; 32], NO_EXPIRY);
        }

        assert!(t.mem_usage() > base);
    }
}
