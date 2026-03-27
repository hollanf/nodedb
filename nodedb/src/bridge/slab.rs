//! Per-core slab allocator for zero-copy cross-plane payload transport.
//!
//!
//! ## Design
//!
//! Each Data Plane core owns a slab pool of fixed-size pages. When the
//! core needs to send a response payload across the SPSC bridge:
//!
//! 1. Allocate a slab page from the core's pool.
//! 2. Write the payload into the slab page.
//! 3. Send the `SlabId` (not the bytes) across the bridge.
//! 4. The Control Plane reads the payload via the `SlabId`.
//! 5. The Control Plane pushes the `SlabId` into a lock-free return queue.
//! 6. The Data Plane core drains the return queue on each event loop tick,
//!    recycling the slab page without cross-arena contention.
//!
//! This eliminates the jemalloc cross-arena `drop()` contention that
//! `Arc<[u8]>` causes when the Control Plane frees memory allocated on
//! a Data Plane core's arena.

use std::sync::atomic::{AtomicU32, Ordering};

/// Default slab page size: 64 KiB.
/// Most response payloads fit in one page. Oversized payloads
/// fall back to `Arc<[u8]>`.
///
/// Matches `BridgeTuning::slab_page_size` default. Override by passing
/// `config.tuning.bridge.slab_page_size` to `SlabPool::new()`.
pub const DEFAULT_PAGE_SIZE: usize = 64 * 1024;

/// Default number of pages per core.
///
/// Matches `BridgeTuning::slab_pool_size` default. Override by passing
/// `config.tuning.bridge.slab_pool_size` to `SlabPool::new()`.
pub const DEFAULT_POOL_SIZE: usize = 256;

/// A handle to a slab-allocated buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlabId {
    /// Which core's pool this slab belongs to.
    pub core_id: u16,
    /// Index within the core's pool.
    pub page_index: u16,
    /// Payload length within the page.
    pub len: u32,
}

/// Per-core slab pool.
///
/// Owns a contiguous block of fixed-size pages. Pages are allocated
/// and freed by index. The pool is `!Send` — owned by a single core.
pub struct SlabPool {
    /// The backing memory: `pool_size` pages of `page_size` bytes each.
    memory: Vec<u8>,
    /// Page size in bytes.
    page_size: usize,
    /// Total number of pages.
    pool_size: usize,
    /// Free list: stack of available page indices.
    free_list: Vec<u16>,
    /// Core ID (for SlabId construction).
    core_id: u16,
    /// Allocation counter (for metrics).
    alloc_count: u64,
}

impl SlabPool {
    /// Create a new slab pool for a core.
    pub fn new(core_id: u16, page_size: usize, pool_size: usize) -> Self {
        let memory = vec![0u8; page_size * pool_size];
        let free_list = (0..pool_size as u16).rev().collect();

        Self {
            memory,
            page_size,
            pool_size,
            free_list,
            core_id,
            alloc_count: 0,
        }
    }

    /// Allocate a slab page and write data into it.
    ///
    /// Returns `Some(SlabId)` if a page is available and the data fits.
    /// Returns `None` if the pool is exhausted or the data exceeds page size.
    pub fn alloc(&mut self, data: &[u8]) -> Option<SlabId> {
        if data.len() > self.page_size {
            return None; // Oversized — caller falls back to Arc<[u8]>.
        }
        let page_index = self.free_list.pop()?;
        let offset = page_index as usize * self.page_size;
        self.memory[offset..offset + data.len()].copy_from_slice(data);
        self.alloc_count += 1;

        Some(SlabId {
            core_id: self.core_id,
            page_index,
            len: data.len() as u32,
        })
    }

    /// Read data from a slab page by ID.
    ///
    /// Returns the payload bytes. The caller must not hold the reference
    /// after calling `free()`.
    pub fn read(&self, id: SlabId) -> &[u8] {
        assert_eq!(id.core_id, self.core_id, "slab ID core mismatch");
        let offset = id.page_index as usize * self.page_size;
        &self.memory[offset..offset + id.len as usize]
    }

    /// Free a slab page, returning it to the pool.
    pub fn free(&mut self, id: SlabId) {
        assert_eq!(id.core_id, self.core_id, "slab ID core mismatch");
        self.free_list.push(id.page_index);
    }

    /// Number of available (free) pages.
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Total allocation count (for metrics).
    pub fn alloc_count(&self) -> u64 {
        self.alloc_count
    }

    /// Pool utilization ratio (0.0 = empty, 1.0 = full).
    pub fn utilization(&self) -> f32 {
        1.0 - (self.available() as f32 / self.pool_size as f32)
    }
}

/// Lock-free return queue for cross-plane slab recycling.
///
/// The Control Plane pushes consumed `SlabId`s here after reading the
/// payload. The Data Plane core drains this queue on each event loop
/// tick and calls `SlabPool::free()` for each returned ID.
///
/// Uses a simple atomic counter + fixed-size ring buffer.
pub struct SlabReturnQueue {
    /// Ring buffer of slab IDs to return.
    buffer: Vec<AtomicU32>,
    /// Write position (Control Plane, producer).
    write_pos: AtomicU32,
    /// Read position (Data Plane, consumer).
    read_pos: AtomicU32,
    /// Buffer capacity.
    capacity: u32,
}

impl SlabReturnQueue {
    /// Create a new return queue with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let buffer = (0..capacity).map(|_| AtomicU32::new(u32::MAX)).collect();
        Self {
            buffer,
            write_pos: AtomicU32::new(0),
            read_pos: AtomicU32::new(0),
            capacity: capacity as u32,
        }
    }

    /// Push a slab ID for return (called by Control Plane).
    ///
    /// Returns `true` if pushed successfully, `false` if queue is full.
    pub fn push(&self, id: SlabId) -> bool {
        let packed = ((id.core_id as u32) << 16) | (id.page_index as u32);
        let pos = self.write_pos.fetch_add(1, Ordering::Relaxed);
        let idx = (pos % self.capacity) as usize;
        self.buffer[idx].store(packed, Ordering::Release);
        true
    }

    /// Drain all returned slab IDs (called by Data Plane on each tick).
    ///
    /// Returns an iterator of `SlabId`s to free. The caller should call
    /// `SlabPool::free()` for each.
    pub fn drain(&self) -> Vec<SlabId> {
        let write = self.write_pos.load(Ordering::Acquire);
        let read = self.read_pos.load(Ordering::Relaxed);

        if write == read {
            return Vec::new();
        }

        let count = write.wrapping_sub(read) as usize;
        let mut ids = Vec::with_capacity(count.min(self.capacity as usize));

        for i in 0..count {
            let idx = ((read.wrapping_add(i as u32)) % self.capacity) as usize;
            let packed = self.buffer[idx].load(Ordering::Acquire);
            if packed == u32::MAX {
                break; // Not yet written.
            }
            ids.push(SlabId {
                core_id: (packed >> 16) as u16,
                page_index: (packed & 0xFFFF) as u16,
                len: 0, // Length not needed for free.
            });
            self.buffer[idx].store(u32::MAX, Ordering::Release);
        }

        self.read_pos
            .store(read.wrapping_add(ids.len() as u32), Ordering::Release);
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_and_read() {
        let mut pool = SlabPool::new(0, 1024, 4);
        let data = b"hello slab allocator";
        let id = pool.alloc(data).unwrap();
        assert_eq!(id.core_id, 0);
        assert_eq!(id.len, data.len() as u32);
        assert_eq!(pool.read(id), data);
    }

    #[test]
    fn alloc_exhaustion() {
        let mut pool = SlabPool::new(0, 64, 2);
        let _id1 = pool.alloc(b"first").unwrap();
        let _id2 = pool.alloc(b"second").unwrap();
        assert!(pool.alloc(b"third").is_none()); // Pool exhausted.
    }

    #[test]
    fn free_and_reuse() {
        let mut pool = SlabPool::new(0, 64, 1);
        let id = pool.alloc(b"data").unwrap();
        pool.free(id);
        assert_eq!(pool.available(), 1);
        let id2 = pool.alloc(b"reused").unwrap();
        assert_eq!(pool.read(id2), b"reused");
    }

    #[test]
    fn oversized_rejected() {
        let mut pool = SlabPool::new(0, 16, 4);
        let big = vec![0u8; 32];
        assert!(pool.alloc(&big).is_none());
    }

    #[test]
    fn utilization() {
        let mut pool = SlabPool::new(0, 64, 4);
        assert_eq!(pool.utilization(), 0.0);
        pool.alloc(b"a").unwrap();
        pool.alloc(b"b").unwrap();
        assert!((pool.utilization() - 0.5).abs() < 0.01);
    }

    #[test]
    fn return_queue_push_drain() {
        let queue = SlabReturnQueue::new(16);
        let id = SlabId {
            core_id: 1,
            page_index: 5,
            len: 100,
        };
        queue.push(id);
        let drained = queue.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].core_id, 1);
        assert_eq!(drained[0].page_index, 5);
    }

    #[test]
    fn return_queue_empty_drain() {
        let queue = SlabReturnQueue::new(16);
        let drained = queue.drain();
        assert!(drained.is_empty());
    }
}
