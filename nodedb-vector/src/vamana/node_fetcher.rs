//! `NodeFetcher` trait — abstracts in-memory vs io_uring-backed FP32 vector
//! retrieval for Vamana beam-search and rerank.
//!
//! The trait is intentionally *not* `Send + Sync` so that `IoUringNodeFetcher`
//! (Data Plane, `!Send`) can implement it without wrapping in `Arc<Mutex<…>>`.
//! The `InMemoryFetcher` impl is `Send + Sync` (plain memory slice), but the
//! trait bound is left absent to keep the interface Data-Plane-compatible.
//!
//! ## Pre-fetch contract
//!
//! `prefetch_batch` submits I/O for a set of node indices *before* compute
//! touches them.  On the in-memory path this is a no-op.  On the io_uring
//! path it issues `IORING_OP_READ` SQEs in parallel; `fetch_fp32` then
//! collects completions.  This satisfies the TPC Page Fault Hazard rule:
//! pages are pre-warmed asynchronously so the reactor thread never blocks
//! on a major fault.

/// Abstraction over FP32 vector storage for a Vamana index partition.
///
/// Two impls exist:
/// - [`InMemoryFetcher`] — plain `Vec<Vec<f32>>`, available on all targets.
/// - `IoUringNodeFetcher` — Data Plane only, lives in `nodedb::data`.
pub trait NodeFetcher {
    /// Vector dimensionality.
    fn dim(&self) -> usize;

    /// Submit pre-fetch hints for `indices` so their data is ready before
    /// `fetch_fp32` is called.
    ///
    /// On the in-memory path this is a no-op.  On the io_uring path this
    /// issues `IORING_OP_READ` SQEs without blocking — completion is polled
    /// lazily inside `fetch_fp32`.
    fn prefetch_batch(&mut self, indices: &[u32]);

    /// Retrieve the full-precision FP32 vector for node `idx`.
    ///
    /// On the in-memory path this copies from a slice.  On the io_uring path
    /// this collects the pre-fetched completion or falls back to a synchronous
    /// read if the pre-fetch was not issued.
    ///
    /// Returns `None` if `idx` is out of range.
    fn fetch_fp32(&mut self, idx: u32) -> Option<Vec<f32>>;
}

/// In-memory `NodeFetcher` backed by a contiguous `Vec<Vec<f32>>`.
///
/// Used on all targets (including WASM) and in unit tests.  In production
/// an mmap-backed variant may wrap this type with the same interface.
pub struct InMemoryFetcher {
    dim: usize,
    vectors: Vec<Vec<f32>>,
}

impl InMemoryFetcher {
    /// Create from an owned vector slice.  All entries must have length `dim`.
    pub fn new(dim: usize, vectors: Vec<Vec<f32>>) -> Self {
        Self { dim, vectors }
    }

    /// Create from a borrowed slice, cloning each vector.
    pub fn from_slice(dim: usize, vectors: &[Vec<f32>]) -> Self {
        Self {
            dim,
            vectors: vectors.to_vec(),
        }
    }

    /// Number of nodes stored.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Returns `true` if no vectors are stored.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

impl NodeFetcher for InMemoryFetcher {
    fn dim(&self) -> usize {
        self.dim
    }

    /// No-op: all data is already in RAM.
    fn prefetch_batch(&mut self, _indices: &[u32]) {}

    fn fetch_fp32(&mut self, idx: u32) -> Option<Vec<f32>> {
        self.vectors.get(idx as usize).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_fetcher_returns_correct_vector() {
        let vecs = vec![
            vec![1.0_f32, 2.0, 3.0],
            vec![4.0_f32, 5.0, 6.0],
            vec![7.0_f32, 8.0, 9.0],
        ];
        let mut f = InMemoryFetcher::new(3, vecs.clone());
        assert_eq!(f.dim(), 3);
        assert_eq!(f.fetch_fp32(0), Some(vecs[0].clone()));
        assert_eq!(f.fetch_fp32(2), Some(vecs[2].clone()));
    }

    #[test]
    fn in_memory_fetcher_oob_returns_none() {
        let mut f = InMemoryFetcher::new(2, vec![vec![1.0, 2.0]]);
        assert!(f.fetch_fp32(1).is_none());
        assert!(f.fetch_fp32(100).is_none());
    }

    #[test]
    fn in_memory_fetcher_empty() {
        let mut f = InMemoryFetcher::new(4, vec![]);
        assert!(f.is_empty());
        assert!(f.fetch_fp32(0).is_none());
    }

    #[test]
    fn prefetch_batch_is_noop() {
        let mut f = InMemoryFetcher::new(2, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
        // Should not panic or error.
        f.prefetch_batch(&[0, 1]);
        assert_eq!(f.fetch_fp32(0), Some(vec![1.0, 2.0]));
    }
}
