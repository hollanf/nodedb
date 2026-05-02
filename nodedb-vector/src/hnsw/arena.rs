//! Per-invocation scratch arena for HNSW beam-search heaps.
//!
//! `BeamSearchArena` holds pre-allocated backing buffers for the three
//! data structures consumed by every `search_layer` call:
//!
//! - `candidates` — min-heap of `(dist, id)` pairs (wrapped in `Reverse`).
//! - `results`    — max-heap of `(dist, id)` pairs.
//! - `visited`    — hash set tracking nodes seen during traversal.
//!
//! The arena is owned by `HnswIndex` (inside a `RefCell`) and reset at the
//! start of each search.  Over time the backing `Vec`s grow to the
//! high-water-mark capacity of the largest search executed, giving amortised
//! zero-allocation steady state.
//!
//! `BeamSearchArena` is intentionally a per-core resource.  It must not be
//! shared or accessed concurrently — the wrapping `RefCell` in `HnswIndex`
//! enforces single-borrower access at runtime.

use std::cmp::Reverse;
use std::collections::HashSet;

use super::graph::Candidate;

/// Scratch arena for a single HNSW beam-search invocation.
///
/// Call [`BeamSearchArena::reset`] at the start of each search to clear the
/// buffers while retaining their allocated capacity.
pub struct BeamSearchArena {
    /// Backing storage for the min-heap of candidates to explore.
    /// Elements are `Reverse<Candidate>` so the `BinaryHeap` gives min-first.
    pub(crate) candidates: Vec<Reverse<Candidate>>,
    /// Backing storage for the max-heap of current best results.
    pub(crate) results: Vec<Candidate>,
    /// Visited-node scratch set.  Cleared on reset; capacity is retained.
    pub(crate) visited: HashSet<u32>,
}

impl BeamSearchArena {
    /// Allocate a new arena with the given initial capacity for all buffers.
    ///
    /// `initial_capacity` should be at least `ef_construction` for the build
    /// path and `ef` for the search path.  A value of 256 is a reasonable
    /// default that covers the common `ef ∈ [32, 200]` range without over-
    /// allocating.
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            // no-governor: hot-path search arena; reused across queries via reset(), instrument cost exceeds benefit
            candidates: Vec::with_capacity(initial_capacity),
            // no-governor: hot-path search arena; reused across queries via reset(), instrument cost exceeds benefit
            results: Vec::with_capacity(initial_capacity),
            visited: HashSet::with_capacity(initial_capacity * 4),
        }
    }

    /// Reset all buffers to empty while retaining their heap allocations.
    ///
    /// Must be called at the start of every `search_layer` invocation.
    #[inline]
    pub fn reset(&mut self) {
        self.candidates.clear();
        self.results.clear();
        self.visited.clear();
    }
}
