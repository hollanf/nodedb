//! Rebuild-related accessors on `VectorCollection`.
//!
//! Provides the methods needed by the concurrent REINDEX path:
//! snapshot extraction, tombstone compaction, and atomic sealed-segment
//! replacement.

use crate::flat::FlatIndex;
use crate::hnsw::HnswParams;

use super::lifecycle::VectorCollection;
use super::segment::SealedSegment;

impl VectorCollection {
    /// Return the HNSW construction parameters for this collection.
    pub fn hnsw_params(&self) -> HnswParams {
        self.params.clone()
    }

    /// Immutable access to the growing flat index.
    ///
    /// The growing index holds vectors that have not yet been sealed into an
    /// HNSW segment.  Its contents should be included in any full-collection
    /// rebuild that wants to produce a complete result.
    pub fn growing_flat(&self) -> &FlatIndex {
        &self.growing
    }

    /// Replace all sealed segments with `new_segments`.
    ///
    /// Used by the concurrent REINDEX cutover: after the background thread
    /// finishes rebuilding the HNSW graph the Data Plane swaps in the single
    /// rebuilt segment.  The growing segment is preserved unchanged.
    ///
    /// Caller is responsible for ensuring that `new_segments` covers all
    /// vectors that were in the old sealed set; any vector not present in the
    /// new segments will return no result on subsequent searches until the
    /// growing segment is sealed.
    pub fn replace_sealed(&mut self, new_segments: Vec<SealedSegment>) {
        self.sealed = new_segments;
    }

    /// Compact tombstoned nodes from all sealed segments.
    ///
    /// This is the same operation as `compact()` — the alias exists so
    /// call sites that want to express "remove tombstones" read clearly.
    pub fn compact_tombstones(&mut self) -> usize {
        self.compact()
    }
}
