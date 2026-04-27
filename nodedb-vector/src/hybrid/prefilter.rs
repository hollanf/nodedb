//! Pre-filter bitmap composition for cross-engine hybrid retrieval.
//!
//! Each upstream engine (Array, Graph, Bitemporal, Spatial, FTS) produces a
//! [`RoaringBitmap`] of matching document IDs.  This module composes those
//! bitmaps into a single *semimask* that the NaviX filtered traversal can
//! consume via its `allowed_ids` sideways-information-passing interface.

use roaring::RoaringBitmap;

/// Identifies the origin of a pre-filter bitmap so the planner can attach
/// provenance and cost hints to the composed semimask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefilterSource {
    /// Array engine — e.g. genomic region bitmaps, dense boolean columns.
    Array,
    /// Graph N-hop traversal — social/knowledge-graph neighborhood set.
    GraphHop,
    /// Bitemporal document filter — IDs whose current-state snapshot is live.
    Bitemporal,
    /// Spatial R-tree bounding-box match — geo-grounded semantic search.
    SpatialBbox,
    /// Full-Text Search (BMW BM25) candidate set.
    Fts,
}

/// A pre-filter bitmap together with its originating engine.
pub struct PrefilterInput {
    /// Which engine produced this bitmap.
    pub source: PrefilterSource,
    /// The set of document/vector IDs that passed the upstream filter.
    pub matched_ids: RoaringBitmap,
}

/// Compose multiple pre-filter inputs into a single semimask bitmap.
///
/// - `union = false` (default) → **AND** (intersection): only IDs that appear
///   in *every* input survive.  Use when all filters must be satisfied
///   simultaneously (e.g. `WHERE bbox AND fts_match`).
/// - `union = true` → **OR** (union): IDs that appear in *any* input survive.
///   Use when filters are alternatives (e.g. any-tag match).
///
/// Returns an empty bitmap when `inputs` is empty.
pub fn compose_prefilters(inputs: &[PrefilterInput], union: bool) -> RoaringBitmap {
    if inputs.is_empty() {
        return RoaringBitmap::new();
    }

    if union {
        inputs
            .iter()
            .fold(RoaringBitmap::new(), |acc, inp| &acc | &inp.matched_ids)
    } else {
        // Start from the first bitmap and intersect the rest.
        let mut result = inputs[0].matched_ids.clone();
        for inp in &inputs[1..] {
            result &= &inp.matched_ids;
        }
        result
    }
}

/// Construct a `NavixSearchOptions`-compatible semimask bitmap from a single
/// pre-filter source.
///
/// This is a typed accessor that simply returns the bitmap contained in the
/// input — its value is that it makes the semimask handoff from planner to
/// NaviX explicit and type-safe without requiring the caller to reach into
/// `PrefilterInput` directly.
pub fn semimask_from_prefilter(input: &PrefilterInput) -> RoaringBitmap {
    input.matched_ids.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_input(source: PrefilterSource, ids: impl IntoIterator<Item = u32>) -> PrefilterInput {
        let mut bitmap = RoaringBitmap::new();
        for id in ids {
            bitmap.insert(id);
        }
        PrefilterInput {
            source,
            matched_ids: bitmap,
        }
    }

    #[test]
    fn and_composition_is_intersection() {
        let a = make_input(PrefilterSource::Fts, [1, 2, 3, 4]);
        let b = make_input(PrefilterSource::SpatialBbox, [2, 4, 6]);

        let result = compose_prefilters(&[a, b], false);

        let expected: RoaringBitmap = [2u32, 4].iter().copied().collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn or_composition_is_union() {
        let a = make_input(PrefilterSource::Fts, [1, 2, 3]);
        let b = make_input(PrefilterSource::GraphHop, [3, 4, 5]);

        let result = compose_prefilters(&[a, b], true);

        let expected: RoaringBitmap = [1u32, 2, 3, 4, 5].iter().copied().collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn empty_inputs_returns_empty_bitmap() {
        let result = compose_prefilters(&[], false);
        assert!(result.is_empty());

        let result_union = compose_prefilters(&[], true);
        assert!(result_union.is_empty());
    }

    #[test]
    fn semimask_from_prefilter_returns_same_bitmap() {
        let ids = [10u32, 20, 30];
        let input = make_input(PrefilterSource::Array, ids);

        let semimask = semimask_from_prefilter(&input);

        assert_eq!(semimask, input.matched_ids);
    }

    #[test]
    fn single_input_and_returns_same_bitmap() {
        let a = make_input(PrefilterSource::Bitemporal, [7, 14, 21]);
        let result = compose_prefilters(&[a], false);
        let expected: RoaringBitmap = [7u32, 14, 21].iter().copied().collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn disjoint_and_composition_is_empty() {
        let a = make_input(PrefilterSource::Fts, [1, 2]);
        let b = make_input(PrefilterSource::SpatialBbox, [3, 4]);
        let result = compose_prefilters(&[a, b], false);
        assert!(result.is_empty());
    }
}
