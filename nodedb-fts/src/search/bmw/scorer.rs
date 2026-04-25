//! Block-Max WAND scoring engine.
//!
//! Two-level skip: WAND pivot selection across terms, then block-level
//! pruning within each term's posting list. Operates on `Surrogate` row
//! identities for zero-allocation scoring.

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::bm25;
use crate::codec::smallfloat;
use crate::posting::Bm25Params;

use super::heap::TopKHeap;
use super::skip_index::TermBlocks;

/// Sentinel returned by an exhausted cursor — sorts after every real surrogate.
const EXHAUSTED: Surrogate = Surrogate(u32::MAX);

/// Per-term iterator state during BMW traversal.
struct TermCursor<'a> {
    /// The term's posting blocks + skip index.
    term: &'a TermBlocks,
    /// Current block index within `term.blocks`.
    block_idx: usize,
    /// Current position within the current block's doc_ids.
    pos_in_block: usize,
    /// Precomputed IDF for this term.
    idf: f32,
    /// Term's global max score (for WAND pivot selection).
    max_score: f32,
}

impl<'a> TermCursor<'a> {
    fn new(term: &'a TermBlocks, total_docs: u32, avg_doc_len: f32, params: &Bm25Params) -> Self {
        let idf = bm25::idf(term.df, total_docs);
        let max_score = bm25::term_max_score(
            term.global_max_tf,
            term.global_min_fieldnorm,
            term.df,
            total_docs,
            avg_doc_len,
            params,
        );
        Self {
            term,
            block_idx: 0,
            pos_in_block: 0,
            idf,
            max_score,
        }
    }

    /// Current surrogate the cursor points to, or `EXHAUSTED` if past end.
    fn current_doc_id(&self) -> Surrogate {
        if self.block_idx >= self.term.blocks.len() {
            return EXHAUSTED;
        }
        let block = &self.term.blocks[self.block_idx];
        if self.pos_in_block >= block.doc_ids.len() {
            return EXHAUSTED;
        }
        block.doc_ids[self.pos_in_block]
    }

    /// Advance cursor to the first surrogate ≥ `target`.
    fn advance_to(&mut self, target: Surrogate) {
        // Skip entire blocks using the skip index.
        if let Some(blk) = self.term.advance_to_block(target) {
            if blk > self.block_idx {
                self.block_idx = blk;
                self.pos_in_block = 0;
            }
        } else {
            // All blocks exhausted.
            self.block_idx = self.term.blocks.len();
            return;
        }

        // Within the current block, linear scan to target.
        if self.block_idx < self.term.blocks.len() {
            let block = &self.term.blocks[self.block_idx];
            while self.pos_in_block < block.doc_ids.len()
                && block.doc_ids[self.pos_in_block] < target
            {
                self.pos_in_block += 1;
            }
            // If we've exhausted the block, move to next.
            if self.pos_in_block >= block.doc_ids.len() {
                self.block_idx += 1;
                self.pos_in_block = 0;
                // The next block's first doc must be ≥ target
                // (blocks are sorted), so no further scan needed.
            }
        }
    }

    /// Advance cursor past the current doc (to the next doc).
    fn next(&mut self) {
        if self.block_idx >= self.term.blocks.len() {
            return;
        }
        self.pos_in_block += 1;
        let block = &self.term.blocks[self.block_idx];
        if self.pos_in_block >= block.doc_ids.len() {
            self.block_idx += 1;
            self.pos_in_block = 0;
        }
    }

    /// Whether the cursor is exhausted (no more docs).
    fn is_exhausted(&self) -> bool {
        self.block_idx >= self.term.blocks.len()
    }

    /// Compute the block upper bound for the current block.
    fn block_upper_bound(&self, total_docs: u32, avg_doc_len: f32, params: &Bm25Params) -> f32 {
        if self.block_idx >= self.term.blocks.len() {
            return 0.0;
        }
        let meta = &self.term.skip[self.block_idx].meta;
        bm25::bm25_block_upper_bound(
            meta.block_max_tf,
            meta.block_min_fieldnorm,
            self.term.df,
            total_docs,
            avg_doc_len,
            params,
        )
    }

    /// Score the current document.
    fn score_current(&self, avg_doc_len: f32, params: &Bm25Params) -> f32 {
        if self.is_exhausted() {
            return 0.0;
        }
        let block = &self.term.blocks[self.block_idx];
        let tf = block.term_freqs[self.pos_in_block];
        let fieldnorm = block.fieldnorms[self.pos_in_block];
        let doc_len = smallfloat::decode(fieldnorm).max(1);

        let tf_f = tf as f32;
        let dl = doc_len as f32;

        let tf_norm = (tf_f * (params.k1 + 1.0))
            / (tf_f + params.k1 * (1.0 - params.b + params.b * dl / avg_doc_len));

        self.idf * tf_norm
    }
}

/// Run BMW scoring across multiple term posting lists.
///
/// Returns the top-k `(score, doc_id_u32)` results.
///
/// When `prefilter` is `Some`, only surrogates present in the bitmap are
/// scored; all others are skipped before any BM25 computation.
pub fn bmw_score(
    term_blocks: &[TermBlocks],
    total_docs: u32,
    avg_doc_len: f32,
    params: &Bm25Params,
    top_k: usize,
    prefilter: Option<&SurrogateBitmap>,
) -> TopKHeap {
    let mut heap = TopKHeap::new(top_k);

    if term_blocks.is_empty() {
        return heap;
    }

    let mut cursors: Vec<TermCursor> = term_blocks
        .iter()
        .filter(|tb| tb.df > 0)
        .map(|tb| TermCursor::new(tb, total_docs, avg_doc_len, params))
        .collect();

    if cursors.is_empty() {
        return heap;
    }

    loop {
        // Sort cursors by current doc_id (ascending). Exhausted cursors go to the end.
        cursors.sort_by_key(|c| c.current_doc_id());

        // Remove exhausted cursors.
        while cursors.last().is_some_and(|c| c.is_exhausted()) {
            cursors.pop();
        }
        if cursors.is_empty() {
            break;
        }

        let threshold = heap.threshold();

        // WAND pivot selection: accumulate max_scores until sum > threshold.
        let mut pivot_idx = 0;
        let mut accumulated = 0.0f32;
        for (i, cursor) in cursors.iter().enumerate() {
            accumulated += cursor.max_score;
            pivot_idx = i;
            if accumulated > threshold {
                break;
            }
        }

        // If even all terms' max scores can't beat threshold, we're done.
        if accumulated <= threshold {
            break;
        }

        let pivot_doc_id = cursors[pivot_idx].current_doc_id();
        if pivot_doc_id == EXHAUSTED {
            break;
        }

        // Check if all cursors [0..=pivot_idx] point to the same doc_id.
        let first_doc_id = cursors[0].current_doc_id();
        if first_doc_id == pivot_doc_id {
            // Prefilter: skip surrogates not present in the bitmap.
            if let Some(bm) = prefilter
                && !bm.contains(pivot_doc_id)
            {
                for cursor in &mut cursors {
                    if cursor.current_doc_id() == pivot_doc_id {
                        cursor.next();
                    }
                }
                continue;
            }

            // All essential terms are at the pivot doc — score it.
            // But first: block-level pruning. Sum block upper bounds.
            let mut block_upper = 0.0f32;
            for cursor in cursors.iter().take(pivot_idx + 1) {
                block_upper += cursor.block_upper_bound(total_docs, avg_doc_len, params);
            }

            if block_upper > threshold {
                // Actually score the document.
                let mut doc_score = 0.0f32;
                for cursor in cursors.iter() {
                    if cursor.current_doc_id() == pivot_doc_id {
                        doc_score += cursor.score_current(avg_doc_len, params);
                    }
                }
                heap.insert(doc_score, pivot_doc_id);
            }

            // Advance all cursors that were at pivot_doc_id.
            for cursor in &mut cursors {
                if cursor.current_doc_id() == pivot_doc_id {
                    cursor.next();
                }
            }
        } else {
            // Not all essential cursors at pivot — advance cursors before pivot to pivot_doc_id.
            for cursor in cursors.iter_mut().take(pivot_idx) {
                if cursor.current_doc_id() < pivot_doc_id {
                    cursor.advance_to(pivot_doc_id);
                }
            }
        }
    }

    heap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::CompactPosting;
    use crate::codec::smallfloat;

    fn make_term(doc_ids: &[u32], tf: u32) -> TermBlocks {
        let postings: Vec<CompactPosting> = doc_ids
            .iter()
            .map(|&id| CompactPosting {
                doc_id: Surrogate(id),
                term_freq: tf,
                fieldnorm: smallfloat::encode(100),
                positions: vec![0],
            })
            .collect();
        let blocks = crate::block::into_blocks(postings);
        TermBlocks::from_blocks(blocks)
    }

    #[test]
    fn bmw_basic() {
        let term_a = make_term(&[0, 1, 2, 3, 4], 2);
        let term_b = make_term(&[2, 3, 5, 6], 3);

        let params = Bm25Params::default();
        let heap = bmw_score(&[term_a, term_b], 100, 100.0, &params, 3, None);

        let results = heap.into_sorted();
        assert!(!results.is_empty());
        // Docs 2 and 3 match both terms — should score highest.
        let top_ids: Vec<Surrogate> = results.iter().map(|r| r.doc_id).collect();
        assert!(top_ids.contains(&Surrogate(2)));
        assert!(top_ids.contains(&Surrogate(3)));
    }

    #[test]
    fn bmw_single_term() {
        let term = make_term(&[10, 20, 30, 40, 50], 1);
        let params = Bm25Params::default();
        let heap = bmw_score(&[term], 1000, 100.0, &params, 3, None);

        let results = heap.into_sorted();
        assert_eq!(results.len(), 3);
        // All have the same score (same tf, same doc_len).
    }

    #[test]
    fn bmw_empty_terms() {
        let params = Bm25Params::default();
        let heap = bmw_score(&[], 1000, 100.0, &params, 10, None);
        assert!(heap.is_empty());
    }

    #[test]
    fn bmw_respects_top_k() {
        let ids: Vec<u32> = (0..500).collect();
        let term = make_term(&ids, 1);
        let params = Bm25Params::default();
        let heap = bmw_score(&[term], 1000, 100.0, &params, 5, None);

        let results = heap.into_sorted();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn bmw_large_collection() {
        // Many docs for term_a, few for term_b (rare = high IDF).
        let common_ids: Vec<u32> = (0..1000).collect();
        let rare_ids = vec![50, 200, 500];

        let term_common = make_term(&common_ids, 1);
        let term_rare = make_term(&rare_ids, 5);

        let params = Bm25Params::default();
        let heap = bmw_score(&[term_common, term_rare], 10_000, 100.0, &params, 3, None);

        let results = heap.into_sorted();
        assert_eq!(results.len(), 3);
        // The rare-term docs (50, 200, 500) should dominate due to high IDF + tf.
        let top_ids: Vec<Surrogate> = results.iter().map(|r| r.doc_id).collect();
        assert!(top_ids.contains(&Surrogate(50)));
        assert!(top_ids.contains(&Surrogate(200)));
        assert!(top_ids.contains(&Surrogate(500)));
    }
}
