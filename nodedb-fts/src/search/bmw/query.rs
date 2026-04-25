//! BMW query entry point: merges memtable + segments via LSM layer,
//! runs BMW scoring on `Surrogate` row identities, resolves top-k back
//! to user-facing PK strings via the per-collection docmap.

use nodedb_types::Surrogate;

use crate::backend::FtsBackend;
use crate::block::{CompactPosting, into_blocks};
use crate::codec::{DocIdMap, smallfloat};
use crate::index::FtsIndex;
use crate::lsm::query as lsm_query;
use crate::posting::{Bm25Params, Posting, TextSearchResult};
use crate::search::bmw::skip_index::TermBlocks;

use super::scorer::bmw_score;

/// Corpus-level parameters for BMW search.
pub struct BmwParams<'a> {
    pub query_tokens: &'a [String],
    pub raw_tokens: &'a [String],
    pub fuzzy_enabled: bool,
    pub top_k: usize,
    pub total_docs: u32,
    pub avg_doc_len: f32,
    pub bm25: &'a Bm25Params,
}

/// Run BMW search over the FtsIndex.
pub fn bmw_search<B: FtsBackend>(
    index: &FtsIndex<B>,
    tid: u32,
    collection: &str,
    p: &BmwParams<'_>,
) -> Result<Option<Vec<TextSearchResult>>, B::Error> {
    let doc_map = index.load_doc_id_map(tid, collection)?;
    if doc_map.is_empty() {
        return Ok(None);
    }

    let mut has_fuzzy = vec![false; p.query_tokens.len()];

    let mut lsm_term_blocks = lsm_query::collect_merged_term_blocks(
        &index.backend,
        tid,
        collection,
        index.memtable(),
        p.query_tokens,
    )?;

    for (i, _token) in p.query_tokens.iter().enumerate() {
        if lsm_term_blocks[i].df == 0 && p.fuzzy_enabled {
            let raw = p.raw_tokens.get(i).unwrap_or(_token);
            let (posts, is_fuzzy) = index.fuzzy_lookup(tid, collection, raw)?;
            has_fuzzy[i] = is_fuzzy;
            if !posts.is_empty() {
                let compact = to_compact(&posts, &doc_map, index, tid, collection)?;
                let blocks = into_blocks(compact);
                lsm_term_blocks[i] = TermBlocks::from_blocks(blocks);
            }
        }
    }

    let all_term_blocks = lsm_term_blocks;

    let heap = bmw_score(
        &all_term_blocks,
        p.total_docs,
        p.avg_doc_len,
        p.bm25,
        p.top_k,
    );
    let scored = heap.into_sorted();

    let mut results = Vec::with_capacity(scored.len());
    for doc in &scored {
        let doc_id_str = match doc_map.to_string(doc.doc_id.0) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let is_fuzzy = has_fuzzy.iter().any(|&f| f);
        results.push(TextSearchResult {
            doc_id: doc_id_str,
            score: doc.score,
            fuzzy: is_fuzzy,
        });
    }

    Ok(Some(results))
}

/// Convert `Vec<Posting>` (String) → `Vec<CompactPosting>` (u32) via DocIdMap.
fn to_compact<B: FtsBackend>(
    postings: &[Posting],
    doc_map: &DocIdMap,
    index: &FtsIndex<B>,
    tid: u32,
    collection: &str,
) -> Result<Vec<CompactPosting>, B::Error> {
    let mut compact = Vec::with_capacity(postings.len());
    for p in postings {
        let Some(int_id) = doc_map.to_u32(&p.doc_id) else {
            continue;
        };
        let surrogate = Surrogate(int_id);
        let fieldnorm = index
            .read_fieldnorm(tid, collection, surrogate)?
            .map(smallfloat::encode)
            .unwrap_or_else(|| smallfloat::encode(p.term_freq));
        compact.push(CompactPosting {
            doc_id: surrogate,
            term_freq: p.term_freq,
            fieldnorm,
            positions: p.positions.clone(),
        });
    }
    Ok(compact)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;

    const T: u32 = 1;

    fn make_params<'a>(
        tokens: &'a [String],
        total: u32,
        avg: f32,
        top_k: usize,
        fuzzy: bool,
        bm25: &'a Bm25Params,
    ) -> BmwParams<'a> {
        BmwParams {
            query_tokens: tokens,
            raw_tokens: tokens,
            fuzzy_enabled: fuzzy,
            top_k,
            total_docs: total,
            avg_doc_len: avg,
            bm25,
        }
    }

    #[test]
    fn bmw_query_basic() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(
            T,
            "docs",
            "d1",
            "The quick brown fox jumps over the lazy dog",
        )
        .unwrap();
        idx.index_document(T, "docs", "d2", "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document(T, "docs", "d3", "Rust programming language for systems")
            .unwrap();

        let tokens = crate::analyze("brown fox");
        let (total, avg) = idx.index_stats(T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, total, avg, 10, false, &bm25);

        let results = bmw_search(&idx, T, "docs", &p).unwrap().unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn bmw_query_empty_collection() {
        let idx = FtsIndex::new(MemoryBackend::new());
        let tokens = crate::analyze("hello");
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, 0, 1.0, 10, false, &bm25);

        let result = bmw_search(&idx, T, "empty", &p).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn bmw_query_respects_top_k() {
        let idx = FtsIndex::new(MemoryBackend::new());
        for i in 0..50 {
            idx.index_document(T, "docs", &format!("d{i}"), &format!("common term word{i}"))
                .unwrap();
        }

        let tokens = crate::analyze("common term");
        let (total, avg) = idx.index_stats(T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, total, avg, 5, false, &bm25);

        let results = bmw_search(&idx, T, "docs", &p).unwrap().unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn bmw_query_with_fuzzy() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", "d1", "distributed database systems")
            .unwrap();

        let stemmed = crate::analyze("databse");
        let raw = crate::analyzer::pipeline::tokenize_no_stem("databse");
        let (total, avg) = idx.index_stats(T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = BmwParams {
            query_tokens: &stemmed,
            raw_tokens: &raw,
            fuzzy_enabled: true,
            top_k: 10,
            total_docs: total,
            avg_doc_len: avg,
            bm25: &bm25,
        };

        let result = bmw_search(&idx, T, "docs", &p);
        match &result {
            Ok(Some(r)) => assert!(!r.is_empty(), "BMW returned empty results"),
            Ok(None) => panic!("BMW returned None (no DocIdMap?)"),
            Err(e) => panic!("BMW returned Err: {e}"),
        }
    }

    #[test]
    fn bmw_query_uses_memtable() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", "d1", "hello world greeting")
            .unwrap();

        assert!(!idx.memtable().is_empty());

        let tokens = crate::analyze("hello");
        let (total, avg) = idx.index_stats(T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, total, avg, 10, false, &bm25);

        let results = bmw_search(&idx, T, "docs", &p).unwrap().unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }
}
