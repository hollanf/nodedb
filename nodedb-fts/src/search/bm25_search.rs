//! BM25 search over the FtsIndex with AND-first OR-fallback and phrase boost.

use std::collections::HashMap;

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::backend::FtsBackend;
use crate::bm25::bm25_score;
use crate::index::FtsIndex;
use crate::posting::{Posting, QueryMode, TextSearchResult};
use crate::search::phrase;

impl<B: FtsBackend> FtsIndex<B> {
    /// Search the index using BM25 scoring.
    pub fn search(
        &self,
        tid: u64,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        prefilter: Option<&SurrogateBitmap>,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        self.search_with_mode(
            tid,
            collection,
            query,
            top_k,
            fuzzy_enabled,
            QueryMode::And,
            prefilter,
        )
    }

    /// Search with explicit boolean mode (AND or OR).
    #[allow(clippy::too_many_arguments)]
    pub fn search_with_mode(
        &self,
        tid: u64,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        mode: QueryMode,
        prefilter: Option<&SurrogateBitmap>,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        let query_tokens = self.analyze_for_collection(tid, collection, query)?;
        if query_tokens.is_empty() {
            return Ok(Vec::new());
        }
        let num_query_terms = query_tokens.len();

        let raw_tokens = if fuzzy_enabled {
            self.tokenize_raw_for_collection(tid, collection, query)?
        } else {
            Vec::new()
        };

        let (total_docs, avg_doc_len) = self.index_stats(tid, collection)?;
        if total_docs == 0 {
            return Ok(Vec::new());
        }

        let bmw_params = super::bmw::query::BmwParams {
            query_tokens: &query_tokens,
            raw_tokens: &raw_tokens,
            fuzzy_enabled,
            top_k: if mode == QueryMode::And && num_query_terms > 1 {
                top_k.saturating_mul(3).max(20)
            } else {
                top_k
            },
            total_docs,
            avg_doc_len,
            bm25: &self.bm25_params,
            prefilter,
        };
        if let Ok(Some(bmw_results)) =
            super::bmw::query::bmw_search(self, tid, collection, &bmw_params)
        {
            if mode == QueryMode::Or || num_query_terms == 1 {
                return Ok(bmw_results.into_iter().take(top_k).collect());
            }

            let and_results = self.filter_and_mode(
                tid,
                collection,
                &query_tokens,
                &bmw_results,
                num_query_terms,
            )?;

            if !and_results.is_empty() {
                return Ok(and_results.into_iter().take(top_k).collect());
            }

            let penalized: Vec<TextSearchResult> = bmw_results
                .into_iter()
                .map(|mut r| {
                    let matched = self.count_term_matches(tid, collection, &query_tokens, r.doc_id);
                    let coverage = matched as f32 / num_query_terms as f32;
                    r.score *= coverage;
                    r
                })
                .collect();
            let mut sorted = penalized;
            sorted.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            sorted.truncate(top_k);
            return Ok(sorted);
        }

        // Fallback: exhaustive BM25 scoring reading directly from the backend.
        #[cfg(feature = "governor")]
        let _term_postings_guard = self.governor.as_ref().and_then(|gov| {
            let bytes = num_query_terms
                * (std::mem::size_of::<Vec<Posting>>() + std::mem::size_of::<bool>());
            gov.reserve(nodedb_mem::EngineId::Fts, bytes).ok()
        });
        let mut term_postings: Vec<(Vec<Posting>, bool)> = Vec::with_capacity(num_query_terms);
        for (i, token) in query_tokens.iter().enumerate() {
            let postings = self.backend.read_postings(tid, collection, token)?;
            if !postings.is_empty() {
                term_postings.push((postings, false));
            } else if fuzzy_enabled {
                let raw = raw_tokens
                    .get(i)
                    .map(String::as_str)
                    .unwrap_or(token.as_str());
                let (fuzzy_posts, is_fuzzy) = self.fuzzy_lookup(tid, collection, raw)?;
                term_postings.push((fuzzy_posts, is_fuzzy));
            } else {
                term_postings.push((Vec::new(), false));
            }
        }

        let mut doc_scores: HashMap<Surrogate, (f32, bool, usize)> = HashMap::new();

        for (token_idx, (postings, is_fuzzy)) in term_postings.iter().enumerate() {
            if postings.is_empty() {
                continue;
            }
            let df = postings.len() as u32;

            for posting in postings {
                // Prefilter: skip surrogates not present in the bitmap.
                if let Some(bm) = prefilter
                    && !bm.contains(posting.doc_id)
                {
                    continue;
                }

                let doc_len = self
                    .backend
                    .read_doc_length(tid, collection, posting.doc_id)?
                    .unwrap_or(1);

                let mut score = bm25_score(
                    posting.term_freq,
                    df,
                    doc_len,
                    total_docs,
                    avg_doc_len,
                    &self.bm25_params,
                );

                if *is_fuzzy {
                    score *= crate::fuzzy::fuzzy_discount(1);
                }

                let entry = doc_scores.entry(posting.doc_id).or_insert((0.0, false, 0));
                entry.0 += score;
                if *is_fuzzy {
                    entry.1 = true;
                }
                entry.2 += 1;
            }
            let _ = token_idx;
        }

        if num_query_terms >= 2 {
            let doc_postings_map = phrase::collect_doc_postings(&query_tokens, &term_postings);
            for (doc_id, token_postings) in &doc_postings_map {
                if let Some(entry) = doc_scores.get_mut(doc_id) {
                    let boost = phrase::phrase_boost(&query_tokens, token_postings);
                    entry.0 *= boost;
                }
            }
        }

        if mode == QueryMode::And && num_query_terms > 1 {
            let and_results: HashMap<Surrogate, (f32, bool, usize)> = doc_scores
                .iter()
                .filter(|(_, (_, _, match_count))| *match_count >= num_query_terms)
                .map(|(k, v)| (*k, *v))
                .collect();

            if !and_results.is_empty() {
                return Ok(Self::to_sorted_results(and_results, top_k));
            }

            for (score, _, match_count) in doc_scores.values_mut() {
                let coverage = *match_count as f32 / num_query_terms as f32;
                *score *= coverage;
            }
        }

        Ok(Self::to_sorted_results(doc_scores, top_k))
    }

    fn filter_and_mode(
        &self,
        tid: u64,
        collection: &str,
        query_tokens: &[String],
        candidates: &[TextSearchResult],
        num_terms: usize,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        let term_blocks = crate::lsm::query::collect_merged_term_blocks(
            &self.backend,
            tid,
            collection,
            self.memtable(),
            query_tokens,
            #[cfg(feature = "governor")]
            self.governor.as_ref(),
        )?;

        let mut results = Vec::new();
        for candidate in candidates {
            let surrogate = candidate.doc_id;
            let matched = term_blocks
                .iter()
                .filter(|tb| tb.blocks.iter().any(|b| b.doc_ids.contains(&surrogate)))
                .count();
            if matched >= num_terms {
                results.push(candidate.clone());
            }
        }
        Ok(results)
    }

    fn count_term_matches(
        &self,
        tid: u64,
        collection: &str,
        query_tokens: &[String],
        doc_id: Surrogate,
    ) -> usize {
        let term_blocks = match crate::lsm::query::collect_merged_term_blocks(
            &self.backend,
            tid,
            collection,
            self.memtable(),
            query_tokens,
            #[cfg(feature = "governor")]
            self.governor.as_ref(),
        ) {
            Ok(tb) => tb,
            Err(_) => return 0,
        };
        term_blocks
            .iter()
            .filter(|tb| tb.blocks.iter().any(|b| b.doc_ids.contains(&doc_id)))
            .count()
    }

    fn to_sorted_results(
        doc_scores: HashMap<Surrogate, (f32, bool, usize)>,
        top_k: usize,
    ) -> Vec<TextSearchResult> {
        let mut results: Vec<TextSearchResult> = doc_scores
            .into_iter()
            .map(|(doc_id, (score, fuzzy_flag, _))| TextSearchResult {
                doc_id,
                score,
                fuzzy: fuzzy_flag,
            })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::posting::QueryMode;

    const T: u64 = 1;
    const D1: Surrogate = Surrogate(1);
    const D2: Surrogate = Surrogate(2);
    const D3: Surrogate = Surrogate(3);

    fn make_index() -> FtsIndex<MemoryBackend> {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "The quick brown fox jumps over the lazy dog")
            .unwrap();
        idx.index_document(T, "docs", D2, "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document(T, "docs", D3, "Rust programming language for systems")
            .unwrap();
        idx
    }

    #[test]
    fn basic_search() {
        let idx = make_index();
        let results = idx.search(T, "docs", "brown fox", 10, false, None).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn search_with_stemming() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "running distributed databases")
            .unwrap();
        idx.index_document(T, "docs", D2, "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search(T, "docs", "database distribution", 10, false, None)
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn or_mode() {
        let idx = make_index();
        let results = idx
            .search_with_mode(T, "docs", "brown fox", 10, false, QueryMode::Or, None)
            .unwrap();
        assert!(results.len() >= 2);
    }

    #[test]
    fn and_mode_filters() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "Rust programming language")
            .unwrap();
        idx.index_document(T, "docs", D2, "Python programming language")
            .unwrap();

        let results = idx
            .search_with_mode(
                T,
                "docs",
                "rust programming",
                10,
                false,
                QueryMode::And,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn and_fallback_to_or() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "rust programming language")
            .unwrap();
        idx.index_document(T, "docs", D2, "python programming language")
            .unwrap();

        let results = idx
            .search(T, "docs", "rust python", 10, false, None)
            .unwrap();
        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.score > 0.0);
        }
    }

    #[test]
    fn and_no_fallback_when_results_exist() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "rust programming language")
            .unwrap();
        idx.index_document(T, "docs", D2, "python programming language")
            .unwrap();

        let results = idx
            .search(T, "docs", "rust programming", 10, false, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn empty_query() {
        let idx = make_index();
        let results = idx.search(T, "docs", "the a is", 10, false, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "col_a", D1, "alpha bravo charlie")
            .unwrap();
        idx.index_document(T, "col_b", D1, "delta echo foxtrot")
            .unwrap();

        assert_eq!(
            idx.search(T, "col_a", "alpha", 10, false, None)
                .unwrap()
                .len(),
            1
        );
        assert!(
            idx.search(T, "col_b", "alpha", 10, false, None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn fuzzy_search() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "distributed database systems")
            .unwrap();

        let results = idx.search(T, "docs", "databse", 10, true, None).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn phrase_boost_consecutive() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "the quick brown fox jumped")
            .unwrap();
        idx.index_document(T, "docs", D2, "a brown dog chased a fox")
            .unwrap();

        let results = idx
            .search_with_mode(T, "docs", "brown fox", 10, false, QueryMode::Or, None)
            .unwrap();
        assert!(results.len() >= 2);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn phrase_boost_no_effect_single_term() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", D1, "hello world").unwrap();

        let results = idx.search(T, "docs", "hello", 10, false, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn tenants_isolated() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(1, "docs", D1, "alpha bravo").unwrap();
        idx.index_document(2, "docs", D1, "charlie delta").unwrap();

        let r1 = idx.search(1, "docs", "alpha", 10, false, None).unwrap();
        let r2 = idx.search(2, "docs", "alpha", 10, false, None).unwrap();
        assert_eq!(r1.len(), 1);
        assert!(r2.is_empty());
    }

    #[test]
    fn prefilter_excludes_non_member_surrogates() {
        let idx = FtsIndex::new(MemoryBackend::new());

        // All three documents contain the query term "rust" to ensure
        // D2 and D3 would score highly without a prefilter.
        idx.index_document(T, "docs", D1, "rust language system")
            .unwrap();
        idx.index_document(T, "docs", D2, "rust rust rust rust rust")
            .unwrap();
        idx.index_document(T, "docs", D3, "rust rust rust rust rust rust")
            .unwrap();

        // Prefilter: only D1 is eligible.
        let mut bm = SurrogateBitmap::new();
        bm.insert(D1);

        let results = idx.search(T, "docs", "rust", 10, false, Some(&bm)).unwrap();

        // D2 and D3 score higher without prefilter, but must be absent with it.
        assert_eq!(results.len(), 1, "only D1 should be returned");
        assert_eq!(results[0].doc_id, D1);

        assert!(
            !results.iter().any(|r| r.doc_id == D2),
            "D2 must be excluded"
        );
        assert!(
            !results.iter().any(|r| r.doc_id == D3),
            "D3 must be excluded"
        );

        // Verify that without prefilter, D2/D3 would dominate.
        let all_results = idx.search(T, "docs", "rust", 10, false, None).unwrap();
        assert_eq!(all_results.len(), 3, "all docs returned without prefilter");
        assert!(
            all_results[0].doc_id == D2 || all_results[0].doc_id == D3,
            "D2 or D3 should lead without prefilter (higher tf)"
        );

        // Prefilter with empty bitmap: no results.
        let empty_bm = SurrogateBitmap::new();
        let empty_results = idx
            .search(T, "docs", "rust", 10, false, Some(&empty_bm))
            .unwrap();
        assert!(empty_results.is_empty(), "empty prefilter → no results");

        // Bitmap containing only D2 and D3 excludes D1.
        let mut bm23 = SurrogateBitmap::new();
        bm23.insert(D2);
        bm23.insert(D3);
        let results23 = idx
            .search(T, "docs", "rust", 10, false, Some(&bm23))
            .unwrap();
        assert_eq!(results23.len(), 2);
        assert!(!results23.iter().any(|r| r.doc_id == D1));
    }
}
