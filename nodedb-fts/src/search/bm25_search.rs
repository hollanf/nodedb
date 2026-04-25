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
        tid: u32,
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
        tid: u32,
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
                    let matched =
                        self.count_term_matches(tid, collection, &query_tokens, &r.doc_id);
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
        // Load the docmap once for surrogate prefilter lookups.
        let fallback_doc_map = if prefilter.is_some() {
            Some(self.load_doc_id_map(tid, collection)?)
        } else {
            None
        };

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

        let mut doc_scores: HashMap<String, (f32, bool, usize)> = HashMap::new();

        for (token_idx, (postings, is_fuzzy)) in term_postings.iter().enumerate() {
            if postings.is_empty() {
                continue;
            }
            let df = postings.len() as u32;

            for posting in postings {
                // Prefilter: skip surrogates not present in the bitmap.
                if let (Some(bm), Some(dm)) = (prefilter, fallback_doc_map.as_ref()) {
                    match dm.to_u32(&posting.doc_id).map(Surrogate) {
                        Some(s) if !bm.contains(s) => continue,
                        None => continue,
                        _ => {}
                    }
                }

                let doc_len = self
                    .backend
                    .read_doc_length(tid, collection, &posting.doc_id)?
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

                let entry = doc_scores
                    .entry(posting.doc_id.clone())
                    .or_insert((0.0, false, 0));
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
                if let Some(entry) = doc_scores.get_mut(doc_id.as_str()) {
                    let boost = phrase::phrase_boost(&query_tokens, token_postings);
                    entry.0 *= boost;
                }
            }
        }

        if mode == QueryMode::And && num_query_terms > 1 {
            let and_results: HashMap<String, (f32, bool, usize)> = doc_scores
                .iter()
                .filter(|(_, (_, _, match_count))| *match_count >= num_query_terms)
                .map(|(k, v)| (k.clone(), *v))
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
        tid: u32,
        collection: &str,
        query_tokens: &[String],
        candidates: &[TextSearchResult],
        num_terms: usize,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        let doc_map = self.load_doc_id_map(tid, collection)?;
        let term_blocks = crate::lsm::query::collect_merged_term_blocks(
            &self.backend,
            tid,
            collection,
            self.memtable(),
            query_tokens,
        )?;

        let mut results = Vec::new();
        for candidate in candidates {
            let surrogate = doc_map.to_u32(&candidate.doc_id).map(Surrogate);
            let matched = term_blocks
                .iter()
                .filter(|tb| {
                    surrogate.is_some_and(|sid| tb.blocks.iter().any(|b| b.doc_ids.contains(&sid)))
                })
                .count();
            if matched >= num_terms {
                results.push(candidate.clone());
            }
        }
        Ok(results)
    }

    fn count_term_matches(
        &self,
        tid: u32,
        collection: &str,
        query_tokens: &[String],
        doc_id: &str,
    ) -> usize {
        let doc_map = match self.load_doc_id_map(tid, collection) {
            Ok(m) => m,
            Err(_) => return 0,
        };
        let Some(int_id) = doc_map.to_u32(doc_id) else {
            return 0;
        };
        let surrogate = Surrogate(int_id);
        let term_blocks = match crate::lsm::query::collect_merged_term_blocks(
            &self.backend,
            tid,
            collection,
            self.memtable(),
            query_tokens,
        ) {
            Ok(tb) => tb,
            Err(_) => return 0,
        };
        term_blocks
            .iter()
            .filter(|tb| tb.blocks.iter().any(|b| b.doc_ids.contains(&surrogate)))
            .count()
    }

    fn to_sorted_results(
        doc_scores: HashMap<String, (f32, bool, usize)>,
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
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::posting::QueryMode;

    const T: u32 = 1;

    fn make_index() -> FtsIndex<MemoryBackend> {
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
        idx
    }

    #[test]
    fn basic_search() {
        let idx = make_index();
        let results = idx.search(T, "docs", "brown fox", 10, false, None).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn search_with_stemming() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", "d1", "running distributed databases")
            .unwrap();
        idx.index_document(T, "docs", "d2", "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search(T, "docs", "database distribution", 10, false, None)
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
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
        idx.index_document(T, "docs", "d1", "Rust programming language")
            .unwrap();
        idx.index_document(T, "docs", "d2", "Python programming language")
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
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn and_fallback_to_or() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", "d1", "rust programming language")
            .unwrap();
        idx.index_document(T, "docs", "d2", "python programming language")
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
        idx.index_document(T, "docs", "d1", "rust programming language")
            .unwrap();
        idx.index_document(T, "docs", "d2", "python programming language")
            .unwrap();

        let results = idx
            .search(T, "docs", "rust programming", 10, false, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d1");
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
        idx.index_document(T, "col_a", "d1", "alpha bravo charlie")
            .unwrap();
        idx.index_document(T, "col_b", "d1", "delta echo foxtrot")
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
        idx.index_document(T, "docs", "d1", "distributed database systems")
            .unwrap();

        let results = idx.search(T, "docs", "databse", 10, true, None).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn phrase_boost_consecutive() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", "d1", "the quick brown fox jumped")
            .unwrap();
        idx.index_document(T, "docs", "d2", "a brown dog chased a fox")
            .unwrap();

        let results = idx
            .search_with_mode(T, "docs", "brown fox", 10, false, QueryMode::Or, None)
            .unwrap();
        assert!(results.len() >= 2);
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn phrase_boost_no_effect_single_term() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(T, "docs", "d1", "hello world").unwrap();

        let results = idx.search(T, "docs", "hello", 10, false, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn tenants_isolated() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(1, "docs", "d1", "alpha bravo").unwrap();
        idx.index_document(2, "docs", "d1", "charlie delta")
            .unwrap();

        let r1 = idx.search(1, "docs", "alpha", 10, false, None).unwrap();
        let r2 = idx.search(2, "docs", "alpha", 10, false, None).unwrap();
        assert_eq!(r1.len(), 1);
        assert!(r2.is_empty());
    }

    #[test]
    fn prefilter_excludes_non_member_surrogates() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let idx = FtsIndex::new(MemoryBackend::new());

        // d1 → surrogate 0, d2 → surrogate 1, d3 → surrogate 2.
        // All three documents contain the query term "rust" to ensure
        // d2 and d3 would score highly without a prefilter.
        idx.index_document(T, "docs", "d1", "rust language system")
            .unwrap();
        idx.index_document(T, "docs", "d2", "rust rust rust rust rust")
            .unwrap();
        idx.index_document(T, "docs", "d3", "rust rust rust rust rust rust")
            .unwrap();

        // Confirm surrogates assigned in insertion order.
        let doc_map = idx.load_doc_id_map(T, "docs").unwrap();
        let s1 = doc_map
            .to_u32("d1")
            .map(Surrogate)
            .expect("d1 has surrogate");
        let s2 = doc_map
            .to_u32("d2")
            .map(Surrogate)
            .expect("d2 has surrogate");
        let s3 = doc_map
            .to_u32("d3")
            .map(Surrogate)
            .expect("d3 has surrogate");

        // Prefilter: only d1 is eligible.
        let mut bm = SurrogateBitmap::new();
        bm.insert(s1);

        let results = idx.search(T, "docs", "rust", 10, false, Some(&bm)).unwrap();

        // d2 and d3 score higher without prefilter, but must be absent with it.
        assert_eq!(results.len(), 1, "only d1 should be returned");
        assert_eq!(results[0].doc_id, "d1");

        // Also verify that d2 and d3 are absent (not just that count is 1).
        assert!(
            !results.iter().any(|r| r.doc_id == "d2"),
            "d2 must be excluded"
        );
        assert!(
            !results.iter().any(|r| r.doc_id == "d3"),
            "d3 must be excluded"
        );

        // Verify that without prefilter, d2/d3 would dominate.
        let all_results = idx.search(T, "docs", "rust", 10, false, None).unwrap();
        assert_eq!(all_results.len(), 3, "all docs returned without prefilter");
        assert!(
            all_results[0].doc_id == "d2" || all_results[0].doc_id == "d3",
            "d2 or d3 should lead without prefilter (higher tf)"
        );

        // Prefilter with empty bitmap: no results.
        let empty_bm = SurrogateBitmap::new();
        let empty_results = idx
            .search(T, "docs", "rust", 10, false, Some(&empty_bm))
            .unwrap();
        assert!(empty_results.is_empty(), "empty prefilter → no results");

        // Bitmap containing only s2 and s3 excludes d1.
        let mut bm23 = SurrogateBitmap::new();
        bm23.insert(s2);
        bm23.insert(s3);
        let results23 = idx
            .search(T, "docs", "rust", 10, false, Some(&bm23))
            .unwrap();
        assert_eq!(results23.len(), 2);
        assert!(!results23.iter().any(|r| r.doc_id == "d1"));
    }
}
