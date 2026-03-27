//! BM25 search and fuzzy lookup for the inverted index.

use std::collections::HashMap;

use super::fuzzy;
use super::inverted::{DOC_LENGTHS, InvertedIndex, POSTINGS, Posting, QueryMode, TextSearchResult};
use super::text_analyzer;

impl InvertedIndex {
    /// Search the inverted index using BM25 scoring.
    pub fn search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.search_with_mode(collection, query, top_k, fuzzy_enabled, QueryMode::And)
    }

    /// Search with explicit boolean mode (AND or OR).
    pub fn search_with_mode(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        mode: QueryMode,
    ) -> crate::Result<Vec<TextSearchResult>> {
        let query_tokens = text_analyzer::analyze(query);
        if query_tokens.is_empty() {
            return Ok(Vec::new());
        }
        let num_query_terms = query_tokens.len();

        let read_txn = self.db().begin_read().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("read txn: {e}"),
        })?;
        let postings_table = read_txn
            .open_table(POSTINGS)
            .map_err(|e| crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("open postings: {e}"),
            })?;
        let lengths_table =
            read_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| crate::Error::Storage {
                    engine: "inverted".into(),
                    detail: format!("open doc_lengths: {e}"),
                })?;

        let (total_docs, avg_doc_len) = self.index_stats(collection)?;
        if total_docs == 0 {
            return Ok(Vec::new());
        }

        // (score, fuzzy_flag, term_match_count)
        let mut doc_scores: HashMap<String, (f32, bool, usize)> = HashMap::new();

        for token in &query_tokens {
            let term_key = format!("{collection}:{token}");

            let (postings, is_fuzzy) = if let Ok(Some(val)) = postings_table.get(term_key.as_str())
            {
                let list: Vec<Posting> = rmp_serde::from_slice(val.value()).unwrap_or_default();
                (list, false)
            } else if fuzzy_enabled {
                self.fuzzy_lookup(collection, token, &postings_table)?
            } else {
                (Vec::new(), false)
            };

            if postings.is_empty() {
                continue;
            }

            let df = postings.len() as f32;
            let idf = ((total_docs as f32 - df + 0.5) / (df + 0.5) + 1.0).ln();

            for posting in &postings {
                let doc_len = lengths_table
                    .get(posting.doc_id.as_str())
                    .ok()
                    .flatten()
                    .and_then(|v| rmp_serde::from_slice::<u32>(v.value()).ok())
                    .unwrap_or(1) as f32;

                let tf = posting.term_freq as f32;
                let tf_norm = (tf * (self.bm25_k1 + 1.0))
                    / (tf
                        + self.bm25_k1 * (1.0 - self.bm25_b + self.bm25_b * doc_len / avg_doc_len));
                let mut score = idf * tf_norm;

                if is_fuzzy {
                    score *= fuzzy::fuzzy_discount(1);
                }

                let entry = doc_scores
                    .entry(posting.doc_id.clone())
                    .or_insert((0.0, false, 0));
                entry.0 += score;
                if is_fuzzy {
                    entry.1 = true;
                }
                entry.2 += 1;
            }
        }

        if mode == QueryMode::And && num_query_terms > 1 {
            doc_scores.retain(|_, (_, _, match_count)| *match_count >= num_query_terms);
        }

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

        Ok(results)
    }

    /// Fuzzy lookup: find the best matching term in the collection's postings.
    fn fuzzy_lookup(
        &self,
        collection: &str,
        query_term: &str,
        postings_table: &redb::ReadOnlyTable<&str, &[u8]>,
    ) -> crate::Result<(Vec<Posting>, bool)> {
        let prefix = format!("{collection}:");
        let end = format!("{collection}:\u{ffff}");

        let terms: Vec<String> = postings_table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("fuzzy range: {e}"),
            })?
            .filter_map(|r| {
                r.ok()
                    .and_then(|(k, _)| k.value().strip_prefix(&prefix).map(String::from))
            })
            .collect();

        let matches = fuzzy::fuzzy_match(query_term, terms.iter().map(String::as_str));
        if let Some((best_term, _dist)) = matches.first() {
            let key = format!("{collection}:{best_term}");
            if let Ok(Some(val)) = postings_table.get(key.as_str()) {
                let list: Vec<Posting> = rmp_serde::from_slice(val.value()).unwrap_or_default();
                return Ok((list, true));
            }
        }

        Ok((Vec::new(), false))
    }

    /// Get total document count and average document length for a collection.
    pub(super) fn index_stats(&self, collection: &str) -> crate::Result<(usize, f32)> {
        let read_txn = self.db().begin_read().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("read txn: {e}"),
        })?;
        let lengths_table =
            read_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| crate::Error::Storage {
                    engine: "inverted".into(),
                    detail: format!("open doc_lengths: {e}"),
                })?;

        let prefix = format!("{collection}:");
        let end = format!("{collection}:\u{ffff}");
        let mut total_len = 0u64;
        let mut count = 0usize;

        for (_, val) in lengths_table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("range: {e}"),
            })?
            .flatten()
        {
            if let Ok(len) = rmp_serde::from_slice::<u32>(val.value()) {
                total_len += len as u64;
                count += 1;
            }
        }

        let avg = if count > 0 {
            total_len as f32 / count as f32
        } else {
            1.0
        };

        Ok((count, avg))
    }
}
