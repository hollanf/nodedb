//! Fuzzy term lookup for the FtsIndex.

use crate::backend::FtsBackend;
use crate::fuzzy;
use crate::index::FtsIndex;
use crate::lsm::query as lsm_query;
use crate::posting::Posting;

impl<B: FtsBackend> FtsIndex<B> {
    /// Find the best fuzzy-matching term and return its posting list.
    ///
    /// Uses the raw (unstemmed) query term for edit distance computation
    /// against stemmed index terms. Resolves postings from BOTH the backend
    /// posting store (for Origin's transaction-based writes) and the LSM
    /// layer (for FtsIndex writes that go to memtable only).
    pub(crate) fn fuzzy_lookup(
        &self,
        collection: &str,
        query_term: &str,
    ) -> Result<(Vec<Posting>, bool), B::Error> {
        // Collect terms from LSM layer (memtable + segments) + backend.
        let mut all_terms =
            lsm_query::collect_all_terms(&self.backend, collection, self.memtable())?;

        let backend_terms = self.backend.collection_terms(collection)?;
        for t in backend_terms {
            all_terms.push(t);
        }
        all_terms.sort();
        all_terms.dedup();

        let matches = fuzzy::fuzzy_match(query_term, all_terms.iter().map(String::as_str));

        if let Some((best_term, _dist)) = matches.first() {
            // Try backend posting store first (Origin's writes).
            let postings = self.backend.read_postings(collection, best_term)?;
            if !postings.is_empty() {
                return Ok((postings, true));
            }

            // Backend empty — resolve from LSM (FtsIndex writes go to memtable only).
            let doc_map = self.load_doc_id_map(collection)?;
            let tokens = vec![best_term.to_string()];
            let term_blocks = lsm_query::collect_merged_term_blocks(
                &self.backend,
                collection,
                self.memtable(),
                &tokens,
            )?;
            if !term_blocks.is_empty() && term_blocks[0].df > 0 {
                let mut postings = Vec::new();
                for block in &term_blocks[0].blocks {
                    for i in 0..block.doc_ids.len() {
                        if let Some(doc_str) = doc_map.to_string(block.doc_ids[i]) {
                            postings.push(Posting {
                                doc_id: doc_str.to_string(),
                                term_freq: block.term_freqs[i],
                                positions: block.positions[i].clone(),
                            });
                        }
                    }
                }
                if !postings.is_empty() {
                    return Ok((postings, true));
                }
            }
        }

        Ok((Vec::new(), false))
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;

    #[test]
    fn fuzzy_lookup_finds_close_term() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "distributed database systems")
            .unwrap();

        // Raw "databse" (7 chars) vs stemmed index term "databas" (7 chars).
        // levenshtein = 2, max_distance(7) = 2 → match.
        // Postings resolved from LSM (memtable), not backend.
        let (postings, is_fuzzy) = idx.fuzzy_lookup("docs", "databse").unwrap();
        assert!(is_fuzzy, "should find fuzzy match");
        assert!(!postings.is_empty(), "should return postings from LSM");
    }

    #[test]
    fn fuzzy_lookup_no_match() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "hello world").unwrap();

        let (postings, is_fuzzy) = idx.fuzzy_lookup("docs", "zzzzzzz").unwrap();
        assert!(postings.is_empty());
        assert!(!is_fuzzy);
    }
}
