//! Inverted index for full-text search with BM25 scoring.
//!
//! Stores term → posting list mappings in redb. Each posting records
//! the document ID, term frequency, and token positions (for phrase matching).
//!
//! Search logic is in `inverted_search.rs`, highlighting in `inverted_highlight.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use tracing::{debug, warn};

use super::text_analyzer;

/// Inverted index table: key = "term", value = MessagePack-encoded Vec<Posting>.
pub(super) const POSTINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("text.postings");

/// Document length table: key = "doc_id", value = u32 (number of tokens).
pub(super) const DOC_LENGTHS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("text.doc_lengths");

/// Index metadata: key = name, value = MessagePack bytes.
const INDEX_META: TableDefinition<&str, &[u8]> = TableDefinition::new("text.meta");

/// Default BM25 parameters. Sourced from `SparseTuning::bm25_k1` / `bm25_b` at runtime.
pub(super) const DEFAULT_BM25_K1: f32 = 1.2;
pub(super) const DEFAULT_BM25_B: f32 = 0.75;

/// A single posting entry for a term in a document.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Posting {
    pub doc_id: String,
    pub term_freq: u32,
    pub positions: Vec<u32>,
}

/// Boolean query mode for multi-term searches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryMode {
    /// All query terms must match (intersection). Default.
    #[default]
    And,
    /// Any query term can match (union).
    Or,
}

/// A scored search result from the inverted index.
#[derive(Debug, Clone)]
pub struct TextSearchResult {
    pub doc_id: String,
    pub score: f32,
    /// Whether any result came from fuzzy matching.
    pub fuzzy: bool,
}

/// A character-level offset of a matched term in the original text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchOffset {
    pub start: usize,
    pub end: usize,
    pub term: String,
}

/// Full-text inverted index backed by redb.
pub struct InvertedIndex {
    db: Arc<Database>,
    /// BM25 term saturation parameter. Set from `SparseTuning::bm25_k1`.
    pub(super) bm25_k1: f32,
    /// BM25 length normalization parameter. Set from `SparseTuning::bm25_b`.
    pub(super) bm25_b: f32,
}

impl InvertedIndex {
    /// Open or create an inverted index at the given redb database.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let write_txn = db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("init tables: {e}"),
        })?;
        {
            write_txn
                .open_table(POSTINGS)
                .map_err(|e| crate::Error::Storage {
                    engine: "inverted".into(),
                    detail: format!("create postings table: {e}"),
                })?;
            write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| crate::Error::Storage {
                    engine: "inverted".into(),
                    detail: format!("create doc_lengths table: {e}"),
                })?;
            write_txn
                .open_table(INDEX_META)
                .map_err(|e| crate::Error::Storage {
                    engine: "inverted".into(),
                    detail: format!("create index_meta table: {e}"),
                })?;
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("commit init: {e}"),
        })?;

        Ok(Self {
            db,
            bm25_k1: DEFAULT_BM25_K1,
            bm25_b: DEFAULT_BM25_B,
        })
    }

    /// Access the underlying database (for sub-modules).
    pub(super) fn db(&self) -> &Database {
        &self.db
    }

    /// Index a document's text content.
    pub fn index_document(&self, collection: &str, doc_id: &str, text: &str) -> crate::Result<()> {
        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("write txn: {e}"),
        })?;
        self.write_index_data(&write_txn, collection, doc_id, text)?;
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("commit index: {e}"),
        })?;
        Ok(())
    }

    /// Index a document within an externally-owned write transaction.
    pub fn index_document_in_txn(
        &self,
        txn: &WriteTransaction,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> crate::Result<()> {
        self.write_index_data(txn, collection, doc_id, text)
    }

    /// Core indexing logic shared by both index methods.
    fn write_index_data(
        &self,
        txn: &WriteTransaction,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> crate::Result<()> {
        let tokens = text_analyzer::analyze(text);
        if tokens.is_empty() {
            return Ok(());
        }

        let mut term_postings: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_postings
                .entry(token.as_str())
                .or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let scoped_doc_id = format!("{collection}:{doc_id}");
        let doc_len = tokens.len() as u32;

        let mut postings_table = txn
            .open_table(POSTINGS)
            .map_err(|e| crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("open postings: {e}"),
            })?;

        for (term, (freq, positions)) in &term_postings {
            let term_key = format!("{collection}:{term}");
            let posting = Posting {
                doc_id: scoped_doc_id.clone(),
                term_freq: *freq,
                positions: positions.clone(),
            };

            let mut existing: Vec<Posting> = postings_table
                .get(term_key.as_str())
                .ok()
                .flatten()
                .and_then(|v| rmp_serde::from_slice(v.value()).ok())
                .unwrap_or_default();

            existing.retain(|p| p.doc_id != scoped_doc_id);
            existing.push(posting);

            let bytes = rmp_serde::to_vec_named(&existing).map_err(|e| crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("serialize postings: {e}"),
            })?;
            postings_table
                .insert(term_key.as_str(), bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "inverted".into(),
                    detail: format!("insert posting: {e}"),
                })?;
        }
        drop(postings_table);

        let mut lengths = txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("open doc_lengths: {e}"),
            })?;
        let len_bytes = rmp_serde::to_vec_named(&doc_len).map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("serialize doc_len: {e}"),
        })?;
        lengths
            .insert(scoped_doc_id.as_str(), len_bytes.as_slice())
            .map_err(|e| crate::Error::Storage {
                engine: "inverted".into(),
                detail: format!("insert doc_len: {e}"),
            })?;

        debug!(%collection, %doc_id, tokens = tokens.len(), terms = term_postings.len(), "indexed document");
        Ok(())
    }

    /// Remove a document from the inverted index.
    pub fn remove_document(&self, collection: &str, doc_id: &str) -> crate::Result<()> {
        let scoped_doc_id = format!("{collection}:{doc_id}");

        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("write txn: {e}"),
        })?;
        {
            let mut postings_table =
                write_txn
                    .open_table(POSTINGS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "inverted".into(),
                        detail: format!("open postings: {e}"),
                    })?;

            let prefix = format!("{collection}:");
            let end = format!("{collection}:\u{ffff}");
            let keys: Vec<String> = postings_table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| crate::Error::Storage {
                    engine: "inverted".into(),
                    detail: format!("range: {e}"),
                })?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();

            let mut updates: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for key in &keys {
                if let Ok(Some(val)) = postings_table.get(key.as_str()) {
                    let mut list: Vec<Posting> =
                        rmp_serde::from_slice(val.value()).unwrap_or_default();
                    let before = list.len();
                    list.retain(|p| p.doc_id != scoped_doc_id);
                    if list.len() != before {
                        if list.is_empty() {
                            updates.push((key.clone(), None));
                        } else {
                            let bytes = rmp_serde::to_vec_named(&list).unwrap_or_default();
                            updates.push((key.clone(), Some(bytes)));
                        }
                    }
                }
            }

            for (key, new_val) in &updates {
                match new_val {
                    None => {
                        if let Err(e) = postings_table.remove(key.as_str()) {
                            warn!(%collection, %doc_id, error = %e, "failed to remove posting");
                        }
                    }
                    Some(bytes) => {
                        if let Err(e) = postings_table.insert(key.as_str(), bytes.as_slice()) {
                            warn!(%collection, %doc_id, error = %e, "failed to update posting");
                        }
                    }
                }
            }

            let mut lengths =
                write_txn
                    .open_table(DOC_LENGTHS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "inverted".into(),
                        detail: format!("open doc_lengths: {e}"),
                    })?;
            if let Err(e) = lengths.remove(scoped_doc_id.as_str()) {
                warn!(%collection, %doc_id, error = %e, "failed to remove doc length");
            }
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "inverted".into(),
            detail: format!("commit remove: {e}"),
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(Database::create(&path).unwrap());
        let idx = InvertedIndex::open(db).unwrap();
        (idx, dir)
    }

    #[test]
    fn index_and_search() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "The quick brown fox jumps over the lazy dog")
            .unwrap();
        idx.index_document("docs", "d2", "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document("docs", "d3", "Rust programming language for systems")
            .unwrap();

        let results = idx.search("docs", "brown fox", 10, false).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "docs:d1");
    }

    #[test]
    fn search_with_stemming() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "running distributed databases")
            .unwrap();
        idx.index_document("docs", "d2", "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search("docs", "database distribution", 10, false)
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "docs:d1");
    }

    #[test]
    fn fuzzy_search() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "distributed database systems")
            .unwrap();

        let results = idx.search("docs", "databse", 10, true).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn remove_document() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "hello world").unwrap();
        idx.index_document("docs", "d2", "hello rust").unwrap();

        idx.remove_document("docs", "d1").unwrap();

        let results = idx.search("docs", "hello", 10, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "docs:d2");
    }

    #[test]
    fn empty_query() {
        let (idx, _dir) = open_temp();
        idx.index_document("docs", "d1", "some text here").unwrap();

        let results = idx.search("docs", "the a is", 10, false).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let (idx, _dir) = open_temp();
        idx.index_document("col_a", "d1", "alpha bravo charlie")
            .unwrap();
        idx.index_document("col_b", "d1", "delta echo foxtrot")
            .unwrap();

        let results = idx.search("col_a", "alpha", 10, false).unwrap();
        assert_eq!(results.len(), 1);

        let results = idx.search("col_b", "alpha", 10, false).unwrap();
        assert!(results.is_empty());
    }
}
