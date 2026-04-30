//! Variable-length multi-vector document storage with Meta Token mode.
//!
//! `MultiVectorStore` holds a collection of `MultiVectorDoc` entries where
//! each document carries one or more embedding vectors.  In `MetaToken` mode
//! every document has exactly `k` vectors (MetaEmbed learnable summary tokens);
//! in `PerToken` mode the count is unconstrained (naive ColBERT).

use std::collections::HashMap;

/// A document represented by one or more embedding vectors.
#[derive(Debug, Clone)]
pub struct MultiVectorDoc {
    pub doc_id: u32,
    /// Variable-length list of vectors.  For Meta Token mode this has fixed length K.
    pub vectors: Vec<Vec<f32>>,
}

/// Controls how many vectors a document may contain and what they represent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MultiVecMode {
    /// Per-token: one vector per token (naive ColBERT, expensive).
    PerToken,
    /// Meta Token: K learnable summary vectors per document (MetaEmbed).
    MetaToken { k: u8 },
}

/// In-memory store for multi-vector documents.
pub struct MultiVectorStore {
    pub dim: usize,
    pub mode: MultiVecMode,
    docs: HashMap<u32, MultiVectorDoc>,
}

/// Errors produced by `MultiVectorStore`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MultivecError {
    #[error("dim mismatch: expected {expected}, got {actual}")]
    DimMismatch { expected: usize, actual: usize },
    #[error("meta-token count mismatch: expected k={expected}, got {actual}")]
    MetaTokenCountMismatch { expected: u8, actual: usize },
}

impl MultiVectorStore {
    /// Create a new store with the given embedding dimension and mode.
    pub fn new(dim: usize, mode: MultiVecMode) -> Self {
        Self {
            dim,
            mode,
            docs: HashMap::new(),
        }
    }

    /// Insert a document, validating dimensions and (for MetaToken mode) vector count.
    pub fn insert(&mut self, doc: MultiVectorDoc) -> Result<(), MultivecError> {
        // Validate each vector's dimension.
        for v in &doc.vectors {
            if v.len() != self.dim {
                return Err(MultivecError::DimMismatch {
                    expected: self.dim,
                    actual: v.len(),
                });
            }
        }

        // In MetaToken mode the count must equal k exactly.
        if let MultiVecMode::MetaToken { k } = self.mode
            && doc.vectors.len() != k as usize
        {
            return Err(MultivecError::MetaTokenCountMismatch {
                expected: k,
                actual: doc.vectors.len(),
            });
        }

        self.docs.insert(doc.doc_id, doc);
        Ok(())
    }

    /// Look up a document by ID.
    pub fn get(&self, doc_id: u32) -> Option<&MultiVectorDoc> {
        self.docs.get(&doc_id)
    }

    /// Number of documents currently stored.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// Returns `true` if the store holds no documents.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Iterate over all stored documents (order unspecified).
    pub fn iter(&self) -> impl Iterator<Item = &MultiVectorDoc> {
        self.docs.values()
    }

    /// Returns `Some(k)` for `MetaToken` mode; `None` for `PerToken`.
    pub fn k(&self) -> Option<u8> {
        match self.mode {
            MultiVecMode::MetaToken { k } => Some(k),
            MultiVecMode::PerToken => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_doc(doc_id: u32, n_vecs: usize, dim: usize) -> MultiVectorDoc {
        MultiVectorDoc {
            doc_id,
            vectors: (0..n_vecs).map(|_| vec![0.0f32; dim]).collect(),
        }
    }

    #[test]
    fn insert_per_token_valid() {
        let mut store = MultiVectorStore::new(4, MultiVecMode::PerToken);
        let doc = make_doc(1, 5, 4);
        assert!(store.insert(doc).is_ok());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn insert_dim_mismatch() {
        let mut store = MultiVectorStore::new(4, MultiVecMode::PerToken);
        let doc = MultiVectorDoc {
            doc_id: 2,
            vectors: vec![vec![0.0f32; 3]], // wrong dim
        };
        let err = store.insert(doc).unwrap_err();
        assert!(matches!(
            err,
            MultivecError::DimMismatch {
                expected: 4,
                actual: 3
            }
        ));
    }

    #[test]
    fn insert_meta_token_valid() {
        let mut store = MultiVectorStore::new(8, MultiVecMode::MetaToken { k: 4 });
        let doc = make_doc(10, 4, 8);
        assert!(store.insert(doc).is_ok());
        assert_eq!(store.k(), Some(4));
    }

    #[test]
    fn insert_meta_token_count_mismatch() {
        let mut store = MultiVectorStore::new(8, MultiVecMode::MetaToken { k: 4 });
        let doc = make_doc(10, 3, 8); // 3 vectors but k=4
        let err = store.insert(doc).unwrap_err();
        assert!(matches!(
            err,
            MultivecError::MetaTokenCountMismatch {
                expected: 4,
                actual: 3
            }
        ));
    }

    #[test]
    fn get_returns_inserted_doc() {
        let mut store = MultiVectorStore::new(2, MultiVecMode::PerToken);
        store.insert(make_doc(42, 2, 2)).unwrap();
        let doc = store.get(42).expect("doc should be present");
        assert_eq!(doc.doc_id, 42);
    }

    #[test]
    fn iter_yields_all_docs() {
        let mut store = MultiVectorStore::new(2, MultiVecMode::PerToken);
        for id in 0..5u32 {
            store.insert(make_doc(id, 1, 2)).unwrap();
        }
        assert_eq!(store.iter().count(), 5);
    }

    #[test]
    fn k_none_for_per_token() {
        let store = MultiVectorStore::new(4, MultiVecMode::PerToken);
        assert_eq!(store.k(), None);
    }
}
