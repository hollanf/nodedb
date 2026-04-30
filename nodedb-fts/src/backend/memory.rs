//! In-memory FTS backend for Lite and WASM deployments.
//!
//! All data lives in HashMaps behind `RefCell` for interior mutability,
//! matching the `&self` trait signature. Rebuilt from documents on cold
//! start — acceptable for edge-scale datasets.
//!
//! Keys are fully structural tuples `(tid, collection, …)` — tenant
//! isolation never depends on lexical-prefix ordering.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

use nodedb_types::Surrogate;

use crate::backend::FtsBackend;
use crate::posting::Posting;

/// In-memory backend error (infallible in practice, but trait requires it).
#[derive(Debug)]
pub struct MemoryError(String);

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "memory backend: {}", self.0)
    }
}

type TripleKey = (u64, String, String);
type DocLenKey = (u64, String, Surrogate);
type PairKey = (u64, String);

/// In-memory FTS backend backed by HashMaps keyed by `(tid, collection, …)`
/// tuples.
///
/// Uses `RefCell` for interior mutability so the `FtsBackend` trait
/// can use `&self` uniformly (redb has its own transactional isolation).
#[derive(Debug, Default)]
pub struct MemoryBackend {
    /// `(tid, collection, term) → posting list`.
    postings: RefCell<HashMap<TripleKey, Vec<Posting>>>,
    /// `(tid, collection, doc_id) → token count`.
    doc_lengths: RefCell<HashMap<DocLenKey, u32>>,
    /// `(tid, collection) → (doc_count, total_token_sum)`.
    stats: RefCell<HashMap<PairKey, (u32, u64)>>,
    /// `(tid, collection, subkey) → blob` for docmap, fieldnorms, analyzer, language.
    meta: RefCell<HashMap<TripleKey, Vec<u8>>>,
    /// `(tid, collection, segment_id) → compressed segment bytes`.
    segments: RefCell<HashMap<TripleKey, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

fn triple(tid: u64, collection: &str, sub: &str) -> TripleKey {
    (tid, collection.to_string(), sub.to_string())
}

fn doc_len_key(tid: u64, collection: &str, doc_id: Surrogate) -> DocLenKey {
    (tid, collection.to_string(), doc_id)
}

fn pair(tid: u64, collection: &str) -> PairKey {
    (tid, collection.to_string())
}

impl FtsBackend for MemoryBackend {
    type Error = MemoryError;

    fn read_postings(
        &self,
        tid: u64,
        collection: &str,
        term: &str,
    ) -> Result<Vec<Posting>, Self::Error> {
        Ok(self
            .postings
            .borrow()
            .get(&triple(tid, collection, term))
            .cloned()
            .unwrap_or_default())
    }

    fn write_postings(
        &self,
        tid: u64,
        collection: &str,
        term: &str,
        postings: &[Posting],
    ) -> Result<(), Self::Error> {
        let key = triple(tid, collection, term);
        let mut map = self.postings.borrow_mut();
        if postings.is_empty() {
            map.remove(&key);
        } else {
            map.insert(key, postings.to_vec());
        }
        Ok(())
    }

    fn remove_postings(&self, tid: u64, collection: &str, term: &str) -> Result<(), Self::Error> {
        self.postings
            .borrow_mut()
            .remove(&triple(tid, collection, term));
        Ok(())
    }

    fn read_doc_length(
        &self,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
    ) -> Result<Option<u32>, Self::Error> {
        Ok(self
            .doc_lengths
            .borrow()
            .get(&doc_len_key(tid, collection, doc_id))
            .copied())
    }

    fn write_doc_length(
        &self,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
        length: u32,
    ) -> Result<(), Self::Error> {
        self.doc_lengths
            .borrow_mut()
            .insert(doc_len_key(tid, collection, doc_id), length);
        Ok(())
    }

    fn remove_doc_length(
        &self,
        tid: u64,
        collection: &str,
        doc_id: Surrogate,
    ) -> Result<(), Self::Error> {
        self.doc_lengths
            .borrow_mut()
            .remove(&doc_len_key(tid, collection, doc_id));
        Ok(())
    }

    fn collection_terms(&self, tid: u64, collection: &str) -> Result<Vec<String>, Self::Error> {
        Ok(self
            .postings
            .borrow()
            .keys()
            .filter(|(t, c, _)| *t == tid && c == collection)
            .map(|(_, _, term)| term.clone())
            .collect())
    }

    fn collection_stats(&self, tid: u64, collection: &str) -> Result<(u32, u64), Self::Error> {
        Ok(self
            .stats
            .borrow()
            .get(&pair(tid, collection))
            .copied()
            .unwrap_or((0, 0)))
    }

    fn increment_stats(&self, tid: u64, collection: &str, doc_len: u32) -> Result<(), Self::Error> {
        let mut stats = self.stats.borrow_mut();
        let entry = stats.entry(pair(tid, collection)).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += doc_len as u64;
        Ok(())
    }

    fn decrement_stats(&self, tid: u64, collection: &str, doc_len: u32) -> Result<(), Self::Error> {
        let mut stats = self.stats.borrow_mut();
        let entry = stats.entry(pair(tid, collection)).or_insert((0, 0));
        entry.0 = entry.0.saturating_sub(1);
        entry.1 = entry.1.saturating_sub(doc_len as u64);
        Ok(())
    }

    fn read_meta(
        &self,
        tid: u64,
        collection: &str,
        subkey: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self
            .meta
            .borrow()
            .get(&triple(tid, collection, subkey))
            .cloned())
    }

    fn write_meta(
        &self,
        tid: u64,
        collection: &str,
        subkey: &str,
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.meta
            .borrow_mut()
            .insert(triple(tid, collection, subkey), value.to_vec());
        Ok(())
    }

    fn write_segment(
        &self,
        tid: u64,
        collection: &str,
        segment_id: &str,
        data: &[u8],
    ) -> Result<(), Self::Error> {
        self.segments
            .borrow_mut()
            .insert(triple(tid, collection, segment_id), data.to_vec());
        Ok(())
    }

    fn read_segment(
        &self,
        tid: u64,
        collection: &str,
        segment_id: &str,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self
            .segments
            .borrow()
            .get(&triple(tid, collection, segment_id))
            .cloned())
    }

    fn list_segments(&self, tid: u64, collection: &str) -> Result<Vec<String>, Self::Error> {
        Ok(self
            .segments
            .borrow()
            .keys()
            .filter(|(t, c, _)| *t == tid && c == collection)
            .map(|(_, _, seg)| seg.clone())
            .collect())
    }

    fn remove_segment(
        &self,
        tid: u64,
        collection: &str,
        segment_id: &str,
    ) -> Result<(), Self::Error> {
        self.segments
            .borrow_mut()
            .remove(&triple(tid, collection, segment_id));
        Ok(())
    }

    fn purge_collection(&self, tid: u64, collection: &str) -> Result<usize, Self::Error> {
        let match_tc = |(t, c, _): &&TripleKey| *t == tid && c == collection;

        let mut postings = self.postings.borrow_mut();
        let mut doc_lengths = self.doc_lengths.borrow_mut();
        let before = postings.len() + doc_lengths.len();
        postings.retain(|k, _| !(k.0 == tid && k.1 == collection));
        doc_lengths.retain(|k, _| !(k.0 == tid && k.1 == collection));
        self.stats.borrow_mut().remove(&pair(tid, collection));
        self.meta
            .borrow_mut()
            .retain(|k, _| !(k.0 == tid && k.1 == collection));
        self.segments
            .borrow_mut()
            .retain(|k, _| !(k.0 == tid && k.1 == collection));
        let after = postings.len() + doc_lengths.len();
        let _ = match_tc;
        Ok(before - after)
    }

    fn purge_tenant(&self, tid: u64) -> Result<usize, Self::Error> {
        let mut postings = self.postings.borrow_mut();
        let mut doc_lengths = self.doc_lengths.borrow_mut();
        let before = postings.len() + doc_lengths.len();
        postings.retain(|k, _| k.0 != tid);
        doc_lengths.retain(|k, _| k.0 != tid);
        self.stats.borrow_mut().retain(|k, _| k.0 != tid);
        self.meta.borrow_mut().retain(|k, _| k.0 != tid);
        self.segments.borrow_mut().retain(|k, _| k.0 != tid);
        let after = postings.len() + doc_lengths.len();
        Ok(before - after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: u64 = 1;

    #[test]
    fn roundtrip_postings() {
        let backend = MemoryBackend::new();
        let postings = vec![Posting {
            doc_id: Surrogate(1),
            term_freq: 2,
            positions: vec![0, 5],
        }];
        backend
            .write_postings(T, "col", "hello", &postings)
            .unwrap();

        let read = backend.read_postings(T, "col", "hello").unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].doc_id, Surrogate(1));
    }

    #[test]
    fn roundtrip_doc_lengths() {
        let backend = MemoryBackend::new();
        backend
            .write_doc_length(T, "col", Surrogate(1), 42)
            .unwrap();
        assert_eq!(
            backend.read_doc_length(T, "col", Surrogate(1)).unwrap(),
            Some(42)
        );

        backend.remove_doc_length(T, "col", Surrogate(1)).unwrap();
        assert_eq!(
            backend.read_doc_length(T, "col", Surrogate(1)).unwrap(),
            None
        );
    }

    #[test]
    fn incremental_stats() {
        let backend = MemoryBackend::new();
        backend.increment_stats(T, "col", 10).unwrap();
        backend.increment_stats(T, "col", 20).unwrap();
        assert_eq!(backend.collection_stats(T, "col").unwrap(), (2, 30));

        backend.decrement_stats(T, "col", 10).unwrap();
        assert_eq!(backend.collection_stats(T, "col").unwrap(), (1, 20));
    }

    #[test]
    fn stats_saturating_sub() {
        let backend = MemoryBackend::new();
        backend.decrement_stats(T, "col", 100).unwrap();
        assert_eq!(backend.collection_stats(T, "col").unwrap(), (0, 0));
    }

    #[test]
    fn purge_clears_stats_and_isolates_collections() {
        let backend = MemoryBackend::new();
        backend.increment_stats(T, "col", 10).unwrap();
        backend
            .write_doc_length(T, "col", Surrogate(1), 10)
            .unwrap();
        backend
            .write_postings(
                T,
                "col",
                "hello",
                &[Posting {
                    doc_id: Surrogate(1),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();

        backend.increment_stats(T, "other", 7).unwrap();
        backend
            .write_doc_length(T, "other", Surrogate(1), 7)
            .unwrap();
        backend
            .write_postings(
                T,
                "other",
                "world",
                &[Posting {
                    doc_id: Surrogate(1),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();

        backend.purge_collection(T, "col").unwrap();
        assert_eq!(backend.collection_stats(T, "col").unwrap(), (0, 0));
        assert!(backend.read_postings(T, "col", "hello").unwrap().is_empty());
        assert_eq!(
            backend.read_doc_length(T, "col", Surrogate(1)).unwrap(),
            None
        );

        assert_eq!(backend.collection_stats(T, "other").unwrap(), (1, 7));
        assert_eq!(backend.read_postings(T, "other", "world").unwrap().len(), 1);
        assert_eq!(
            backend.read_doc_length(T, "other", Surrogate(1)).unwrap(),
            Some(7)
        );
    }

    #[test]
    fn collection_terms() {
        let backend = MemoryBackend::new();
        backend
            .write_postings(
                T,
                "col",
                "hello",
                &[Posting {
                    doc_id: Surrogate(1),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();
        backend
            .write_postings(
                T,
                "col",
                "world",
                &[Posting {
                    doc_id: Surrogate(1),
                    term_freq: 1,
                    positions: vec![1],
                }],
            )
            .unwrap();

        let mut terms = backend.collection_terms(T, "col").unwrap();
        terms.sort();
        assert_eq!(terms, vec!["hello", "world"]);
    }

    #[test]
    fn segment_roundtrip() {
        let backend = MemoryBackend::new();
        let data = b"compressed segment bytes";
        backend.write_segment(T, "col", "id1", data).unwrap();
        assert_eq!(
            backend.read_segment(T, "col", "id1").unwrap(),
            Some(data.to_vec())
        );
        assert_eq!(backend.read_segment(T, "col", "missing").unwrap(), None);
    }

    #[test]
    fn segment_list_filters_by_collection() {
        let backend = MemoryBackend::new();
        backend.write_segment(T, "col", "a", b"a").unwrap();
        backend.write_segment(T, "col", "b", b"b").unwrap();
        backend.write_segment(T, "other", "c", b"c").unwrap();

        let mut segs = backend.list_segments(T, "col").unwrap();
        segs.sort();
        assert_eq!(segs, vec!["a", "b"]);

        let other = backend.list_segments(T, "other").unwrap();
        assert_eq!(other, vec!["c"]);
    }

    #[test]
    fn segment_remove() {
        let backend = MemoryBackend::new();
        backend.write_segment(T, "col", "id1", b"data").unwrap();
        backend.remove_segment(T, "col", "id1").unwrap();
        assert_eq!(backend.read_segment(T, "col", "id1").unwrap(), None);
    }

    #[test]
    fn purge_clears_segments() {
        let backend = MemoryBackend::new();
        backend.write_segment(T, "col", "a", b"a").unwrap();
        backend.write_segment(T, "other", "b", b"b").unwrap();

        backend.purge_collection(T, "col").unwrap();
        assert!(backend.list_segments(T, "col").unwrap().is_empty());
        assert_eq!(backend.list_segments(T, "other").unwrap().len(), 1);
    }

    #[test]
    fn purge_tenant_isolates_tenants() {
        let backend = MemoryBackend::new();
        backend.increment_stats(1, "col", 5).unwrap();
        backend.increment_stats(2, "col", 7).unwrap();
        backend
            .write_postings(
                1,
                "col",
                "t",
                &[Posting {
                    doc_id: Surrogate(1),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();
        backend
            .write_postings(
                2,
                "col",
                "t",
                &[Posting {
                    doc_id: Surrogate(1),
                    term_freq: 1,
                    positions: vec![0],
                }],
            )
            .unwrap();

        backend.purge_tenant(1).unwrap();
        assert_eq!(backend.collection_stats(1, "col").unwrap(), (0, 0));
        assert!(backend.read_postings(1, "col", "t").unwrap().is_empty());
        assert_eq!(backend.collection_stats(2, "col").unwrap(), (1, 7));
        assert_eq!(backend.read_postings(2, "col", "t").unwrap().len(), 1);
    }
}
