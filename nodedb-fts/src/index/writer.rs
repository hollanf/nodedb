//! Core FtsIndex: indexing and document management over any backend.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Surrogate;
use tracing::debug;

use crate::backend::FtsBackend;
use crate::block::CompactPosting;
use crate::codec::DocIdMap;
use crate::codec::smallfloat;
use crate::lsm::compaction;
use crate::lsm::memtable::{Memtable, MemtableConfig};
use crate::lsm::segment::writer as seg_writer;
use crate::posting::Bm25Params;

/// Full-text search index generic over storage backend.
///
/// Provides identical indexing, search, and highlighting logic
/// for Origin (redb), Lite (in-memory), and WASM deployments.
///
/// Writes accumulate in an in-memory `Memtable`. When the memtable
/// exceeds its threshold, it is flushed to an immutable segment
/// stored via the backend. Queries merge the active memtable with
/// all persisted segments.
pub struct FtsIndex<B: FtsBackend> {
    pub(crate) backend: B,
    pub(crate) bm25_params: Bm25Params,
    pub(crate) memtable: Memtable,
    /// Monotonic segment ID counter.
    next_segment_id: AtomicU64,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Create a new FTS index with the given backend and default BM25 params.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            bm25_params: Bm25Params::default(),
            memtable: Memtable::new(MemtableConfig::default()),
            next_segment_id: AtomicU64::new(1),
        }
    }

    /// Create a new FTS index with custom BM25 parameters.
    pub fn with_params(backend: B, params: Bm25Params) -> Self {
        Self {
            backend,
            bm25_params: params,
            memtable: Memtable::new(MemtableConfig::default()),
            next_segment_id: AtomicU64::new(1),
        }
    }

    /// Access the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Mutable access to the underlying backend.
    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Access the active memtable (for LSM query merging).
    pub fn memtable(&self) -> &Memtable {
        &self.memtable
    }

    /// Load the DocIdMap for a collection from backend metadata.
    pub fn load_doc_id_map(&self, tid: u32, collection: &str) -> Result<DocIdMap, B::Error> {
        match self.backend.read_meta(tid, collection, "docmap")? {
            Some(bytes) => Ok(DocIdMap::from_bytes(&bytes).unwrap_or_default()),
            None => Ok(DocIdMap::new()),
        }
    }

    /// Persist the DocIdMap for a collection to backend metadata.
    fn save_doc_id_map(&self, tid: u32, collection: &str, map: &DocIdMap) -> Result<(), B::Error> {
        self.backend
            .write_meta(tid, collection, "docmap", &map.to_bytes())
    }

    /// Index a document's text content.
    pub fn index_document(
        &self,
        tid: u32,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> Result<(), B::Error> {
        let tokens = self.analyze_for_collection(tid, collection, text)?;
        if tokens.is_empty() {
            return Ok(());
        }

        let mut doc_map = self.load_doc_id_map(tid, collection)?;
        let int_id = doc_map.get_or_assign(doc_id);
        self.save_doc_id_map(tid, collection, &doc_map)?;

        let mut term_data: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_data.entry(token.as_str()).or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;

        let surrogate = Surrogate(int_id);
        for (term, (freq, positions)) in &term_data {
            let compact = CompactPosting {
                doc_id: surrogate,
                term_freq: *freq,
                fieldnorm: smallfloat::encode(doc_len),
                positions: positions.clone(),
            };
            let scoped_term = memtable_key(tid, collection, term);
            self.memtable.insert(&scoped_term, compact);
        }
        self.memtable.record_doc(surrogate, doc_len);

        // Write document length, fieldnorm, and update incremental stats.
        self.backend
            .write_doc_length(tid, collection, doc_id, doc_len)?;
        self.write_fieldnorm(tid, collection, surrogate, doc_len)?;
        self.backend.increment_stats(tid, collection, doc_len)?;

        if self.memtable.should_flush() {
            self.flush_memtable(tid, collection)?;
        }

        debug!(tid, %collection, %doc_id, int_id, tokens = tokens.len(), terms = term_data.len(), "indexed document");
        Ok(())
    }

    /// Flush the active memtable to an immutable segment.
    fn flush_memtable(&self, tid: u32, collection: &str) -> Result<(), B::Error> {
        let drained = self.memtable.drain();
        if drained.is_empty() {
            return Ok(());
        }

        let segment_bytes = seg_writer::flush_to_segment(drained);
        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let id = compaction::segment_id(seg_id, 0);
        self.backend
            .write_segment(tid, collection, &id, &segment_bytes)?;

        debug!(tid, %collection, seg_id, bytes = segment_bytes.len(), "flushed memtable to segment");
        Ok(())
    }

    /// Remove a document from the index.
    pub fn remove_document(
        &self,
        tid: u32,
        collection: &str,
        doc_id: &str,
    ) -> Result<(), B::Error> {
        let doc_len = self.backend.read_doc_length(tid, collection, doc_id)?;

        let mut doc_map = self.load_doc_id_map(tid, collection)?;
        if let Some(int_id) = doc_map.to_u32(doc_id) {
            self.memtable.remove_doc(Surrogate(int_id));
        }
        doc_map.remove(doc_id);
        self.save_doc_id_map(tid, collection, &doc_map)?;

        self.backend.remove_doc_length(tid, collection, doc_id)?;

        if let Some(len) = doc_len {
            self.backend.decrement_stats(tid, collection, len)?;
        }

        Ok(())
    }

    /// Purge all entries for a collection. Returns count of removed entries.
    pub fn purge_collection(&self, tid: u32, collection: &str) -> Result<usize, B::Error> {
        self.memtable
            .drain_collection(&memtable_collection_prefix(tid, collection));
        self.backend.purge_collection(tid, collection)
    }

    /// Purge all entries for a tenant across every collection.
    pub fn purge_tenant(&self, tid: u32) -> Result<usize, B::Error> {
        self.memtable.drain_collection(&memtable_tenant_prefix(tid));
        self.backend.purge_tenant(tid)
    }
}

/// Memtable key format: `"{tid}:{collection}:{term}"`. The memtable is a
/// single in-memory map shared across tenants, so keys must carry the
/// full tenant + collection scope.
pub(crate) fn memtable_key(tid: u32, collection: &str, term: &str) -> String {
    format!("{tid}:{collection}:{term}")
}

/// Prefix used by `drain_collection` to remove all memtable entries for
/// a given `(tid, collection)`.
pub(crate) fn memtable_collection_prefix(tid: u32, collection: &str) -> String {
    format!("{tid}:{collection}:")
}

/// Prefix used to remove every memtable entry for a given tenant.
pub(crate) fn memtable_tenant_prefix(tid: u32) -> String {
    format!("{tid}:")
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;

    use super::*;

    const T: u32 = 1;

    fn make_index() -> FtsIndex<MemoryBackend> {
        FtsIndex::new(MemoryBackend::new())
    }

    #[test]
    fn index_writes_to_memtable() {
        let idx = make_index();
        idx.index_document(T, "docs", "d1", "hello world greeting")
            .unwrap();

        assert!(!idx.memtable.is_empty());
        assert!(idx.memtable.posting_count() > 0);
    }

    #[test]
    fn memtable_flush_on_threshold() {
        let backend = MemoryBackend::new();
        let idx = FtsIndex {
            backend,
            bm25_params: Bm25Params::default(),
            memtable: Memtable::new(MemtableConfig {
                max_postings: 5,
                max_terms: 100,
            }),
            next_segment_id: AtomicU64::new(1),
        };

        idx.index_document(T, "docs", "d1", "alpha bravo charlie delta echo foxtrot")
            .unwrap();

        assert!(idx.memtable.is_empty());
        let segments = idx.backend.list_segments(T, "docs").unwrap();
        assert!(!segments.is_empty(), "segment should have been written");
    }

    #[test]
    fn index_assigns_doc_ids() {
        let idx = make_index();
        idx.index_document(T, "docs", "d1", "hello world greeting")
            .unwrap();
        idx.index_document(T, "docs", "d2", "hello rust language")
            .unwrap();

        let map = idx.load_doc_id_map(T, "docs").unwrap();
        assert_eq!(map.to_u32("d1"), Some(0));
        assert_eq!(map.to_u32("d2"), Some(1));
    }

    #[test]
    fn remove_tombstones_docmap() {
        let idx = make_index();
        idx.index_document(T, "docs", "d1", "hello world").unwrap();
        idx.index_document(T, "docs", "d2", "hello rust").unwrap();

        idx.remove_document(T, "docs", "d1").unwrap();

        let map = idx.load_doc_id_map(T, "docs").unwrap();
        assert_eq!(map.to_u32("d1"), None);
        assert_eq!(map.to_u32("d2"), Some(1));
    }

    #[test]
    fn index_updates_stats() {
        let idx = make_index();
        idx.index_document(T, "docs", "d1", "hello world greeting")
            .unwrap();
        idx.index_document(T, "docs", "d2", "hello rust language")
            .unwrap();

        let (count, total) = idx.backend.collection_stats(T, "docs").unwrap();
        assert_eq!(count, 2);
        assert!(total > 0);
    }

    #[test]
    fn remove_decrements_stats() {
        let idx = make_index();
        idx.index_document(T, "docs", "d1", "hello world").unwrap();
        idx.index_document(T, "docs", "d2", "hello rust").unwrap();

        idx.remove_document(T, "docs", "d1").unwrap();

        let (count, _) = idx.backend.collection_stats(T, "docs").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn purge_collection_preserves_others() {
        let idx = make_index();
        idx.index_document(T, "col_a", "d1", "alpha bravo").unwrap();
        idx.index_document(T, "col_b", "d1", "delta echo").unwrap();

        idx.purge_collection(T, "col_a").unwrap();
        assert_eq!(idx.backend.collection_stats(T, "col_a").unwrap(), (0, 0));
        assert!(idx.backend.collection_stats(T, "col_b").unwrap().0 > 0);

        assert!(
            !idx.memtable
                .get_postings(&memtable_key(T, "col_b", "delta"))
                .is_empty()
        );
        assert!(
            idx.memtable
                .get_postings(&memtable_key(T, "col_a", "alpha"))
                .is_empty()
        );
    }

    #[test]
    fn empty_text_is_noop() {
        let idx = make_index();
        idx.index_document(T, "docs", "d1", "the a is").unwrap();
        assert_eq!(idx.backend.collection_stats(T, "docs").unwrap(), (0, 0));
        assert!(idx.memtable.is_empty());
    }
}
