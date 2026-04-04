//! Core FtsIndex: indexing and document management over any backend.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

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
    pub fn load_doc_id_map(&self, collection: &str) -> Result<DocIdMap, B::Error> {
        let key = format!("{collection}:docmap");
        match self.backend.read_meta(&key)? {
            Some(bytes) => Ok(DocIdMap::from_bytes(&bytes).unwrap_or_default()),
            None => Ok(DocIdMap::new()),
        }
    }

    /// Persist the DocIdMap for a collection to backend metadata.
    fn save_doc_id_map(&self, collection: &str, map: &DocIdMap) -> Result<(), B::Error> {
        let key = format!("{collection}:docmap");
        self.backend.write_meta(&key, &map.to_bytes())
    }

    /// Index a document's text content.
    ///
    /// Analyzes `text` into tokens, writes postings to the LSM memtable,
    /// and flushes to an immutable segment when the threshold is reached.
    pub fn index_document(
        &self,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> Result<(), B::Error> {
        let tokens = self.analyze_for_collection(collection, text)?;
        if tokens.is_empty() {
            return Ok(());
        }

        // Assign u32 ID and persist map.
        let mut doc_map = self.load_doc_id_map(collection)?;
        let int_id = doc_map.get_or_assign(doc_id);
        self.save_doc_id_map(collection, &doc_map)?;

        // Build per-term frequency and position data.
        let mut term_data: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_data.entry(token.as_str()).or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;

        // Write to LSM memtable (CompactPosting with u32 doc ID).
        for (term, (freq, positions)) in &term_data {
            let compact = CompactPosting {
                doc_id: int_id,
                term_freq: *freq,
                fieldnorm: smallfloat::encode(doc_len),
                positions: positions.clone(),
            };
            let scoped_term = format!("{collection}:{term}");
            self.memtable.insert(&scoped_term, compact);
        }
        self.memtable.record_doc(int_id, doc_len);

        // Write document length, fieldnorm, and update incremental stats.
        // Note: postings are NOT written to the backend — they live in the LSM
        // memtable (and segments after flush). The backend stores only metadata
        // (doc lengths, stats, DocIdMap, fieldnorms). Origin's transaction-based
        // writes bypass FtsIndex entirely and write directly to redb tables.
        self.backend.write_doc_length(collection, doc_id, doc_len)?;
        self.write_fieldnorm(collection, int_id, doc_len)?;
        self.backend.increment_stats(collection, doc_len)?;

        // Check if memtable needs flushing.
        if self.memtable.should_flush() {
            self.flush_memtable(collection)?;
        }

        debug!(%collection, %doc_id, int_id, tokens = tokens.len(), terms = term_data.len(), "indexed document");
        Ok(())
    }

    /// Flush the active memtable to an immutable segment.
    fn flush_memtable(&self, collection: &str) -> Result<(), B::Error> {
        let drained = self.memtable.drain();
        if drained.is_empty() {
            return Ok(());
        }

        let segment_bytes = seg_writer::flush_to_segment(drained);
        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let key = compaction::segment_key(collection, seg_id, 0);
        self.backend.write_segment(&key, &segment_bytes)?;

        debug!(%collection, seg_id, bytes = segment_bytes.len(), "flushed memtable to segment");
        Ok(())
    }

    /// Remove a document from the index.
    ///
    /// Tombstones the document in the DocIdMap, removes from the LSM memtable,
    /// and decrements backend stats. Segment postings are filtered at query time
    /// via the DocIdMap tombstone.
    pub fn remove_document(&self, collection: &str, doc_id: &str) -> Result<(), B::Error> {
        // Read doc length before removing (needed for stats decrement).
        let doc_len = self.backend.read_doc_length(collection, doc_id)?;

        // Tombstone in DocIdMap and remove from memtable.
        let mut doc_map = self.load_doc_id_map(collection)?;
        if let Some(int_id) = doc_map.to_u32(doc_id) {
            self.memtable.remove_doc(int_id);
        }
        doc_map.remove(doc_id);
        self.save_doc_id_map(collection, &doc_map)?;

        self.backend.remove_doc_length(collection, doc_id)?;

        if let Some(len) = doc_len {
            self.backend.decrement_stats(collection, len)?;
        }

        Ok(())
    }

    /// Purge all entries for a collection. Returns count of removed entries.
    pub fn purge_collection(&self, collection: &str) -> Result<usize, B::Error> {
        // Selectively remove only this collection's terms from the shared memtable.
        self.memtable.drain_collection(collection);
        self.backend.purge_collection(collection)
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;

    use super::*;

    fn make_index() -> FtsIndex<MemoryBackend> {
        FtsIndex::new(MemoryBackend::new())
    }

    #[test]
    fn index_writes_to_memtable() {
        let idx = make_index();
        idx.index_document("docs", "d1", "hello world greeting")
            .unwrap();

        // Memtable should have postings.
        assert!(!idx.memtable.is_empty());
        assert!(idx.memtable.posting_count() > 0);
    }

    #[test]
    fn memtable_flush_on_threshold() {
        // Use a tiny threshold to trigger flush.
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

        // Index enough to exceed 5 postings.
        idx.index_document("docs", "d1", "alpha bravo charlie delta echo foxtrot")
            .unwrap();

        // After flush, memtable should be empty and a segment should exist.
        assert!(idx.memtable.is_empty());
        let segments = idx.backend.list_segments("docs").unwrap();
        assert!(!segments.is_empty(), "segment should have been written");
    }

    #[test]
    fn index_assigns_doc_ids() {
        let idx = make_index();
        idx.index_document("docs", "d1", "hello world greeting")
            .unwrap();
        idx.index_document("docs", "d2", "hello rust language")
            .unwrap();

        let map = idx.load_doc_id_map("docs").unwrap();
        assert_eq!(map.to_u32("d1"), Some(0));
        assert_eq!(map.to_u32("d2"), Some(1));
    }

    #[test]
    fn remove_tombstones_docmap() {
        let idx = make_index();
        idx.index_document("docs", "d1", "hello world").unwrap();
        idx.index_document("docs", "d2", "hello rust").unwrap();

        idx.remove_document("docs", "d1").unwrap();

        // DocIdMap should have d1 tombstoned.
        let map = idx.load_doc_id_map("docs").unwrap();
        assert_eq!(map.to_u32("d1"), None);
        assert_eq!(map.to_u32("d2"), Some(1));
    }

    #[test]
    fn index_updates_stats() {
        let idx = make_index();
        idx.index_document("docs", "d1", "hello world greeting")
            .unwrap();
        idx.index_document("docs", "d2", "hello rust language")
            .unwrap();

        let (count, total) = idx.backend.collection_stats("docs").unwrap();
        assert_eq!(count, 2);
        assert!(total > 0);
    }

    #[test]
    fn remove_decrements_stats() {
        let idx = make_index();
        idx.index_document("docs", "d1", "hello world").unwrap();
        idx.index_document("docs", "d2", "hello rust").unwrap();

        idx.remove_document("docs", "d1").unwrap();

        let (count, _) = idx.backend.collection_stats("docs").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn purge_collection_preserves_others() {
        let idx = make_index();
        idx.index_document("col_a", "d1", "alpha bravo").unwrap();
        idx.index_document("col_b", "d1", "delta echo").unwrap();

        idx.purge_collection("col_a").unwrap();
        assert_eq!(idx.backend.collection_stats("col_a").unwrap(), (0, 0));
        assert!(idx.backend.collection_stats("col_b").unwrap().0 > 0);

        // col_b's memtable postings should still be queryable.
        assert!(!idx.memtable.get_postings("col_b:delta").is_empty());
        // col_a's memtable postings should be gone.
        assert!(idx.memtable.get_postings("col_a:alpha").is_empty());
    }

    #[test]
    fn empty_text_is_noop() {
        let idx = make_index();
        idx.index_document("docs", "d1", "the a is").unwrap();
        assert_eq!(idx.backend.collection_stats("docs").unwrap(), (0, 0));
        assert!(idx.memtable.is_empty());
    }
}
