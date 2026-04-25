//! Full-text search operations dispatched to the Data Plane.

use nodedb_types::SurrogateBitmap;

/// Full-text search physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TextOp {
    /// BM25 full-text search on the inverted index.
    Search {
        collection: String,
        query: String,
        top_k: usize,
        /// Enable fuzzy matching (Levenshtein) for typo tolerance.
        fuzzy: bool,
        /// Pre-computed bitmap of eligible surrogates (from prefilter evaluation).
        /// `None` = no prefilter; all postings are eligible.
        prefilter: Option<SurrogateBitmap>,
        /// RLS post-score filters (serialized `Vec<ScanFilter>`).
        /// Applied after BM25 scoring, before returning to client.
        /// Result count may be less than requested `top_k`.
        rls_filters: Vec<u8>,
    },

    /// Hybrid search: vector similarity + BM25 text, fused via RRF.
    HybridSearch {
        collection: String,
        query_vector: Vec<f32>,
        query_text: String,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        /// Weight for vector results in RRF (0.0–1.0). Default: 0.5.
        vector_weight: f32,
        filter_bitmap: Option<SurrogateBitmap>,
        /// RLS post-fusion filters.
        rls_filters: Vec<u8>,
    },
}
