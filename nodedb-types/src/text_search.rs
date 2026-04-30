//! Text search parameter types shared across the NodeDb trait boundary.
//!
//! These are the user-facing knobs for full-text search queries. The
//! implementation (BM25 scoring, BMW pruning, fuzzy matching) lives in
//! `nodedb-fts`. These types are in `nodedb-types` so both `nodedb-client`
//! (trait definition) and all implementations can use them without pulling
//! in the full FTS engine as a dependency.

use serde::{Deserialize, Serialize};

/// Boolean query mode for full-text search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QueryMode {
    /// Any query term can match (union). Most permissive — best recall.
    #[default]
    Or,
    /// All query terms must match (intersection). More precise — best precision.
    And,
}

/// BM25 ranking parameters.
///
/// Controls how term frequency and document length affect scoring.
/// The defaults (`k1 = 1.2`, `b = 0.75`) are standard Okapi BM25 values
/// that work well across most corpora.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bm25Params {
    /// Term frequency saturation factor.
    /// Higher values give more weight to repeated terms. Range: 0.5–3.0.
    /// Default: 1.2.
    pub k1: f32,
    /// Length normalization factor.
    /// `0.0` = no length normalization, `1.0` = full normalization.
    /// Default: 0.75.
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Per-query parameters for full-text search.
///
/// These are the knobs that vary per-query. BM25 scoring parameters (`k1`, `b`)
/// are corpus-level settings configured at collection creation time — they depend
/// on document characteristics (length, vocabulary), not on individual queries.
///
/// Pass [`TextSearchParams::default()`] for standard OR-mode non-fuzzy search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchParams {
    /// Boolean query mode: `Or` (any term) or `And` (all terms).
    /// Default: `Or`.
    pub mode: QueryMode,
    /// Enable fuzzy (Levenshtein distance) matching for approximate lookup.
    /// Fuzzy hits are scored with a discount relative to exact matches.
    /// Default: `false`.
    pub fuzzy: bool,
}

impl Default for TextSearchParams {
    fn default() -> Self {
        Self {
            mode: QueryMode::Or,
            fuzzy: false,
        }
    }
}
