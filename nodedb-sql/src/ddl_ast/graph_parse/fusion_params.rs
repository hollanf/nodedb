//! Shared parameter extraction for graph-vector fusion SQL surfaces.
//!
//! Two syntaxes reach the same `GraphOp::RagFusion` executor today:
//!
//! - `GRAPH RAG FUSION ON <col> QUERY ARRAY[...] ...` (DSL form)
//! - `SEARCH <col> USING FUSION(ARRAY[...] ...)` (wrapped form)
//!
//! They use different keyword aliases for the same parameters
//! (`EXPANSION_DEPTH` vs `DEPTH`, `EDGE_LABEL` vs `LABEL`, `FINAL_TOP_K`
//! vs `TOP`). Both must extract the same typed bag so future fusion
//! variants (hybrid text+vector, multi-vector, etc.) can share this
//! code and cannot silently drop parameters the way substring-find
//! parsing did.

use super::super::statement::GraphDirection;
use super::helpers::{array_floats_after, float_pair_after, quoted_after, usize_after, word_after};
use super::tokenizer::{Tok, tokenize};

/// Keyword aliases for the shared fusion parameters.
///
/// Each fusion SQL surface picks one of the `*_KEYWORDS` constants below.
/// New fusion variants add their own constant rather than editing the
/// extractor.
pub struct FusionKeywords {
    pub vector_top_k: &'static str,
    pub expansion_depth: &'static str,
    pub edge_label: &'static str,
    pub final_top_k: &'static str,
    pub rrf_k: &'static str,
    pub vector_field: &'static str,
    pub direction: &'static str,
    pub max_visited: &'static str,
    /// Keyword that precedes `ARRAY[...]` in raw SQL (e.g. `QUERY` or
    /// `ARRAY` itself when there is no leading keyword).
    pub query_anchor: &'static str,
}

/// Keywords used by `GRAPH RAG FUSION ON ...`.
pub const RAG_FUSION_KEYWORDS: FusionKeywords = FusionKeywords {
    vector_top_k: "VECTOR_TOP_K",
    expansion_depth: "EXPANSION_DEPTH",
    edge_label: "EDGE_LABEL",
    final_top_k: "FINAL_TOP_K",
    rrf_k: "RRF_K",
    vector_field: "VECTOR_FIELD",
    direction: "DIRECTION",
    max_visited: "MAX_VISITED",
    query_anchor: "QUERY",
};

/// Keywords used by `SEARCH ... USING FUSION(...)`.
pub const SEARCH_FUSION_KEYWORDS: FusionKeywords = FusionKeywords {
    vector_top_k: "VECTOR_TOP_K",
    expansion_depth: "DEPTH",
    edge_label: "LABEL",
    final_top_k: "TOP",
    rrf_k: "RRF_K",
    vector_field: "VECTOR_FIELD",
    direction: "DIRECTION",
    max_visited: "MAX_VISITED",
    query_anchor: "ARRAY",
};

/// Typed parameter bag for every graph-vector fusion SQL surface.
///
/// All fields are optional at parse time — bounds, caps, and
/// "absent but required" errors are enforced at the pgwire boundary.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FusionParams {
    pub query_vector: Option<Vec<f32>>,
    pub vector_top_k: Option<usize>,
    pub expansion_depth: Option<usize>,
    pub edge_label: Option<String>,
    pub final_top_k: Option<usize>,
    pub rrf_k: Option<(f64, f64)>,
    pub vector_field: Option<String>,
    pub direction: Option<GraphDirection>,
    pub max_visited: Option<usize>,
}

impl FusionParams {
    pub(super) fn extract(toks: &[Tok<'_>], sql: &str, kw: &FusionKeywords) -> Self {
        let direction = match word_after(toks, kw.direction)
            .as_deref()
            .map(str::to_ascii_uppercase)
            .as_deref()
        {
            Some("IN") => Some(GraphDirection::In),
            Some("BOTH") => Some(GraphDirection::Both),
            Some("OUT") => Some(GraphDirection::Out),
            _ => None,
        };
        Self {
            query_vector: array_floats_after(sql, kw.query_anchor),
            vector_top_k: usize_after(toks, kw.vector_top_k),
            expansion_depth: usize_after(toks, kw.expansion_depth),
            edge_label: quoted_after(toks, kw.edge_label),
            final_top_k: usize_after(toks, kw.final_top_k),
            rrf_k: float_pair_after(toks, kw.rrf_k),
            vector_field: quoted_after(toks, kw.vector_field),
            direction,
            max_visited: usize_after(toks, kw.max_visited),
        }
    }
}

/// Parse `SEARCH <collection> USING FUSION(...)` into its collection name
/// and a typed [`FusionParams`]. Returns `None` when the SQL does not
/// match the expected shape.
///
/// Body extraction uses the same quote- and bracket-aware tokenizer as
/// the `GRAPH RAG FUSION` path, so a keyword-shaped string literal (e.g.
/// a label value `'TOP'`) cannot shadow a real parameter keyword.
pub fn parse_search_using_fusion(sql: &str) -> Option<(String, FusionParams)> {
    let toks = tokenize(sql);
    let collection = match toks.as_slice() {
        [Tok::Word(s), Tok::Word(c), Tok::Word(u), Tok::Word(f), ..]
            if s.eq_ignore_ascii_case("SEARCH")
                && u.eq_ignore_ascii_case("USING")
                && f.eq_ignore_ascii_case("FUSION") =>
        {
            (*c).to_string()
        }
        _ => return None,
    };
    Some((
        collection,
        FusionParams::extract(&toks, sql, &SEARCH_FUSION_KEYWORDS),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_fusion_full_surface_parses() {
        let (col, p) = parse_search_using_fusion(
            "SEARCH mycol USING FUSION(ARRAY[0.1, 0.2] VECTOR_TOP_K 5 DEPTH 2 \
             LABEL 'related' TOP 10 RRF_K (60.0, 35.0))",
        )
        .unwrap();
        assert_eq!(col, "mycol");
        assert_eq!(p.query_vector.as_deref().map(<[f32]>::len), Some(2));
        assert_eq!(p.vector_top_k, Some(5));
        assert_eq!(p.expansion_depth, Some(2));
        assert_eq!(p.edge_label.as_deref(), Some("related"));
        assert_eq!(p.final_top_k, Some(10));
        assert_eq!(p.rrf_k, Some((60.0, 35.0)));
    }

    #[test]
    fn search_fusion_label_literal_that_shadows_top_keyword() {
        // A quoted label value containing the `TOP` keyword must not be
        // misread as the `TOP` numeric parameter — the tokenizer keeps
        // quoted strings whole, so `TOP 10` is the real parameter.
        let (_, p) =
            parse_search_using_fusion("SEARCH c USING FUSION(ARRAY[0.5] LABEL 'TOP_SECRET' TOP 7)")
                .unwrap();
        assert_eq!(p.edge_label.as_deref(), Some("TOP_SECRET"));
        assert_eq!(p.final_top_k, Some(7));
    }

    #[test]
    fn search_fusion_rejects_wrong_prefix() {
        assert!(parse_search_using_fusion("INSERT INTO x VALUES (1)").is_none());
        assert!(parse_search_using_fusion("SEARCH x USING VECTOR(ARRAY[1.0])").is_none());
    }
}
