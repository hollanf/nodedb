use super::{
    super::statement::NodedbStatement,
    fusion_params::{FusionParams, RAG_FUSION_KEYWORDS},
    helpers::{
        direction_after, extract_properties, quoted_after, quoted_list_after, usize_after,
        word_after,
    },
    tokenizer::Tok,
};

pub(super) fn parse_insert_edge(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let collection = quoted_after(toks, "IN")?;
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let label = quoted_after(toks, "TYPE")?;
    let properties = extract_properties(toks);
    Some(NodedbStatement::GraphInsertEdge {
        collection,
        src,
        dst,
        label,
        properties,
    })
}

pub(super) fn parse_delete_edge(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let collection = quoted_after(toks, "IN")?;
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let label = quoted_after(toks, "TYPE")?;
    Some(NodedbStatement::GraphDeleteEdge {
        collection,
        src,
        dst,
        label,
    })
}

pub(super) fn parse_set_labels(toks: &[Tok<'_>], remove: bool) -> Option<NodedbStatement> {
    let keyword = if remove { "UNLABEL" } else { "LABEL" };
    let node_id = quoted_after(toks, keyword)?;
    let labels = quoted_list_after(toks, "AS");
    Some(NodedbStatement::GraphSetLabels {
        node_id,
        labels,
        remove,
    })
}

pub(super) fn parse_traverse(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let start = quoted_after(toks, "FROM")?;
    let depth = usize_after(toks, "DEPTH").unwrap_or(2);
    let edge_label = quoted_after(toks, "LABEL");
    let direction = direction_after(toks);
    Some(NodedbStatement::GraphTraverse {
        start,
        depth,
        edge_label,
        direction,
    })
}

pub(super) fn parse_neighbors(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let node = quoted_after(toks, "OF")?;
    let edge_label = quoted_after(toks, "LABEL");
    let direction = direction_after(toks);
    Some(NodedbStatement::GraphNeighbors {
        node,
        edge_label,
        direction,
    })
}

pub(super) fn parse_path(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let max_depth = usize_after(toks, "MAX_DEPTH").unwrap_or(10);
    let edge_label = quoted_after(toks, "LABEL");
    Some(NodedbStatement::GraphPath {
        src,
        dst,
        max_depth,
        edge_label,
    })
}

pub(super) fn parse_algo(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let algorithm =
        super::helpers::find_keyword(toks, "ALGO").and_then(|i| match toks.get(i + 1)? {
            Tok::Word(w) => Some(w.to_ascii_uppercase()),
            _ => None,
        })?;
    let collection = word_after(toks, "ON")?.to_lowercase();
    Some(NodedbStatement::GraphAlgo {
        algorithm,
        collection,
        damping: super::helpers::float_after(toks, "DAMPING"),
        tolerance: super::helpers::float_after(toks, "TOLERANCE"),
        resolution: super::helpers::float_after(toks, "RESOLUTION"),
        max_iterations: usize_after(toks, "ITERATIONS"),
        sample_size: usize_after(toks, "SAMPLE"),
        source_node: quoted_after(toks, "FROM"),
        direction: word_after(toks, "DIRECTION"),
        mode: word_after(toks, "MODE"),
    })
}

/// Parse `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`.
///
/// All fusion parameters are delegated to [`FusionParams::extract`] so
/// every fusion SQL surface shares one typed, quote-aware extractor.
pub(super) fn parse_rag_fusion(toks: &[Tok<'_>], sql: &str) -> Option<NodedbStatement> {
    let collection = word_after(toks, "ON").or_else(|| quoted_after(toks, "ON"))?;
    let params = FusionParams::extract(toks, sql, &RAG_FUSION_KEYWORDS);
    Some(NodedbStatement::GraphRagFusion { collection, params })
}
