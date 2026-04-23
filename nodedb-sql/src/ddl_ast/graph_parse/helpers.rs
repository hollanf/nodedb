use super::{
    super::statement::{GraphDirection, GraphProperties},
    tokenizer::Tok,
};

pub(super) fn find_keyword(toks: &[Tok<'_>], keyword: &str) -> Option<usize> {
    toks.iter()
        .position(|t| matches!(t, Tok::Word(w) if w.eq_ignore_ascii_case(keyword)))
}

pub(super) fn quoted_after(toks: &[Tok<'_>], keyword: &str) -> Option<String> {
    let pos = find_keyword(toks, keyword)?;
    match toks.get(pos + 1)? {
        Tok::Quoted(s) => Some(s.clone().into_owned()),
        Tok::Word(w) => Some((*w).to_string()),
        Tok::Object(_) => None,
    }
}

pub(super) fn quoted_list_after(toks: &[Tok<'_>], keyword: &str) -> Vec<String> {
    let Some(pos) = find_keyword(toks, keyword) else {
        return Vec::new();
    };
    toks[pos + 1..]
        .iter()
        .map_while(|t| match t {
            Tok::Quoted(s) => Some(s.clone().into_owned()),
            _ => None,
        })
        .collect()
}

pub(super) fn word_after(toks: &[Tok<'_>], keyword: &str) -> Option<String> {
    let pos = find_keyword(toks, keyword)?;
    if let Tok::Word(w) = toks.get(pos + 1)? {
        Some((*w).to_string())
    } else {
        None
    }
}

pub(super) fn usize_after(toks: &[Tok<'_>], keyword: &str) -> Option<usize> {
    word_after(toks, keyword)?.parse().ok()
}

pub(super) fn float_after(toks: &[Tok<'_>], keyword: &str) -> Option<f64> {
    word_after(toks, keyword)?.parse().ok()
}

/// Extract the two consecutive float tokens that follow `keyword`.
///
/// The tokenizer strips `(` and `)`, so `RRF_K (60.0, 35.0)` becomes the
/// token sequence `[Word("RRF_K"), Word("60.0"), Word("35.0")]`. This helper
/// reads both values without requiring the caller to know about that stripping.
pub(super) fn float_pair_after(toks: &[Tok<'_>], keyword: &str) -> Option<(f64, f64)> {
    let pos = find_keyword(toks, keyword)?;
    let k1 = match toks.get(pos + 1)? {
        Tok::Word(w) => w.parse::<f64>().ok()?,
        _ => return None,
    };
    let k2 = match toks.get(pos + 2)? {
        Tok::Word(w) => w.parse::<f64>().ok()?,
        _ => return None,
    };
    Some((k1, k2))
}

pub(super) fn direction_after(toks: &[Tok<'_>]) -> GraphDirection {
    match word_after(toks, "DIRECTION")
        .as_deref()
        .map(str::to_ascii_uppercase)
        .as_deref()
    {
        Some("IN") => GraphDirection::In,
        Some("BOTH") => GraphDirection::Both,
        _ => GraphDirection::Out,
    }
}

pub(super) fn extract_properties(toks: &[Tok<'_>]) -> GraphProperties {
    let Some(pos) = find_keyword(toks, "PROPERTIES") else {
        return GraphProperties::None;
    };
    match toks.get(pos + 1) {
        Some(Tok::Object(obj_str)) => GraphProperties::Object((*obj_str).to_string()),
        Some(Tok::Quoted(s)) => GraphProperties::Quoted(s.clone().into_owned()),
        _ => GraphProperties::None,
    }
}

/// Extract `ARRAY[f1, f2, …]` that appears after `keyword` in raw SQL.
///
/// Used for `QUERY ARRAY[…]` in `GRAPH RAG FUSION` where the vector payload
/// cannot be tokenized as keyword-value pairs. Searches for the first `[`
/// after the keyword and collects comma-separated f32 values up to `]`.
pub(super) fn array_floats_after(sql: &str, keyword: &str) -> Option<Vec<f32>> {
    let upper = sql.to_ascii_uppercase();
    let kw_pos = upper.find(keyword)?;
    let after_kw = &sql[kw_pos + keyword.len()..];
    let bracket_start = after_kw.find('[').map(|i| i + 1)?;
    let bracket_end = after_kw[bracket_start..]
        .find(']')
        .map(|i| i + bracket_start)?;
    let content = &after_kw[bracket_start..bracket_end];
    let floats: Vec<f32> = content
        .split(',')
        .filter_map(|s| s.trim().parse::<f32>().ok())
        .collect();
    if floats.is_empty() {
        None
    } else {
        Some(floats)
    }
}
