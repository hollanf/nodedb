//! Executor implementations for PG FTS surface functions.
//!
//! These functions are evaluated at row-scan time via `eval_function`.
//! They operate on `nodedb_types::Value` arguments and return `Value`.
//!
//! `pg_fts_match` performs a simple contains-all-terms check suitable for
//! scalar filter evaluation.  The high-performance BM25 index path is
//! triggered by the planner (`SearchTrigger::TextMatch`) and never reaches
//! this executor for indexed collections.
//!
//! `pg_phraseto_tsquery` and any call producing `FtsQuery::Phrase/Not` are
//! evaluated here but immediately return `Value::Null` — those variants are
//! detected and rejected with `Unsupported` earlier in the planning stack
//! whenever the query reaches the planner.  At the executor level they are
//! surfaced as `Null` so outer expressions degrade gracefully rather than
//! panicking.

use nodedb_types::Value;

/// Dispatch entry for PG FTS functions.  Returns `None` for unrecognised names.
pub fn try_eval_fts(name: &str, args: &[Value]) -> Option<Value> {
    match name {
        "pg_fts_match" => Some(eval_pg_fts_match(args)),
        "pg_to_tsquery" | "pg_plainto_tsquery" | "pg_websearch_to_tsquery" => {
            Some(eval_tsquery_passthrough(args))
        }
        "pg_phraseto_tsquery" => {
            // Phrase queries are unsupported.  Return Null so that filter
            // expressions on unsupported query types produce no matches
            // rather than a runtime error at the row-scan layer.
            Some(Value::Null)
        }
        "pg_to_tsvector" => {
            // tsvector cast — just return the text value unchanged.
            Some(
                args.last()
                    .and_then(|v| {
                        if let Value::String(s) = v {
                            Some(Value::String(s.clone()))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(Value::Null),
            )
        }
        "pg_ts_rank" => {
            // ts_rank maps to BM25 score; at the scalar level we return 0.0
            // (actual scoring happens in the BM25 search path, not here).
            Some(Value::Float(0.0))
        }
        "pg_ts_headline" => Some(eval_pg_ts_headline(args)),
        _ => None,
    }
}

/// `pg_fts_match(document_text, tsquery_text)` → Bool.
///
/// Performs an AND-first containment check: splits the tsquery into terms
/// (stripping `&`, `|`, `!`, `:*`) and returns `true` when all terms are
/// present (case-insensitive substring match) in the document text.
///
/// This is the scalar fallback used when there is no BM25 index; the indexed
/// path is handled by `SearchTrigger::TextMatch` in the planner.
fn eval_pg_fts_match(args: &[Value]) -> Value {
    let doc = match args.first() {
        Some(Value::String(s)) => s.to_ascii_lowercase(),
        _ => return Value::Null,
    };
    let query = match args.get(1) {
        Some(Value::String(s)) => s.clone(),
        _ => return Value::Null,
    };
    let terms = extract_terms(&query);
    if terms.is_empty() {
        return Value::Bool(false);
    }
    Value::Bool(terms.iter().all(|t| doc.contains(t.as_str())))
}

/// Extract searchable terms from a tsquery string by stripping operators.
fn extract_terms(query: &str) -> Vec<String> {
    query
        .split(['&', '|', '!', '(', ')'])
        .flat_map(|s| s.split_whitespace())
        .map(|s| s.trim_end_matches(":*").to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// `pg_to_tsquery` / `pg_plainto_tsquery` / `pg_websearch_to_tsquery` at scalar level.
///
/// These functions produce a tsquery text value.  The last argument is the
/// query string (optional first argument is the language, which is ignored at
/// the scalar layer — language configuration is handled at the index level).
fn eval_tsquery_passthrough(args: &[Value]) -> Value {
    // If 2 args, first is language (ignored), second is query text.
    // If 1 arg, it is the query text.
    let query_val = if args.len() >= 2 {
        args.get(1)
    } else {
        args.first()
    };
    match query_val {
        Some(Value::String(s)) => Value::String(s.clone()),
        _ => Value::Null,
    }
}

/// `pg_ts_headline(document, query[, options])` → String.
///
/// Returns a snippet of the document with matched query terms marked.
/// Uses simple term highlighting: matches are wrapped in `<b>…</b>`.
fn eval_pg_ts_headline(args: &[Value]) -> Value {
    let doc = match args.first() {
        Some(Value::String(s)) => s.clone(),
        _ => return Value::Null,
    };
    let query = match args.get(1) {
        Some(Value::String(s)) => s.clone(),
        _ => return Value::Null,
    };
    let terms = extract_terms(&query);
    if terms.is_empty() {
        return Value::String(doc);
    }

    // Build a simple highlight: find first sentence/window that contains a
    // matched term and wrap occurrences in <b>…</b>.
    let lower_doc = doc.to_ascii_lowercase();
    let mut result = doc.clone();
    // Apply replacements in reverse order of byte offset to preserve positions.
    let mut replacements: Vec<(usize, usize)> = Vec::new();
    for term in &terms {
        let mut search_from = 0;
        while let Some(pos) = lower_doc[search_from..].find(term.as_str()) {
            let abs_pos = search_from + pos;
            replacements.push((abs_pos, abs_pos + term.len()));
            search_from = abs_pos + term.len();
        }
    }
    // Deduplicate overlapping ranges and sort descending so insertions don't
    // shift earlier positions.
    replacements.sort_by_key(|r| std::cmp::Reverse(r.0));
    replacements.dedup_by(|a, b| a.0 >= b.0 && a.0 < b.1);
    for (start, end) in &replacements {
        if *end <= result.len() {
            let matched = result[*start..*end].to_string();
            result.replace_range(*start..*end, &format!("<b>{matched}</b>"));
        }
    }
    Value::String(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::String(v.to_string())
    }

    #[test]
    fn fts_match_all_present() {
        let result = eval_pg_fts_match(&[
            s("The quick brown fox jumps over the lazy dog"),
            s("quick & fox"),
        ]);
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn fts_match_missing_term() {
        let result = eval_pg_fts_match(&[s("The quick brown fox"), s("quick & cat")]);
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn fts_match_prefix_stripped() {
        let result = eval_pg_fts_match(&[s("rustlang is great"), s("rust:*")]);
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn fts_match_null_doc() {
        let result = eval_pg_fts_match(&[Value::Null, s("query")]);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn tsquery_passthrough_one_arg() {
        let result = eval_tsquery_passthrough(&[s("rust & lang")]);
        assert_eq!(result, s("rust & lang"));
    }

    #[test]
    fn tsquery_passthrough_two_args_uses_second() {
        let result = eval_tsquery_passthrough(&[s("english"), s("rust")]);
        assert_eq!(result, s("rust"));
    }

    #[test]
    fn ts_headline_wraps_terms() {
        let result = eval_pg_ts_headline(&[s("The quick brown fox jumps"), s("quick & fox")]);
        match result {
            Value::String(s) => {
                assert!(s.contains("<b>quick</b>") || s.contains("<b>fox</b>"));
            }
            other => panic!("expected String, got {other:?}"),
        }
    }
}
