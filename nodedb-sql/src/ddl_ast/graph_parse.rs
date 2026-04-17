//! Typed parsing for the graph DSL (`GRAPH ...`, `MATCH ...`).
//!
//! The handler layer historically parsed graph statements with
//! `upper.find("KEYWORD")` substring matching, which collapsed when
//! a node id, label, or property value shadowed a DSL keyword. This
//! module is the structural replacement: a quote- and brace-aware
//! tokeniser feeds a variant-building parser that produces a typed
//! [`NodedbStatement`]. Every graph DSL command flows through here
//! before reaching a pgwire handler, so the handlers never touch
//! raw SQL again.
//!
//! Values collected here are intentionally unvalidated — numeric
//! bounds, absent-but-required fields, and engine-level caps are
//! enforced at the pgwire boundary where the error response is
//! formed. That keeps this module free of `pgwire` dependencies
//! and out of the `nodedb` → `nodedb-sql` dependency edge.

use std::borrow::Cow;

use super::statement::{GraphDirection, GraphProperties, NodedbStatement};

/// Entry point: returns `Some` when `sql` starts with a graph DSL
/// keyword (`GRAPH ...`, `MATCH ...`, `OPTIONAL MATCH ...`), else
/// `None` so the main DDL parser can continue trying other cases.
///
/// Within those prefixes a malformed statement still returns
/// `Some(stmt)` whenever a sensible variant can be produced; the
/// pgwire handler decides whether required fields are present.
/// Statements that cannot be shaped into any variant return `None`
/// so the legacy string-prefix router can still handle them during
/// the migration window.
pub fn try_parse(sql: &str) -> Option<NodedbStatement> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("MATCH ") || upper.starts_with("OPTIONAL MATCH ") {
        return Some(NodedbStatement::MatchQuery {
            raw_sql: trimmed.to_string(),
        });
    }

    if !upper.starts_with("GRAPH ") {
        return None;
    }

    let toks = tokenize(trimmed);

    if upper.starts_with("GRAPH INSERT EDGE ") {
        return parse_insert_edge(&toks);
    }
    if upper.starts_with("GRAPH DELETE EDGE ") {
        return parse_delete_edge(&toks);
    }
    if upper.starts_with("GRAPH LABEL ") {
        return parse_set_labels(&toks, false);
    }
    if upper.starts_with("GRAPH UNLABEL ") {
        return parse_set_labels(&toks, true);
    }
    if upper.starts_with("GRAPH TRAVERSE ") {
        return parse_traverse(&toks);
    }
    if upper.starts_with("GRAPH NEIGHBORS ") {
        return parse_neighbors(&toks);
    }
    if upper.starts_with("GRAPH PATH ") {
        return parse_path(&toks);
    }
    if upper.starts_with("GRAPH ALGO ") {
        return parse_algo(&toks);
    }

    None
}

// ── Variant builders ─────────────────────────────────────────────

fn parse_insert_edge(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let label = quoted_after(toks, "TYPE")?;
    let properties = extract_properties(toks);
    Some(NodedbStatement::GraphInsertEdge {
        src,
        dst,
        label,
        properties,
    })
}

fn parse_delete_edge(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let label = quoted_after(toks, "TYPE")?;
    Some(NodedbStatement::GraphDeleteEdge { src, dst, label })
}

fn parse_set_labels(toks: &[Tok<'_>], remove: bool) -> Option<NodedbStatement> {
    let keyword = if remove { "UNLABEL" } else { "LABEL" };
    let node_id = quoted_after(toks, keyword)?;
    let labels = quoted_list_after(toks, "AS");
    Some(NodedbStatement::GraphSetLabels {
        node_id,
        labels,
        remove,
    })
}

fn parse_traverse(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
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

fn parse_neighbors(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let node = quoted_after(toks, "OF")?;
    let edge_label = quoted_after(toks, "LABEL");
    let direction = direction_after(toks);
    Some(NodedbStatement::GraphNeighbors {
        node,
        edge_label,
        direction,
    })
}

fn parse_path(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
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

fn parse_algo(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    // Algorithm name is the first `Word` token after `ALGO`.
    let algorithm = find_keyword(toks, "ALGO").and_then(|i| match toks.get(i + 1)? {
        Tok::Word(w) => Some(w.to_ascii_uppercase()),
        _ => None,
    })?;
    let collection = word_after(toks, "ON")?.to_lowercase();
    Some(NodedbStatement::GraphAlgo {
        algorithm,
        collection,
        damping: float_after(toks, "DAMPING"),
        tolerance: float_after(toks, "TOLERANCE"),
        resolution: float_after(toks, "RESOLUTION"),
        max_iterations: usize_after(toks, "ITERATIONS"),
        sample_size: usize_after(toks, "SAMPLE"),
        source_node: quoted_after(toks, "FROM"),
        direction: word_after(toks, "DIRECTION"),
        mode: word_after(toks, "MODE"),
    })
}

fn extract_properties(toks: &[Tok<'_>]) -> GraphProperties {
    let Some(pos) = find_keyword(toks, "PROPERTIES") else {
        return GraphProperties::None;
    };
    match toks.get(pos + 1) {
        Some(Tok::Object(obj_str)) => GraphProperties::Object((*obj_str).to_string()),
        Some(Tok::Quoted(s)) => GraphProperties::Quoted(s.clone().into_owned()),
        _ => GraphProperties::None,
    }
}

fn direction_after(toks: &[Tok<'_>]) -> GraphDirection {
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

// ── Tokeniser ────────────────────────────────────────────────────

enum Tok<'a> {
    Word(&'a str),
    Quoted(Cow<'a, str>),
    /// Brace-balanced object literal, including the outer braces.
    Object(&'a str),
}

fn tokenize(sql: &str) -> Vec<Tok<'_>> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() || b == b',' || b == b';' || b == b'(' || b == b')' {
            i += 1;
            continue;
        }
        if b == b'\'' {
            i = consume_quoted(sql, bytes, i, &mut out);
            continue;
        }
        if b == b'{' {
            i = consume_object(sql, bytes, i, &mut out);
            continue;
        }
        i = consume_word(sql, bytes, i, &mut out);
    }
    out
}

fn consume_quoted<'a>(sql: &'a str, bytes: &[u8], start: usize, out: &mut Vec<Tok<'a>>) -> usize {
    let content_start = start + 1;
    let mut j = content_start;
    let mut has_escape = false;
    while j < bytes.len() {
        if bytes[j] == b'\'' {
            if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                has_escape = true;
                j += 2;
                continue;
            }
            break;
        }
        j += 1;
    }
    let slice = &sql[content_start..j];
    let content = if has_escape {
        Cow::Owned(slice.replace("''", "'"))
    } else {
        Cow::Borrowed(slice)
    };
    out.push(Tok::Quoted(content));
    if j < bytes.len() { j + 1 } else { j }
}

fn consume_object<'a>(sql: &'a str, bytes: &[u8], start: usize, out: &mut Vec<Tok<'a>>) -> usize {
    let mut depth = 0i32;
    let mut j = start;
    let mut in_quote = false;
    while j < bytes.len() {
        let c = bytes[j];
        if in_quote {
            if c == b'\'' {
                if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                    j += 2;
                    continue;
                }
                in_quote = false;
            }
        } else {
            match c {
                b'\'' => in_quote = true,
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        j += 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        j += 1;
    }
    out.push(Tok::Object(&sql[start..j]));
    j
}

fn consume_word<'a>(sql: &'a str, bytes: &[u8], start: usize, out: &mut Vec<Tok<'a>>) -> usize {
    let mut j = start;
    while j < bytes.len() {
        let c = bytes[j];
        if c.is_ascii_whitespace()
            || c == b'\''
            || c == b'{'
            || c == b','
            || c == b';'
            || c == b'('
            || c == b')'
        {
            break;
        }
        j += 1;
    }
    if j > start {
        out.push(Tok::Word(&sql[start..j]));
        j
    } else {
        start + 1
    }
}

// ── Token-level extraction helpers ───────────────────────────────

fn find_keyword(toks: &[Tok<'_>], keyword: &str) -> Option<usize> {
    toks.iter()
        .position(|t| matches!(t, Tok::Word(w) if w.eq_ignore_ascii_case(keyword)))
}

fn quoted_after(toks: &[Tok<'_>], keyword: &str) -> Option<String> {
    let pos = find_keyword(toks, keyword)?;
    match toks.get(pos + 1)? {
        Tok::Quoted(s) => Some(s.clone().into_owned()),
        Tok::Word(w) => Some((*w).to_string()),
        Tok::Object(_) => None,
    }
}

fn quoted_list_after(toks: &[Tok<'_>], keyword: &str) -> Vec<String> {
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

fn word_after(toks: &[Tok<'_>], keyword: &str) -> Option<String> {
    let pos = find_keyword(toks, keyword)?;
    if let Tok::Word(w) = toks.get(pos + 1)? {
        Some((*w).to_string())
    } else {
        None
    }
}

fn usize_after(toks: &[Tok<'_>], keyword: &str) -> Option<usize> {
    word_after(toks, keyword)?.parse().ok()
}

fn float_after(toks: &[Tok<'_>], keyword: &str) -> Option<f64> {
    word_after(toks, keyword)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_graph_insert_edge_keyword_shaped_ids() {
        let stmt = try_parse("GRAPH INSERT EDGE FROM 'TO' TO 'FROM' TYPE 'LABEL'").unwrap();
        match stmt {
            NodedbStatement::GraphInsertEdge {
                src,
                dst,
                label,
                properties,
            } => {
                assert_eq!(src, "TO");
                assert_eq!(dst, "FROM");
                assert_eq!(label, "LABEL");
                assert_eq!(properties, GraphProperties::None);
            }
            other => panic!("expected GraphInsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_insert_edge_with_object_properties() {
        let stmt = try_parse(
            "GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l' PROPERTIES { note: '} DEPTH 999' }",
        )
        .unwrap();
        match stmt {
            NodedbStatement::GraphInsertEdge { properties, .. } => match properties {
                GraphProperties::Object(s) => assert!(s.contains("} DEPTH 999")),
                other => panic!("expected Object properties, got {other:?}"),
            },
            other => panic!("expected GraphInsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_traverse_keyword_substring_id() {
        let stmt =
            try_parse("GRAPH TRAVERSE FROM 'node_with_DEPTH_in_name' DEPTH 2 LABEL 'l'").unwrap();
        match stmt {
            NodedbStatement::GraphTraverse { start, depth, .. } => {
                assert_eq!(start, "node_with_DEPTH_in_name");
                assert_eq!(depth, 2);
            }
            other => panic!("expected GraphTraverse, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_path() {
        let stmt = try_parse("GRAPH PATH FROM 'a' TO 'b' MAX_DEPTH 5 LABEL 'l'").unwrap();
        match stmt {
            NodedbStatement::GraphPath {
                src,
                dst,
                max_depth,
                edge_label,
            } => {
                assert_eq!(src, "a");
                assert_eq!(dst, "b");
                assert_eq!(max_depth, 5);
                assert_eq!(edge_label.as_deref(), Some("l"));
            }
            other => panic!("expected GraphPath, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_labels_list() {
        let stmt = try_parse("GRAPH LABEL 'alice' AS 'Person', 'User'").unwrap();
        match stmt {
            NodedbStatement::GraphSetLabels {
                node_id,
                labels,
                remove,
            } => {
                assert_eq!(node_id, "alice");
                assert_eq!(labels, vec!["Person".to_string(), "User".to_string()]);
                assert!(!remove);
            }
            other => panic!("expected GraphSetLabels, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_algo_pagerank() {
        let stmt = try_parse("GRAPH ALGO PAGERANK ON users ITERATIONS 5 DAMPING 0.85").unwrap();
        match stmt {
            NodedbStatement::GraphAlgo {
                algorithm,
                collection,
                damping,
                max_iterations,
                ..
            } => {
                assert_eq!(algorithm, "PAGERANK");
                assert_eq!(collection, "users");
                assert_eq!(damping, Some(0.85));
                assert_eq!(max_iterations, Some(5));
            }
            other => panic!("expected GraphAlgo, got {other:?}"),
        }
    }

    #[test]
    fn parse_match_query_captures_raw() {
        let stmt = try_parse("MATCH (x)-[:l]->(y) RETURN x, y").unwrap();
        match stmt {
            NodedbStatement::MatchQuery { raw_sql } => {
                assert!(raw_sql.starts_with("MATCH"));
            }
            other => panic!("expected MatchQuery, got {other:?}"),
        }
    }

    #[test]
    fn non_graph_returns_none() {
        assert!(try_parse("SELECT * FROM users").is_none());
        assert!(try_parse("CREATE COLLECTION users").is_none());
    }
}
