//! NodeDB reserved identifier list.
//!
//! These keywords are intercepted by the DDL dispatcher or DSL rewriter
//! before the SQL reaches sqlparser. Using them as bare (unquoted)
//! identifiers would cause silent misdispatch, so they are rejected at
//! parse time with an actionable error message that includes the quoted
//! escape form.

use crate::error::SqlError;

/// Words that NodeDB claims as dispatch or rewrite keywords.
///
/// Each entry is stored in UPPER case. All comparisons normalise the
/// input to upper case before checking containment.
pub const RESERVED_KEYWORDS: &[&str] = &[
    "GRAPH",    // graph dispatch keyword
    "MATCH",    // graph dispatch keyword
    "OPTIONAL", // graph dispatch keyword
    "UPSERT",   // preprocess rewrite keyword
    "UNDROP",   // DDL dispatch keyword
    "PURGE",    // DROP modifier keyword
    "CASCADE",  // DROP modifier keyword
    "SEARCH",   // DSL dispatch keyword
    "CRDT",     // DSL dispatch keyword
];

fn reason_for(upper: &str) -> &'static str {
    match upper {
        "GRAPH" | "MATCH" | "OPTIONAL" => "graph dispatch keyword",
        "UPSERT" => "preprocess rewrite keyword",
        "UNDROP" => "DDL dispatch keyword",
        "PURGE" | "CASCADE" => "DROP modifier keyword",
        "SEARCH" | "CRDT" => "DSL dispatch keyword",
        _ => "reserved by NodeDB",
    }
}

/// Return `true` if `name` matches a NodeDB reserved keyword
/// (case-insensitive).
pub fn is_reserved(name: &str) -> bool {
    let upper = name.to_uppercase();
    RESERVED_KEYWORDS.contains(&upper.as_str())
}

/// Validate a raw identifier token extracted from SQL.
///
/// * If `raw_name` is surrounded by double-quotes (standard SQL quoting),
///   the quotes are stripped and the inner text is returned unchanged as
///   `Ok(inner)` — the user has opted in to the reserved word.
/// * Otherwise, the token is normalised to upper case and compared against
///   [`RESERVED_KEYWORDS`]. A match returns
///   `Err(SqlError::ReservedIdentifier { .. })` with an actionable hint.
/// * A clean identifier is returned as `Ok(name.to_lowercase())` to match
///   the existing `parse_col_token` behaviour.
pub fn check_identifier(raw_name: &str) -> Result<String, SqlError> {
    if raw_name.starts_with('"') && raw_name.ends_with('"') && raw_name.len() >= 2 {
        // Standard SQL quoted identifier: strip the surrounding quotes.
        return Ok(raw_name[1..raw_name.len() - 1].to_string());
    }

    let upper = raw_name.to_uppercase();
    if RESERVED_KEYWORDS.contains(&upper.as_str()) {
        let reason = reason_for(&upper);
        return Err(SqlError::ReservedIdentifier {
            name: raw_name.to_string(),
            reason,
        });
    }

    Ok(raw_name.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SqlError;

    // ── is_reserved ──────────────────────────────────────────────────────────

    #[test]
    fn reserved_upper() {
        assert!(is_reserved("MATCH"));
    }

    #[test]
    fn reserved_lower() {
        assert!(is_reserved("match"));
    }

    #[test]
    fn not_reserved() {
        assert!(!is_reserved("id"));
    }

    // ── check_identifier ─────────────────────────────────────────────────────

    #[test]
    fn bare_reserved_is_err() {
        let err = check_identifier("match").unwrap_err();
        assert!(matches!(err, SqlError::ReservedIdentifier { .. }));
    }

    #[test]
    fn quoted_lower_is_ok() {
        assert_eq!(check_identifier("\"match\"").unwrap(), "match");
    }

    #[test]
    fn quoted_upper_is_ok() {
        assert_eq!(check_identifier("\"MATCH\"").unwrap(), "MATCH");
    }

    #[test]
    fn clean_identifier_is_ok() {
        assert_eq!(check_identifier("id").unwrap(), "id");
    }

    // ── one test per reserved word: bare rejected, quoted accepted ────────────

    #[test]
    fn graph_reserved() {
        assert!(check_identifier("graph").is_err());
        assert_eq!(check_identifier("\"graph\"").unwrap(), "graph");
    }

    #[test]
    fn match_reserved() {
        assert!(check_identifier("match").is_err());
        assert_eq!(check_identifier("\"match\"").unwrap(), "match");
    }

    #[test]
    fn optional_reserved() {
        assert!(check_identifier("optional").is_err());
        assert_eq!(check_identifier("\"optional\"").unwrap(), "optional");
    }

    #[test]
    fn upsert_reserved() {
        assert!(check_identifier("upsert").is_err());
        assert_eq!(check_identifier("\"upsert\"").unwrap(), "upsert");
    }

    #[test]
    fn undrop_reserved() {
        assert!(check_identifier("undrop").is_err());
        assert_eq!(check_identifier("\"undrop\"").unwrap(), "undrop");
    }

    #[test]
    fn purge_reserved() {
        assert!(check_identifier("purge").is_err());
        assert_eq!(check_identifier("\"purge\"").unwrap(), "purge");
    }

    #[test]
    fn cascade_reserved() {
        assert!(check_identifier("cascade").is_err());
        assert_eq!(check_identifier("\"cascade\"").unwrap(), "cascade");
    }

    #[test]
    fn search_reserved() {
        assert!(check_identifier("search").is_err());
        assert_eq!(check_identifier("\"search\"").unwrap(), "search");
    }

    #[test]
    fn crdt_reserved() {
        assert!(check_identifier("crdt").is_err());
        assert_eq!(check_identifier("\"crdt\"").unwrap(), "crdt");
    }
}
