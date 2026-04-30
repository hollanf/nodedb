//! FTS query type used by `SqlPlan::TextSearch`.
//!
//! `FtsQuery` is the structured representation of a full-text search query
//! after lowering from PG surface syntax (`to_tsquery`, `plainto_tsquery`,
//! `websearch_to_tsquery`, `@@`).  The executor in `nodedb-query` maps each
//! variant to the corresponding `nodedb-fts` query mode.

/// A structured full-text search query, lowered from PG tsquery syntax.
///
/// Variants that require engine support not yet available in `nodedb-fts`
/// (`Phrase`, `Not`) are represented here but rejected by the executor with
/// `SqlError::Unsupported` — they are never silently approximated.
#[derive(Debug, Clone, PartialEq)]
pub enum FtsQuery {
    /// A single search term, optionally with fuzzy matching.
    Plain { text: String, fuzzy: bool },
    /// All sub-queries must match (tsquery `&` / `plainto_tsquery` space-AND).
    And(Vec<FtsQuery>),
    /// Any sub-query may match (tsquery `|`).
    Or(Vec<FtsQuery>),
    /// Excludes documents matching the inner query (tsquery `!`).
    ///
    /// Not yet supported by `nodedb-fts`; the executor returns
    /// `Unsupported` when this variant is encountered.
    Not(Box<FtsQuery>),
    /// Strict consecutive-position phrase match (`phraseto_tsquery`).
    ///
    /// Not yet supported by `nodedb-fts`; the executor returns
    /// `Unsupported` when this variant is encountered.
    Phrase(Vec<String>),
    /// Prefix search — matches all terms that begin with the given string
    /// (tsquery `term:*`).
    Prefix(String),
}

impl FtsQuery {
    /// Extract the raw query text when the variant is `Plain`.
    /// Used by the executor to pass the text to `nodedb-fts::FtsIndex::search`.
    pub fn as_plain_text(&self) -> Option<&str> {
        match self {
            FtsQuery::Plain { text, .. } => Some(text.as_str()),
            _ => None,
        }
    }

    /// Return `true` if fuzzy matching is requested.
    /// Only meaningful for `Plain`; all other variants return `false`.
    pub fn is_fuzzy(&self) -> bool {
        match self {
            FtsQuery::Plain { fuzzy, .. } => *fuzzy,
            _ => false,
        }
    }

    /// Flatten an `And` or `Or` of `Plain` terms to a space-separated string
    /// suitable for `FtsIndex::search`.  Returns `None` for structured queries
    /// that cannot be expressed as a plain string.
    pub fn to_plain_string(&self) -> Option<String> {
        match self {
            FtsQuery::Plain { text, .. } => Some(text.clone()),
            FtsQuery::And(terms) | FtsQuery::Or(terms) => {
                let parts: Option<Vec<String>> =
                    terms.iter().map(|t| t.to_plain_string()).collect();
                parts.map(|p| p.join(" "))
            }
            FtsQuery::Prefix(p) => Some(p.clone()),
            FtsQuery::Not(_) | FtsQuery::Phrase(_) => None,
        }
    }
}
