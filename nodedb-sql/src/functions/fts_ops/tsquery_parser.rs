//! Recursive-descent parser for PostgreSQL `tsquery` boolean syntax.
//!
//! Supported grammar (left-recursive precedence, lowest-to-highest):
//! ```text
//! query   ::= or_expr
//! or_expr ::= and_expr ( '|' and_expr )*
//! and_expr::= not_expr ( '&' not_expr )*
//! not_expr::= '!' not_expr | atom
//! atom    ::= '(' query ')'
//!           | term ':*'    (prefix)
//!           | term         (plain)
//! ```
//!
//! A `term` is any contiguous non-operator, non-paren token.
//!
//! `phraseto_tsquery` is handled via `parse_phrase_terms` — the result is
//! always `FtsQuery::Phrase`, which the executor rejects with Unsupported.

use crate::error::{Result, SqlError};
use crate::fts_types::FtsQuery;

/// Parse a PG `tsquery` string into an `FtsQuery` tree.
///
/// Empty input is rejected with `SqlError::Parse`.
pub fn parse_tsquery(input: &str) -> Result<FtsQuery> {
    let input = input.trim();
    if input.is_empty() {
        return Err(SqlError::Parse {
            detail: "tsquery: empty query string".into(),
        });
    }
    let tokens = tokenize(input);
    let mut pos = 0;
    let query = parse_or(&tokens, &mut pos)?;
    if pos < tokens.len() {
        return Err(SqlError::Parse {
            detail: format!(
                "tsquery: unexpected token '{}' at position {pos}",
                tokens[pos]
            ),
        });
    }
    Ok(query)
}

/// Build an `FtsQuery::Phrase` from whitespace-separated terms.
///
/// Used by `phraseto_tsquery`.  The resulting variant is always rejected by
/// the executor with `Unsupported` — it is only produced so that the error
/// path is reached correctly.
pub fn parse_phrase_terms(input: &str) -> FtsQuery {
    let terms: Vec<String> = input
        .split_whitespace()
        .map(|s| s.to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    FtsQuery::Phrase(terms)
}

// ── Tokenizer ────────────────────────────────────────────────────────────────

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            ' ' | '\t' | '\n' | '\r' => continue,
            '(' | ')' | '&' | '|' | '!' => tokens.push(c.to_string()),
            ':' => {
                // ':*' suffix on the preceding term — already handled in term
                // parsing; if we see a bare ':' here it's part of the next
                // term text.  Push ':' as a token so we can detect ':*'.
                if chars.peek().map(|(_, nc)| *nc) == Some('*') {
                    chars.next(); // consume '*'
                    tokens.push(":*".into());
                } else {
                    tokens.push(":".into());
                }
            }
            _ => {
                // Collect a term: alphanumeric + hyphen + underscore.
                let start = i;
                let mut end = i + c.len_utf8();
                while let Some(&(j, nc)) = chars.peek() {
                    if nc.is_alphanumeric() || nc == '-' || nc == '_' {
                        end = j + nc.len_utf8();
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(input[start..end].to_ascii_lowercase());
            }
        }
    }
    tokens
}

// ── Recursive-descent parser ──────────────────────────────────────────────────

fn parse_or(tokens: &[String], pos: &mut usize) -> Result<FtsQuery> {
    let mut left = parse_and(tokens, pos)?;
    while *pos < tokens.len() && tokens[*pos] == "|" {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        left = match left {
            FtsQuery::Or(mut parts) => {
                parts.push(right);
                FtsQuery::Or(parts)
            }
            other => FtsQuery::Or(vec![other, right]),
        };
    }
    Ok(left)
}

fn parse_and(tokens: &[String], pos: &mut usize) -> Result<FtsQuery> {
    let mut left = parse_not(tokens, pos)?;
    while *pos < tokens.len() && tokens[*pos] == "&" {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        left = match left {
            FtsQuery::And(mut parts) => {
                parts.push(right);
                FtsQuery::And(parts)
            }
            other => FtsQuery::And(vec![other, right]),
        };
    }
    Ok(left)
}

fn parse_not(tokens: &[String], pos: &mut usize) -> Result<FtsQuery> {
    if *pos < tokens.len() && tokens[*pos] == "!" {
        *pos += 1;
        let inner = parse_not(tokens, pos)?;
        return Ok(FtsQuery::Not(Box::new(inner)));
    }
    parse_atom(tokens, pos)
}

fn parse_atom(tokens: &[String], pos: &mut usize) -> Result<FtsQuery> {
    if *pos >= tokens.len() {
        return Err(SqlError::Parse {
            detail: "tsquery: unexpected end of input".into(),
        });
    }
    if tokens[*pos] == "(" {
        *pos += 1;
        let inner = parse_or(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != ")" {
            return Err(SqlError::Parse {
                detail: "tsquery: unmatched '('".into(),
            });
        }
        *pos += 1;
        return Ok(inner);
    }
    // Must be a term token.
    let term = tokens[*pos].clone();
    if term == ")" || term == "&" || term == "|" || term == "!" {
        return Err(SqlError::Parse {
            detail: format!("tsquery: unexpected operator '{term}'"),
        });
    }
    *pos += 1;
    // Check for ':*' suffix (prefix search).
    if *pos < tokens.len() && tokens[*pos] == ":*" {
        *pos += 1;
        return Ok(FtsQuery::Prefix(term));
    }
    Ok(FtsQuery::Plain {
        text: term,
        fuzzy: false,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_term() {
        let q = parse_tsquery("rust").unwrap();
        assert_eq!(
            q,
            FtsQuery::Plain {
                text: "rust".into(),
                fuzzy: false
            }
        );
    }

    #[test]
    fn parse_and_two_terms() {
        let q = parse_tsquery("rust & lang").unwrap();
        assert_eq!(
            q,
            FtsQuery::And(vec![
                FtsQuery::Plain {
                    text: "rust".into(),
                    fuzzy: false
                },
                FtsQuery::Plain {
                    text: "lang".into(),
                    fuzzy: false
                },
            ])
        );
    }

    #[test]
    fn parse_or_two_terms() {
        let q = parse_tsquery("rust | lang").unwrap();
        assert_eq!(
            q,
            FtsQuery::Or(vec![
                FtsQuery::Plain {
                    text: "rust".into(),
                    fuzzy: false
                },
                FtsQuery::Plain {
                    text: "lang".into(),
                    fuzzy: false
                },
            ])
        );
    }

    #[test]
    fn parse_prefix() {
        let q = parse_tsquery("rust:*").unwrap();
        assert_eq!(q, FtsQuery::Prefix("rust".into()));
    }

    #[test]
    fn parse_nested() {
        // (a & b) | c
        let q = parse_tsquery("(a & b) | c").unwrap();
        assert_eq!(
            q,
            FtsQuery::Or(vec![
                FtsQuery::And(vec![
                    FtsQuery::Plain {
                        text: "a".into(),
                        fuzzy: false
                    },
                    FtsQuery::Plain {
                        text: "b".into(),
                        fuzzy: false
                    },
                ]),
                FtsQuery::Plain {
                    text: "c".into(),
                    fuzzy: false
                },
            ])
        );
    }

    #[test]
    fn parse_not_term() {
        let q = parse_tsquery("!foo").unwrap();
        assert_eq!(
            q,
            FtsQuery::Not(Box::new(FtsQuery::Plain {
                text: "foo".into(),
                fuzzy: false
            }))
        );
    }

    #[test]
    fn parse_empty_is_error() {
        assert!(parse_tsquery("").is_err());
        assert!(parse_tsquery("   ").is_err());
    }

    #[test]
    fn parse_phrase_terms_basic() {
        let q = parse_phrase_terms("hello world");
        assert_eq!(q, FtsQuery::Phrase(vec!["hello".into(), "world".into()]));
    }

    #[test]
    fn and_is_left_associative() {
        let q = parse_tsquery("a & b & c").unwrap();
        // a & b produces And([a,b]); then & c extends to And([a,b,c])
        assert_eq!(
            q,
            FtsQuery::And(vec![
                FtsQuery::Plain {
                    text: "a".into(),
                    fuzzy: false
                },
                FtsQuery::Plain {
                    text: "b".into(),
                    fuzzy: false
                },
                FtsQuery::Plain {
                    text: "c".into(),
                    fuzzy: false
                },
            ])
        );
    }

    #[test]
    fn or_is_left_associative() {
        let q = parse_tsquery("a | b | c").unwrap();
        assert_eq!(
            q,
            FtsQuery::Or(vec![
                FtsQuery::Plain {
                    text: "a".into(),
                    fuzzy: false
                },
                FtsQuery::Plain {
                    text: "b".into(),
                    fuzzy: false
                },
                FtsQuery::Plain {
                    text: "c".into(),
                    fuzzy: false
                },
            ])
        );
    }
}
