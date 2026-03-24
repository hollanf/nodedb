//! SQL syntax highlighting for the input area.
//!
//! Simple byte-scanning tokenizer — no regex, no parser dependencies.
//! Colors: keywords=magenta, strings=green, numbers=cyan, comments=gray,
//! operators=yellow, plain=default.

use ratatui::style::{Color, Style};
use ratatui::text::Span;

/// SQL keywords to highlight (uppercase for matching).
const KEYWORDS: &[&str] = &[
    "SELECT",
    "INSERT",
    "INTO",
    "UPDATE",
    "DELETE",
    "FROM",
    "WHERE",
    "CREATE",
    "DROP",
    "ALTER",
    "COLLECTION",
    "INDEX",
    "VECTOR",
    "GRAPH",
    "SEARCH",
    "USING",
    "FUSION",
    "VALUES",
    "SET",
    "AND",
    "OR",
    "NOT",
    "IN",
    "EXISTS",
    "BETWEEN",
    "LIKE",
    "ORDER",
    "BY",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "GROUP",
    "HAVING",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "ON",
    "AS",
    "DISTINCT",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "SHOW",
    "COLLECTIONS",
    "INDEXES",
    "USERS",
    "NODES",
    "CLUSTER",
    "CRDT",
    "MERGE",
    "TRAVERSE",
    "NEIGHBORS",
    "PATH",
    "EDGE",
    "ARRAY",
    "NULL",
    "TRUE",
    "FALSE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "EXPLAIN",
    "COPY",
    "BACKUP",
    "RESTORE",
    "GRANT",
    "REVOKE",
    "ROLE",
    "TENANT",
    "TYPE",
    "DEPTH",
    "DIRECTION",
    "METRIC",
    "FULLTEXT",
    "FIELD",
    "FIELDS",
    "WITH",
    "FILTER",
    "TOP",
    "SAVEPOINT",
    "RELEASE",
    "ABORT",
    "END",
    "TRANSACTION",
    "TABLE",
];

fn is_keyword(word: &str) -> bool {
    let upper = word.to_uppercase();
    KEYWORDS.contains(&upper.as_str())
}

/// Highlight a single line of SQL, returning styled spans.
pub fn highlight_line(line: &str) -> Vec<Span<'_>> {
    let mut spans = Vec::new();
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Line comment: -- to end
        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            spans.push(Span::styled(
                &line[i..],
                Style::default().fg(Color::DarkGray),
            ));
            return spans;
        }

        // Single-quoted string
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < len && bytes[i] == b'\'' {
                        i += 1; // escaped ''
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            spans.push(Span::styled(
                &line[start..i],
                Style::default().fg(Color::Green),
            ));
            continue;
        }

        // Number (digits, optionally with dot)
        if bytes[i].is_ascii_digit()
            || (bytes[i] == b'-'
                && i + 1 < len
                && bytes[i + 1].is_ascii_digit()
                && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric()))
        {
            let start = i;
            if bytes[i] == b'-' {
                i += 1;
            }
            while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                i += 1;
            }
            spans.push(Span::styled(
                &line[start..i],
                Style::default().fg(Color::Cyan),
            ));
            continue;
        }

        // Word (identifier or keyword)
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' || bytes[i] == b'\\' {
            let start = i;
            while i < len
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'\\')
            {
                i += 1;
            }
            let word = &line[start..i];
            if is_keyword(word) {
                spans.push(Span::styled(word, Style::default().fg(Color::Magenta)));
            } else if word.starts_with('\\') {
                spans.push(Span::styled(word, Style::default().fg(Color::Yellow)));
            } else {
                spans.push(Span::raw(word));
            }
            continue;
        }

        // Operator
        if matches!(
            bytes[i],
            b'=' | b'<' | b'>' | b'(' | b')' | b',' | b'*' | b';' | b'[' | b']'
        ) {
            spans.push(Span::styled(
                &line[i..i + 1],
                Style::default().fg(Color::Yellow),
            ));
            i += 1;
            continue;
        }

        // Whitespace or other
        let start = i;
        while i < len
            && !bytes[i].is_ascii_alphanumeric()
            && bytes[i] != b'_'
            && bytes[i] != b'\''
            && bytes[i] != b'-'
            && bytes[i] != b'\\'
            && !matches!(
                bytes[i],
                b'=' | b'<' | b'>' | b'(' | b')' | b',' | b'*' | b';' | b'[' | b']'
            )
        {
            i += 1;
        }
        if i > start {
            spans.push(Span::raw(&line[start..i]));
        }
    }

    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highlight_keywords() {
        let spans = highlight_line("SELECT * FROM users");
        assert!(spans.len() >= 4); // SELECT, *, FROM, users
        // SELECT should be magenta
        assert_eq!(spans[0].style.fg, Some(Color::Magenta));
    }

    #[test]
    fn highlight_string() {
        let spans = highlight_line("WHERE name = 'Alice'");
        let string_span = spans.iter().find(|s| s.content.contains("Alice")).unwrap();
        assert_eq!(string_span.style.fg, Some(Color::Green));
    }

    #[test]
    fn highlight_comment() {
        let spans = highlight_line("SELECT 1 -- comment");
        let comment = spans.last().unwrap();
        assert_eq!(comment.style.fg, Some(Color::DarkGray));
        assert!(comment.content.contains("comment"));
    }

    #[test]
    fn highlight_number() {
        let spans = highlight_line("LIMIT 100");
        let num = spans.iter().find(|s| s.content.contains("100")).unwrap();
        assert_eq!(num.style.fg, Some(Color::Cyan));
    }

    #[test]
    fn empty_line() {
        let spans = highlight_line("");
        assert!(spans.is_empty());
    }
}
