//! Parser utility functions: keyword search, comma splitting, etc.

/// Find a keyword at top level (not inside parentheses/brackets/braces).
pub(super) fn find_top_level_keyword(upper: &str, keyword: &str) -> Option<usize> {
    let mut depth = 0i32;
    let kw_len = keyword.len();
    let bytes = upper.as_bytes();

    for i in 0..upper.len().saturating_sub(kw_len - 1) {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b'\'' => {
                if let Some(end) = upper[i + 1..].find('\'') {
                    let _ = end;
                }
            }
            _ => {}
        }

        if depth == 0 && upper[i..].starts_with(keyword) {
            let before_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
            let after_ok = i + kw_len >= upper.len()
                || bytes[i + kw_len].is_ascii_whitespace()
                || bytes[i + kw_len] == b'(';
            if before_ok && after_ok {
                return Some(i);
            }
        }
    }
    None
}

/// Find the next MATCH or OPTIONAL MATCH keyword in text.
pub(super) fn find_next_match_keyword(upper: &str) -> Option<usize> {
    let trimmed_offset = upper.len() - upper.trim_start().len();
    let search = &upper[trimmed_offset..];

    for (i, _) in search.char_indices() {
        if i == 0 {
            continue;
        }
        let remaining = &search[i..];
        if (remaining.starts_with("OPTIONAL MATCH") || remaining.starts_with("MATCH"))
            && search
                .as_bytes()
                .get(i.wrapping_sub(1))
                .is_none_or(|b| b.is_ascii_whitespace())
        {
            return Some(trimmed_offset + i);
        }
    }
    None
}

/// Split text by commas at top level (not inside parentheses/brackets).
pub(super) fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;

    for (i, ch) in text.char_indices() {
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&text[start..]);
    parts
}

/// Split text by AND at top level.
pub(super) fn split_by_and(text: &str) -> Vec<&str> {
    let upper = text.to_uppercase();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0i32;

    let bytes = upper.as_bytes();
    for i in 0..text.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }

        if depth == 0 && upper[i..].starts_with(" AND ") {
            parts.push(&text[start..i]);
            start = i + 5;
        }
    }
    parts.push(&text[start..]);
    parts
}
