use std::borrow::Cow;

pub(super) enum Tok<'a> {
    Word(&'a str),
    Quoted(Cow<'a, str>),
    /// Brace-balanced object literal including outer braces.
    Object(&'a str),
}

pub(super) fn tokenize(sql: &str) -> Vec<Tok<'_>> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace()
            || b == b','
            || b == b';'
            || b == b'('
            || b == b')'
            || b == b'['
            || b == b']'
        {
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
            || c == b'['
            || c == b']'
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
