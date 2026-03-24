//! Highlight and match offset utilities for the inverted index.

use super::inverted::{InvertedIndex, MatchOffset};
use super::text_analyzer;

impl InvertedIndex {
    /// Generate highlighted text with matched query terms wrapped in tags.
    ///
    /// Returns the original text with each occurrence of a matched query term
    /// surrounded by `prefix` and `suffix` (e.g., `<b>` and `</b>`).
    pub fn highlight(&self, text: &str, query: &str, prefix: &str, suffix: &str) -> String {
        let query_tokens = text_analyzer::analyze(query);
        if query_tokens.is_empty() {
            return text.to_string();
        }

        let query_set: std::collections::HashSet<&str> =
            query_tokens.iter().map(String::as_str).collect();
        let stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);

        let mut result = String::with_capacity(
            text.len() + query_tokens.len() * (prefix.len() + suffix.len()) * 2,
        );
        let mut last_end = 0;

        for (start, word) in WordBoundaryIter::new(text) {
            let end = start + word.len();
            let lower = word.to_lowercase();
            let stemmed = stemmer.stem(&lower);

            if query_set.contains(stemmed.as_ref()) {
                result.push_str(&text[last_end..start]);
                result.push_str(prefix);
                result.push_str(word);
                result.push_str(suffix);
                last_end = end;
            }
        }
        result.push_str(&text[last_end..]);
        result
    }

    /// Return byte offsets of matched query terms in the original text.
    ///
    /// Each `MatchOffset` contains the start/end byte positions and the
    /// matched stemmed term.
    pub fn offsets(&self, text: &str, query: &str) -> Vec<MatchOffset> {
        let query_tokens = text_analyzer::analyze(query);
        if query_tokens.is_empty() {
            return Vec::new();
        }

        let query_set: std::collections::HashSet<&str> =
            query_tokens.iter().map(String::as_str).collect();
        let stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);

        let mut matches = Vec::new();
        for (start, word) in WordBoundaryIter::new(text) {
            let lower = word.to_lowercase();
            let stemmed = stemmer.stem(&lower);
            if query_set.contains(stemmed.as_ref()) {
                matches.push(MatchOffset {
                    start,
                    end: start + word.len(),
                    term: stemmed.into_owned(),
                });
            }
        }
        matches
    }
}

/// Iterator over word boundaries in text, yielding `(byte_offset, &str)` pairs.
///
/// Words are sequences of alphanumeric chars, hyphens, and underscores.
struct WordBoundaryIter<'a> {
    text: &'a str,
    pos: usize,
}

impl<'a> WordBoundaryIter<'a> {
    fn new(text: &'a str) -> Self {
        Self { text, pos: 0 }
    }
}

impl<'a> Iterator for WordBoundaryIter<'a> {
    type Item = (usize, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.text.as_bytes();
        // Skip non-word characters.
        while self.pos < bytes.len() {
            let c = self.text[self.pos..].chars().next()?;
            if c.is_alphanumeric() || c == '-' || c == '_' {
                break;
            }
            self.pos += c.len_utf8();
        }
        if self.pos >= bytes.len() {
            return None;
        }

        let start = self.pos;
        // Consume word characters.
        while self.pos < bytes.len() {
            let Some(c) = self.text[self.pos..].chars().next() else {
                break;
            };
            if c.is_alphanumeric() || c == '-' || c == '_' {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
        let word = &self.text[start..self.pos];
        let trimmed_start = start + word.len() - word.trim_start_matches(['-', '_']).len();
        let trimmed_end = trimmed_start + word.trim_matches(['-', '_']).len();
        if trimmed_start >= trimmed_end {
            return self.next();
        }
        Some((trimmed_start, &self.text[trimmed_start..trimmed_end]))
    }
}
