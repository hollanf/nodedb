//! Text analysis pipeline for full-text search indexing and querying.
//!
//! Pipeline stages (applied at both index time and query time):
//! 1. Unicode NFD normalization + strip combining marks
//! 2. Lowercase
//! 3. Split on non-alphanumeric boundaries (preserving hyphens within words)
//! 4. Filter empty tokens and single characters
//! 5. Remove English stop words (binary search on sorted list)
//! 6. Snowball English stemming
//!
//! Produces a `Vec<String>` of normalized, stemmed tokens suitable for
//! inverted index insertion and BM25 query matching.

use rust_stemmers::{Algorithm, Stemmer};
use unicode_normalization::UnicodeNormalization;

/// Analyze text into searchable tokens.
///
/// Apply the full pipeline: normalize → lowercase → split → filter →
/// stop words → stem. Used for both indexing and querying.
pub fn analyze(text: &str) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);
    let mut tokens = Vec::new();

    // Stage 1-2: NFD normalize, strip combining marks, lowercase.
    let normalized: String = text
        .nfd()
        .filter(|c| !c.is_ascii() || !unicode_normalization::char::is_combining_mark(*c))
        .flat_map(char::to_lowercase)
        .collect();

    // Stage 3: Split on non-alphanumeric boundaries.
    // Keep hyphens and underscores within words (e.g., "e-mail" stays together).
    for word in normalized.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_') {
        let trimmed = word.trim_matches(|c: char| c == '-' || c == '_');
        if trimmed.is_empty() {
            continue;
        }

        // Stage 4: Filter single characters.
        if trimmed.len() <= 1 {
            continue;
        }

        // Stage 5: Stop word removal.
        if is_stop_word(trimmed) {
            continue;
        }

        // Stage 6: Snowball English stemming.
        let stemmed = stemmer.stem(trimmed);
        if !stemmed.is_empty() {
            tokens.push(stemmed.into_owned());
        }
    }

    tokens
}

/// Check if a word is a common English stop word.
///
/// Uses binary search on a sorted static list for O(log n) lookup.
fn is_stop_word(word: &str) -> bool {
    STOP_WORDS.binary_search(&word).is_ok()
}

/// Sorted English stop words for binary search.
static STOP_WORDS: &[&str] = &[
    "a", "about", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can", "do", "for",
    "from", "had", "has", "have", "he", "her", "him", "his", "how", "if", "in", "into", "is", "it",
    "its", "just", "me", "my", "no", "not", "of", "on", "or", "our", "out", "own", "say", "she",
    "so", "some", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this",
    "to", "too", "up", "us", "very", "was", "we", "were", "what", "when", "which", "who", "will",
    "with", "would", "you", "your",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_analysis() {
        let tokens = analyze("The quick Brown FOX jumped over the lazy dog");
        // "the" is a stop word, removed. All lowercased. "jumped" stemmed to "jump".
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
        assert!(tokens.contains(&"fox".to_string()));
        assert!(tokens.contains(&"jump".to_string())); // stemmed
        assert!(tokens.contains(&"lazi".to_string())); // stemmed
        assert!(tokens.contains(&"dog".to_string()));
        assert!(!tokens.contains(&"the".to_string())); // stop word
    }

    #[test]
    fn stop_words_removed() {
        let tokens = analyze("this is a test of the system");
        // "this", "is", "a", "of", "the" are stop words.
        assert_eq!(tokens, vec!["test", "system"]);
    }

    #[test]
    fn stemming_works() {
        let tokens = analyze("running databases distributed systems");
        assert!(tokens.contains(&"run".to_string()));
        assert!(tokens.contains(&"databas".to_string()));
        assert!(tokens.contains(&"distribut".to_string()));
        assert!(tokens.contains(&"system".to_string()));
    }

    #[test]
    fn unicode_normalization() {
        let tokens = analyze("cafe\u{0301}"); // "café" with combining acute
        assert_eq!(tokens, vec!["cafe"]); // normalized to "cafe", stemmer keeps it
    }

    #[test]
    fn hyphenated_words_preserved() {
        let tokens = analyze("e-mail real-time");
        assert!(tokens.contains(&"e-mail".to_string()) || tokens.contains(&"email".to_string()));
        assert!(
            tokens.contains(&"real-tim".to_string()) || tokens.contains(&"real-time".to_string())
        );
    }

    #[test]
    fn empty_and_single_char_filtered() {
        let tokens = analyze("I a x  ");
        assert!(tokens.is_empty());
    }
}
