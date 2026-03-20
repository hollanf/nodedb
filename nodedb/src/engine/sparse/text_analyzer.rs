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

use std::collections::HashMap;

use rust_stemmers::{Algorithm, Stemmer};
use unicode_normalization::UnicodeNormalization;

/// Text analyzer trait: transforms raw text into searchable tokens.
///
/// Implementations must produce the same tokens for equivalent text at
/// both index time and query time (deterministic).
pub trait TextAnalyzer: Send + Sync {
    /// Analyze text into tokens.
    fn analyze(&self, text: &str) -> Vec<String>;

    /// Analyzer name (for serialization and config).
    fn name(&self) -> &str;
}

/// Standard English text analyzer (default).
///
/// Pipeline: NFD normalize → lowercase → split → filter → stop words → Snowball stem.
pub struct StandardAnalyzer;

impl TextAnalyzer for StandardAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        analyze(text)
    }

    fn name(&self) -> &str {
        "standard"
    }
}

/// Simple analyzer: lowercase + split on whitespace. No stemming or stop words.
///
/// Useful for exact-match fields (email addresses, tags, identifiers).
pub struct SimpleAnalyzer;

impl TextAnalyzer for SimpleAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .filter(|w| w.len() > 1)
            .map(|w| w.to_string())
            .collect()
    }

    fn name(&self) -> &str {
        "simple"
    }
}

/// Keyword analyzer: treats entire input as a single token (lowercase).
///
/// Used for fields where the entire value is the token (status fields,
/// enum-like values, exact-match tags).
pub struct KeywordAnalyzer;

impl TextAnalyzer for KeywordAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let trimmed = text.trim().to_lowercase();
        if trimmed.is_empty() {
            Vec::new()
        } else {
            vec![trimmed]
        }
    }

    fn name(&self) -> &str {
        "keyword"
    }
}

/// Language-specific analyzer: uses Snowball stemming for the configured language.
pub struct LanguageAnalyzer {
    algorithm: Algorithm,
    lang_name: String,
}

impl LanguageAnalyzer {
    pub fn new(language: &str) -> Option<Self> {
        let algorithm = match language.to_lowercase().as_str() {
            "english" | "en" => Algorithm::English,
            "german" | "de" => Algorithm::German,
            "french" | "fr" => Algorithm::French,
            "spanish" | "es" => Algorithm::Spanish,
            "italian" | "it" => Algorithm::Italian,
            "portuguese" | "pt" => Algorithm::Portuguese,
            "dutch" | "nl" => Algorithm::Dutch,
            "swedish" | "sv" => Algorithm::Swedish,
            "norwegian" | "no" => Algorithm::Norwegian,
            "danish" | "da" => Algorithm::Danish,
            "finnish" | "fi" => Algorithm::Finnish,
            "russian" | "ru" => Algorithm::Russian,
            "turkish" | "tr" => Algorithm::Turkish,
            "hungarian" | "hu" => Algorithm::Hungarian,
            "romanian" | "ro" => Algorithm::Romanian,
            _ => return None,
        };
        Some(Self {
            algorithm,
            lang_name: language.to_lowercase(),
        })
    }
}

impl TextAnalyzer for LanguageAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let stemmer = Stemmer::create(self.algorithm);
        let normalized: String = text
            .nfd()
            .filter(|c| !c.is_ascii() || !unicode_normalization::char::is_combining_mark(*c))
            .flat_map(char::to_lowercase)
            .collect();

        let mut tokens = Vec::new();
        for word in normalized.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_') {
            let trimmed = word.trim_matches(|c: char| c == '-' || c == '_');
            if trimmed.is_empty() || trimmed.len() <= 1 {
                continue;
            }
            if is_stop_word(trimmed) {
                continue;
            }
            let stemmed = stemmer.stem(trimmed);
            if !stemmed.is_empty() {
                tokens.push(stemmed.into_owned());
            }
        }
        tokens
    }

    fn name(&self) -> &str {
        &self.lang_name
    }
}

/// Registry of named text analyzers per collection.
///
/// Collections can configure their analyzer via:
/// `ALTER COLLECTION articles SET text_analyzer = 'german'`
///
/// If no analyzer is set, the standard English analyzer is used.
pub struct AnalyzerRegistry {
    /// Per-collection analyzer override: collection → analyzer instance.
    overrides: HashMap<String, Box<dyn TextAnalyzer>>,
}

impl AnalyzerRegistry {
    pub fn new() -> Self {
        Self {
            overrides: HashMap::new(),
        }
    }

    /// Set the analyzer for a collection.
    ///
    /// Supported names: "standard", "simple", "keyword", or any Snowball
    /// language ("english", "german", "french", "spanish", etc.).
    pub fn set_analyzer(&mut self, collection: &str, analyzer_name: &str) -> bool {
        let analyzer: Box<dyn TextAnalyzer> = match analyzer_name {
            "standard" => Box::new(StandardAnalyzer),
            "simple" => Box::new(SimpleAnalyzer),
            "keyword" => Box::new(KeywordAnalyzer),
            lang => match LanguageAnalyzer::new(lang) {
                Some(a) => Box::new(a),
                None => return false,
            },
        };
        self.overrides.insert(collection.to_string(), analyzer);
        true
    }

    /// Get the analyzer for a collection (falls back to standard).
    pub fn analyze(&self, collection: &str, text: &str) -> Vec<String> {
        match self.overrides.get(collection) {
            Some(analyzer) => analyzer.analyze(text),
            None => analyze(text),
        }
    }
}

impl Default for AnalyzerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Analyze text into searchable tokens using the standard English analyzer.
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
