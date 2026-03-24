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
        tokenize_with_stemmer(text, &stemmer)
    }

    fn name(&self) -> &str {
        &self.lang_name
    }
}

/// N-gram analyzer: generates all character n-grams of sizes min..=max for each token.
///
/// Useful for substring matching and partial-word search (e.g., autocomplete).
/// Example: "database" with min=3, max=4 → ["dat", "ata", "tab", "aba", "bas", "ase", "data", "atab", "taba", "abas", "base"]
pub struct NgramAnalyzer {
    min: usize,
    max: usize,
}

impl NgramAnalyzer {
    pub fn new(min: usize, max: usize) -> Self {
        Self {
            min: min.max(1),
            max: max.max(min.max(1)),
        }
    }
}

impl TextAnalyzer for NgramAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let lower = text.to_lowercase();
        let mut ngrams = Vec::new();
        for word in lower.split(|c: char| !c.is_alphanumeric()) {
            if word.is_empty() {
                continue;
            }
            let chars: Vec<char> = word.chars().collect();
            for n in self.min..=self.max {
                if n > chars.len() {
                    break;
                }
                for window in chars.windows(n) {
                    ngrams.push(window.iter().collect());
                }
            }
        }
        ngrams
    }

    fn name(&self) -> &str {
        "ngram"
    }
}

/// Edge n-gram analyzer: generates n-grams anchored to the start of each token.
///
/// Useful for prefix/autocomplete search.
/// Example: "database" with min=2, max=5 → ["da", "dat", "data", "datab"]
pub struct EdgeNgramAnalyzer {
    min: usize,
    max: usize,
}

impl EdgeNgramAnalyzer {
    pub fn new(min: usize, max: usize) -> Self {
        Self {
            min: min.max(1),
            max: max.max(min.max(1)),
        }
    }
}

impl TextAnalyzer for EdgeNgramAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let lower = text.to_lowercase();
        let mut ngrams = Vec::new();
        for word in lower.split(|c: char| !c.is_alphanumeric()) {
            if word.is_empty() {
                continue;
            }
            let chars: Vec<char> = word.chars().collect();
            for n in self.min..=self.max.min(chars.len()) {
                ngrams.push(chars[..n].iter().collect());
            }
        }
        ngrams
    }

    fn name(&self) -> &str {
        "edge_ngram"
    }
}

/// Synonym map: expands query terms with their synonyms at query time.
///
/// Each entry maps a term to a set of synonym terms. When a query term
/// matches a synonym key, all synonym values are added to the query.
/// Applied after tokenization/stemming, so synonyms should be in
/// stemmed form (e.g., "db" → ["databas"], not "database").
///
/// Example:
/// ```ignore
/// let mut synonyms = SynonymMap::new();
/// synonyms.add("db", &["databas", "rdbms"]);
/// synonyms.add("ml", &["machin", "learn"]);
/// ```
pub struct SynonymMap {
    /// term → [synonym_terms]. All keys and values are lowercased.
    entries: HashMap<String, Vec<String>>,
}

impl SynonymMap {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Add a synonym mapping. Both `term` and `synonyms` are lowercased.
    pub fn add(&mut self, term: &str, synonyms: &[&str]) {
        let key = term.to_lowercase();
        let vals: Vec<String> = synonyms.iter().map(|s| s.to_lowercase()).collect();
        self.entries.insert(key, vals);
    }

    /// Expand a list of tokens with their synonyms.
    ///
    /// Returns a new token list containing the originals plus any
    /// synonym expansions. Duplicates are preserved (BM25 handles
    /// term frequency naturally).
    pub fn expand(&self, tokens: &[String]) -> Vec<String> {
        let mut expanded = Vec::with_capacity(tokens.len() * 2);
        for token in tokens {
            expanded.push(token.clone());
            if let Some(synonyms) = self.entries.get(token.as_str()) {
                expanded.extend(synonyms.iter().cloned());
            }
        }
        expanded
    }

    /// Number of synonym entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for SynonymMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry of named text analyzers and synonym maps per collection.
///
/// Collections can configure their analyzer via:
/// `ALTER COLLECTION articles SET text_analyzer = 'german'`
///
/// Synonyms are configured via:
/// `ALTER COLLECTION articles ADD SYNONYM 'DB' => 'database'`
///
/// If no analyzer is set, the standard English analyzer is used.
pub struct AnalyzerRegistry {
    /// Per-collection analyzer override: collection → analyzer instance.
    overrides: HashMap<String, Box<dyn TextAnalyzer>>,
    /// Per-collection synonym maps: collection → SynonymMap.
    synonyms: HashMap<String, SynonymMap>,
}

impl AnalyzerRegistry {
    pub fn new() -> Self {
        Self {
            overrides: HashMap::new(),
            synonyms: HashMap::new(),
        }
    }

    /// Set the analyzer for a collection.
    ///
    /// Supported names: "standard", "simple", "keyword", or any Snowball
    /// language ("english", "german", "french", "spanish", etc.).
    /// Set the analyzer for a collection.
    ///
    /// Supported names: "standard", "simple", "keyword", "ngram", "edge_ngram",
    /// or any Snowball language ("english", "german", "french", etc.).
    ///
    /// N-gram analyzers accept optional parameters: "ngram:2:4" (min:max).
    pub fn set_analyzer(&mut self, collection: &str, analyzer_name: &str) -> bool {
        let analyzer: Box<dyn TextAnalyzer> = match analyzer_name {
            "standard" => Box::new(StandardAnalyzer),
            "simple" => Box::new(SimpleAnalyzer),
            "keyword" => Box::new(KeywordAnalyzer),
            "ngram" => Box::new(NgramAnalyzer::new(3, 4)),
            "edge_ngram" => Box::new(EdgeNgramAnalyzer::new(2, 5)),
            name if name.starts_with("ngram:") => {
                let parts: Vec<&str> = name.splitn(3, ':').collect();
                let min = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(3);
                let max = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
                Box::new(NgramAnalyzer::new(min, max))
            }
            name if name.starts_with("edge_ngram:") => {
                let parts: Vec<&str> = name.splitn(3, ':').collect();
                let min = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
                let max = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
                Box::new(EdgeNgramAnalyzer::new(min, max))
            }
            lang => match LanguageAnalyzer::new(lang) {
                Some(a) => Box::new(a),
                None => return false,
            },
        };
        self.overrides.insert(collection.to_string(), analyzer);
        true
    }

    /// Add a synonym for a collection. Both term and synonyms are lowercased.
    pub fn add_synonym(&mut self, collection: &str, term: &str, synonyms: &[&str]) {
        self.synonyms
            .entry(collection.to_string())
            .or_default()
            .add(term, synonyms);
    }

    /// Get the synonym map for a collection (if any).
    pub fn get_synonyms(&self, collection: &str) -> Option<&SynonymMap> {
        self.synonyms.get(collection)
    }

    /// Analyze text for a collection, applying synonym expansion at query time.
    ///
    /// For indexing, call `analyze_for_index()` (no synonym expansion).
    /// For querying, call `analyze()` (with synonym expansion).
    pub fn analyze(&self, collection: &str, text: &str) -> Vec<String> {
        let tokens = match self.overrides.get(collection) {
            Some(analyzer) => analyzer.analyze(text),
            None => analyze(text),
        };
        // Apply synonym expansion.
        match self.synonyms.get(collection) {
            Some(syn_map) if !syn_map.is_empty() => syn_map.expand(&tokens),
            _ => tokens,
        }
    }

    /// Analyze text for indexing (no synonym expansion).
    pub fn analyze_for_index(&self, collection: &str, text: &str) -> Vec<String> {
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
    tokenize_with_stemmer(text, &stemmer)
}

/// Shared tokenization pipeline used by both the standard `analyze()` function
/// and `LanguageAnalyzer`. Normalizes Unicode (NFD + strip combining marks +
/// lowercase), splits on word boundaries, removes stop words, and stems.
fn tokenize_with_stemmer(text: &str, stemmer: &Stemmer) -> Vec<String> {
    // Stage 1-2: NFD normalize, strip combining marks, lowercase.
    let normalized: String = text
        .nfd()
        .filter(|c| !c.is_ascii() || !unicode_normalization::char::is_combining_mark(*c))
        .flat_map(char::to_lowercase)
        .collect();

    let mut tokens = Vec::new();

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

        // Stage 6: Snowball stemming.
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

    #[test]
    fn synonym_expansion() {
        let mut syn = SynonymMap::new();
        syn.add("db", &["databas", "rdbms"]);
        let tokens = vec!["db".to_string(), "query".to_string()];
        let expanded = syn.expand(&tokens);
        assert_eq!(expanded.len(), 4); // db, databas, rdbms, query
        assert!(expanded.contains(&"databas".to_string()));
        assert!(expanded.contains(&"rdbms".to_string()));
    }

    #[test]
    fn analyzer_registry_with_synonyms() {
        let mut registry = AnalyzerRegistry::new();
        registry.add_synonym("docs", "db", &["databas"]);

        // Query-time analysis includes synonym expansion.
        let tokens = registry.analyze("docs", "db query");
        assert!(tokens.contains(&"databas".to_string()));

        // Index-time analysis does NOT expand synonyms.
        let index_tokens = registry.analyze_for_index("docs", "db query");
        assert!(!index_tokens.contains(&"databas".to_string()));
    }

    #[test]
    fn simple_analyzer() {
        let analyzer = SimpleAnalyzer;
        let tokens = analyzer.analyze("Hello World foo");
        assert_eq!(tokens, vec!["hello", "world", "foo"]);
    }

    #[test]
    fn keyword_analyzer() {
        let analyzer = KeywordAnalyzer;
        let tokens = analyzer.analyze("Active Status");
        assert_eq!(tokens, vec!["active status"]);
    }

    #[test]
    fn language_analyzer_german() {
        let analyzer = LanguageAnalyzer::new("german").unwrap();
        let tokens = analyzer.analyze("Die Datenbanken sind schnell");
        // German stemming should apply.
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t == &t.to_lowercase()));
    }

    #[test]
    fn ngram_analyzer() {
        let analyzer = NgramAnalyzer::new(3, 4);
        let tokens = analyzer.analyze("hello");
        // 3-grams: hel, ell, llo  (3)
        // 4-grams: hell, ello     (2)
        assert_eq!(tokens.len(), 5);
        assert!(tokens.contains(&"hel".to_string()));
        assert!(tokens.contains(&"ell".to_string()));
        assert!(tokens.contains(&"llo".to_string()));
        assert!(tokens.contains(&"hell".to_string()));
        assert!(tokens.contains(&"ello".to_string()));
    }

    #[test]
    fn ngram_short_word() {
        let analyzer = NgramAnalyzer::new(3, 5);
        let tokens = analyzer.analyze("ab");
        // "ab" is shorter than min=3, no n-grams produced.
        assert!(tokens.is_empty());
    }

    #[test]
    fn edge_ngram_analyzer() {
        let analyzer = EdgeNgramAnalyzer::new(2, 5);
        let tokens = analyzer.analyze("database");
        // 2: "da", 3: "dat", 4: "data", 5: "datab"
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], "da");
        assert_eq!(tokens[1], "dat");
        assert_eq!(tokens[2], "data");
        assert_eq!(tokens[3], "datab");
    }

    #[test]
    fn edge_ngram_multiple_words() {
        let analyzer = EdgeNgramAnalyzer::new(2, 3);
        let tokens = analyzer.analyze("foo bar");
        // "fo", "foo", "ba", "bar"
        assert_eq!(tokens.len(), 4);
        assert!(tokens.contains(&"fo".to_string()));
        assert!(tokens.contains(&"foo".to_string()));
        assert!(tokens.contains(&"ba".to_string()));
        assert!(tokens.contains(&"bar".to_string()));
    }

    #[test]
    fn registry_ngram_with_params() {
        let mut registry = AnalyzerRegistry::new();
        assert!(registry.set_analyzer("col", "ngram:2:3"));
        let tokens = registry.analyze_for_index("col", "hello");
        // 2-grams: he, el, ll, lo (4), 3-grams: hel, ell, llo (3) = 7
        assert_eq!(tokens.len(), 7);
        assert!(tokens.contains(&"he".to_string()));
    }

    #[test]
    fn registry_edge_ngram() {
        let mut registry = AnalyzerRegistry::new();
        assert!(registry.set_analyzer("col", "edge_ngram:1:3"));
        let tokens = registry.analyze_for_index("col", "test");
        // 1: "t", 2: "te", 3: "tes"
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], "t");
        assert_eq!(tokens[2], "tes");
    }
}
