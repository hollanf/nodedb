//! Synonym expansion for query-time term enrichment.

use std::collections::HashMap;

/// Synonym map: expands query terms with their synonyms at query time.
///
/// Each entry maps a term to a set of synonym terms. When a query term
/// matches a synonym key, all synonym values are added to the query.
/// Applied after tokenization/stemming, so synonyms should be in
/// stemmed form (e.g., "db" → ["databas"], not "database").
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
        // no-governor: hot-path per-query token expansion; bounded by tokens.len()*2 (small for typical queries)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synonym_expansion() {
        let mut syn = SynonymMap::new();
        syn.add("db", &["databas", "rdbms"]);
        let tokens = vec!["db".to_string(), "query".to_string()];
        let expanded = syn.expand(&tokens);
        assert_eq!(expanded.len(), 4);
        assert!(expanded.contains(&"databas".to_string()));
        assert!(expanded.contains(&"rdbms".to_string()));
    }

    #[test]
    fn empty_synonym_map() {
        let syn = SynonymMap::new();
        assert!(syn.is_empty());
        assert_eq!(syn.len(), 0);
        let tokens = vec!["hello".to_string()];
        assert_eq!(syn.expand(&tokens), tokens);
    }
}
