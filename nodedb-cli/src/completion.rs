//! Tab completion engine for SQL keywords, metacommands, and collection names.

use nodedb_client::NativeClient;

/// SQL keywords for completion (uppercase).
const SQL_KEYWORDS: &[&str] = &[
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
];

/// Metacommands for completion.
const METACOMMANDS: &[&str] = &[
    "\\d",
    "\\di",
    "\\du",
    "\\nodes",
    "\\node",
    "\\cluster",
    "\\raft",
    "\\migrations",
    "\\health",
    "\\rebalance",
    "\\s",
    "\\status",
    "\\connections",
    "\\format",
    "\\timing",
    "\\conninfo",
    "\\e",
    "\\i",
    "\\g",
    "\\watch",
    "\\x",
    "\\?",
    "\\q",
    "\\quit",
];

/// Completion engine state.
pub struct Completor {
    collections: Vec<String>,
}

/// Active completion popup state.
pub struct CompletionState {
    pub candidates: Vec<String>,
    pub selected: usize,
    pub prefix_start: usize,
    pub prefix_len: usize,
    pub active: bool,
}

impl CompletionState {
    pub fn empty() -> Self {
        Self {
            candidates: Vec::new(),
            selected: 0,
            prefix_start: 0,
            prefix_len: 0,
            active: false,
        }
    }

    /// Get the currently selected candidate, if any.
    pub fn selected_candidate(&self) -> Option<&str> {
        if self.candidates.is_empty() {
            return None;
        }
        self.candidates.get(self.selected).map(|s| s.as_str())
    }

    /// Cycle to next candidate.
    pub fn next(&mut self) {
        if !self.candidates.is_empty() {
            self.selected = (self.selected + 1) % self.candidates.len();
        }
    }

    /// Cycle to previous candidate.
    pub fn prev(&mut self) {
        if !self.candidates.is_empty() {
            self.selected = if self.selected == 0 {
                self.candidates.len() - 1
            } else {
                self.selected - 1
            };
        }
    }
}

impl Completor {
    pub fn new() -> Self {
        Self {
            collections: Vec::new(),
        }
    }

    /// Refresh cached collection names from the server.
    pub async fn refresh_collections(&mut self, client: &NativeClient) {
        if let Ok(result) = client.query("SHOW COLLECTIONS").await {
            self.collections = result
                .rows
                .iter()
                .filter_map(|row| row.first()?.as_str().map(String::from))
                .collect();
        }
    }

    /// Generate completions for the word at the cursor position.
    pub fn complete(&self, buffer: &str, cursor: usize) -> CompletionState {
        let (prefix_start, prefix) = extract_word_before_cursor(buffer, cursor);
        if prefix.is_empty() {
            return CompletionState::empty();
        }

        let mut candidates = Vec::new();

        if prefix.starts_with('\\') {
            // Complete metacommands.
            for mc in METACOMMANDS {
                if mc.starts_with(prefix) {
                    candidates.push(mc.to_string());
                }
            }
        } else {
            let upper = prefix.to_uppercase();
            // SQL keywords.
            for kw in SQL_KEYWORDS {
                if kw.starts_with(&upper) {
                    candidates.push(kw.to_string());
                }
            }
            // Collection names (case-insensitive prefix match).
            for c in &self.collections {
                if c.to_uppercase().starts_with(&upper) && !candidates.contains(c) {
                    candidates.push(c.clone());
                }
            }
        }

        if candidates.is_empty() {
            return CompletionState::empty();
        }

        // Sort: exact prefix matches first, then alphabetical.
        candidates.sort();

        CompletionState {
            candidates,
            selected: 0,
            prefix_start,
            prefix_len: prefix.len(),
            active: true,
        }
    }
}

/// Extract the word being typed at the cursor position.
/// Returns (start_byte_offset, word_slice).
fn extract_word_before_cursor(buffer: &str, cursor: usize) -> (usize, &str) {
    let before = &buffer[..cursor];
    // Walk backwards to find word start.
    let start = before
        .rfind(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == ',' || c == ';')
        .map(|i| i + 1)
        .unwrap_or(0);
    (start, &before[start..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_keyword() {
        let c = Completor::new();
        let state = c.complete("SEL", 3);
        assert!(state.active);
        assert!(state.candidates.contains(&"SELECT".to_string()));
    }

    #[test]
    fn complete_metacommand() {
        let c = Completor::new();
        let state = c.complete("\\no", 3);
        assert!(state.active);
        assert!(state.candidates.contains(&"\\nodes".to_string()));
    }

    #[test]
    fn complete_mid_query() {
        let c = Completor::new();
        let state = c.complete("SELECT * FR", 11);
        assert!(state.active);
        assert!(state.candidates.contains(&"FROM".to_string()));
    }

    #[test]
    fn no_match() {
        let c = Completor::new();
        let state = c.complete("xyzzy", 5);
        assert!(!state.active);
    }

    #[test]
    fn empty_prefix() {
        let c = Completor::new();
        let state = c.complete("SELECT ", 7);
        assert!(!state.active);
    }

    #[test]
    fn extract_word() {
        let (start, word) = extract_word_before_cursor("SELECT * FROM us", 16);
        assert_eq!(word, "us");
        assert_eq!(start, 14);
    }

    #[test]
    fn cycle_candidates() {
        let mut state = CompletionState {
            candidates: vec!["A".into(), "B".into(), "C".into()],
            selected: 0,
            prefix_start: 0,
            prefix_len: 0,
            active: true,
        };
        assert_eq!(state.selected_candidate(), Some("A"));
        state.next();
        assert_eq!(state.selected_candidate(), Some("B"));
        state.next();
        state.next();
        assert_eq!(state.selected_candidate(), Some("A")); // wraps
        state.prev();
        assert_eq!(state.selected_candidate(), Some("C"));
    }
}
