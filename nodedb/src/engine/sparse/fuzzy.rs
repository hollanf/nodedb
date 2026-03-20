//! Fuzzy string matching via Levenshtein edit distance.
//!
//! Used for typo-tolerant full-text search. When an exact term match
//! isn't found in the inverted index, fuzzy matching finds terms within
//! a configurable edit distance.
//!
//! Distance thresholds are adaptive based on token length:
//! - 1-3 chars: exact only (no fuzzy — too many false positives)
//! - 4-6 chars: max distance 1
//! - 7+ chars: max distance 2

/// Compute Levenshtein edit distance between two strings.
///
/// Uses the two-row matrix approach: O(min(a,b)) space, O(a*b) time.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let a_len = a_bytes.len();
    let b_len = b_bytes.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    // Optimize: always iterate over the shorter string for rows.
    if a_len > b_len {
        return levenshtein(b, a);
    }

    let mut prev_row: Vec<usize> = (0..=a_len).collect();
    let mut curr_row: Vec<usize> = vec![0; a_len + 1];

    for (j, b_byte) in b_bytes.iter().enumerate() {
        curr_row[0] = j + 1;
        for (i, a_byte) in a_bytes.iter().enumerate() {
            let cost = if a_byte == b_byte { 0 } else { 1 };
            curr_row[i + 1] = (prev_row[i + 1] + 1) // deletion
                .min(curr_row[i] + 1) // insertion
                .min(prev_row[i] + cost); // substitution
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[a_len]
}

/// Maximum allowed edit distance for a given token length.
///
/// Short tokens get no fuzzy matching (too many false positives).
/// Longer tokens allow more edits.
pub fn max_distance_for_length(len: usize) -> usize {
    match len {
        0..=3 => 0, // Exact only — "cat" → "bat" is too different
        4..=6 => 1, // One typo — "database" → "databse"
        _ => 2,     // Two typos — "distributed" → "distrubted"
    }
}

/// Find terms in the index that fuzzy-match the query term.
///
/// Returns `(matched_term, edit_distance)` pairs for all terms within
/// the adaptive distance threshold. Results are sorted by distance
/// (closest matches first).
///
/// `index_terms` should be an iterator over all terms in the inverted
/// index. For large indexes, consider prefix-bucketing to reduce the
/// scan space.
pub fn fuzzy_match<'a>(
    query_term: &str,
    index_terms: impl Iterator<Item = &'a str>,
) -> Vec<(&'a str, usize)> {
    let max_dist = max_distance_for_length(query_term.len());
    if max_dist == 0 {
        return Vec::new();
    }

    let mut matches: Vec<(&str, usize)> = index_terms
        .filter_map(|term| {
            // Quick length check — edit distance can't be less than
            // the length difference.
            let len_diff = query_term.len().abs_diff(term.len());
            if len_diff > max_dist {
                return None;
            }
            let dist = levenshtein(query_term, term);
            if dist > 0 && dist <= max_dist {
                Some((term, dist))
            } else {
                None
            }
        })
        .collect();

    matches.sort_by_key(|&(_, d)| d);
    matches
}

/// Score discount for fuzzy matches based on edit distance.
///
/// Exact match = 1.0 (not handled here — caller checks exact first).
/// Distance 1 = 0.7, Distance 2 = 0.4.
pub fn fuzzy_discount(distance: usize) -> f32 {
    match distance {
        0 => 1.0,
        1 => 0.7,
        2 => 0.4,
        _ => 0.2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_basic() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("", "xyz"), 3);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("saturday", "sunday"), 3);
    }

    #[test]
    fn levenshtein_same() {
        assert_eq!(levenshtein("database", "database"), 0);
    }

    #[test]
    fn levenshtein_one_edit() {
        assert_eq!(levenshtein("database", "databse"), 1); // missing 'a'
        assert_eq!(levenshtein("index", "indx"), 1); // missing 'e'
    }

    #[test]
    fn max_distance_thresholds() {
        assert_eq!(max_distance_for_length(2), 0); // "db" — no fuzzy
        assert_eq!(max_distance_for_length(3), 0); // "cat" — no fuzzy
        assert_eq!(max_distance_for_length(4), 1); // "data" — 1 edit
        assert_eq!(max_distance_for_length(6), 1); // "search" — 1 edit
        assert_eq!(max_distance_for_length(7), 2); // "databas" — 2 edits
        assert_eq!(max_distance_for_length(10), 2);
    }

    #[test]
    fn fuzzy_match_finds_typos() {
        let index = ["database", "distributed", "document", "data", "date"];
        let matches = fuzzy_match("databse", index.iter().copied());
        assert!(!matches.is_empty());
        assert_eq!(matches[0].0, "database");
        assert_eq!(matches[0].1, 1);
    }

    #[test]
    fn fuzzy_match_respects_length_threshold() {
        let index = ["cat", "bat", "car"];
        // "cat" is 3 chars → max_dist = 0 → no fuzzy
        let matches = fuzzy_match("cat", index.iter().copied());
        assert!(matches.is_empty());
    }
}
