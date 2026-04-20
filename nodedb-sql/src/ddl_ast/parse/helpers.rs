//! Shared token-extraction helpers for the DDL parsers.

/// Extract the object name that follows a keyword (e.g. "COLLECTION"
/// in "CREATE COLLECTION users ..."). Handles IF NOT EXISTS by
/// skipping those tokens.
pub(super) fn extract_name_after_keyword(parts: &[&str], keyword: &str) -> Option<String> {
    let kw_upper = keyword.to_uppercase();
    let pos = parts.iter().position(|p| p.to_uppercase() == kw_upper)?;
    let mut idx = pos + 1;
    // Skip IF NOT EXISTS tokens.
    if parts.get(idx).map(|s| s.to_uppercase()) == Some("IF".to_string()) {
        idx += 1; // NOT
        if parts.get(idx).map(|s| s.to_uppercase()) == Some("NOT".to_string()) {
            idx += 1; // EXISTS
        }
        if parts.get(idx).map(|s| s.to_uppercase()) == Some("EXISTS".to_string()) {
            idx += 1;
        }
    }
    parts.get(idx).map(|s| s.to_string())
}

/// Extract the object name for DROP-style commands where IF EXISTS
/// may appear between the keyword and the name.
pub(super) fn extract_name_after_if_exists(parts: &[&str], keyword: &str) -> Option<String> {
    extract_name_after_keyword(parts, keyword)
}

/// Extract the token after a keyword like "ON" or "TO".
pub(super) fn extract_after_keyword(parts: &[&str], keyword: &str) -> Option<String> {
    let kw_upper = keyword.to_uppercase();
    let pos = parts.iter().position(|p| p.to_uppercase() == kw_upper)?;
    parts.get(pos + 1).map(|s| s.to_string())
}
