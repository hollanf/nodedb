//! Shared utilities for the security subsystem.

/// Decode a base64url-encoded string (no padding required).
///
/// Handles the URL-safe alphabet (`-` and `_`) and adds padding as needed.
/// Used across JWT validation, JWKS key parsing, and token introspection.
pub fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    let padded = match input.len() % 4 {
        2 => format!("{input}=="),
        3 => format!("{input}="),
        _ => input.to_string(),
    };
    let standard = padded.replace('-', "+").replace('_', "/");
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(&standard)
        .ok()
}
