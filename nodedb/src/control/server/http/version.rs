//! HTTP content-version negotiation for `application/vnd.nodedb.v{N}+json`.
//!
//! ## Accept header semantics
//!
//! - `*/*` or `application/json` → default V1 (no 406).
//! - `application/vnd.nodedb.v1+json` → V1.
//! - Any Accept that contains only vendor `application/vnd.nodedb.*` types
//!   and none of them match a supported version → 406 Not Acceptable.
//! - Mixed Accept containing at least one non-vendor media type (e.g.
//!   `application/vnd.nodedb.v99+json, application/json`) → the non-vendor
//!   type acts as a wildcard, default V1 is used, no 406.
//! - Absent Accept header → default V1 (RFC 7231 §5.3.2: absence = `*/*`).
//!
//! ## Response Content-Type
//!
//! JSON routes: `application/vnd.nodedb.v1+json; charset=utf-8`
//! Streaming / SSE routes: middleware sets a `ContentVersion` request
//! extension so handlers can read the negotiated version; Content-Type is
//! left to the handler (NDJson / SSE has its own type).

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// The supported API versions, in ascending order.
pub const SUPPORTED_VERSIONS: &[u32] = &[1];

/// The canonical `Content-Type` value for V1 JSON responses.
pub const CONTENT_TYPE_V1: &str = "application/vnd.nodedb.v1+json; charset=utf-8";

/// The vendor media-type prefix used in `Accept` / `Content-Type` headers.
const VENDOR_PREFIX: &str = "application/vnd.nodedb.v";
const VENDOR_SUFFIX: &str = "+json";

/// Negotiated content version, stored as a request extension by the middleware
/// so that handlers can read it without re-parsing the `Accept` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentVersion(pub u32);

/// Errors produced during `Accept` header negotiation.
#[derive(Debug, thiserror::Error)]
pub enum VersionError {
    #[error(
        "None of the requested media types are supported. Supported: application/vnd.nodedb.v1+json"
    )]
    NotAcceptable,
}

impl IntoResponse for VersionError {
    fn into_response(self) -> Response {
        (
            StatusCode::NOT_ACCEPTABLE,
            axum::Json(serde_json::json!({
                "error": "not_acceptable",
                "message": self.to_string(),
                "supported": SUPPORTED_VERSIONS,
            })),
        )
            .into_response()
    }
}

/// Parse an `Accept` header value and return the highest supported version, or
/// `Err(VersionError::NotAcceptable)` if the caller explicitly requested only
/// unsupported vendor versions.
///
/// Returns `Ok(ContentVersion(1))` when no vendor-specific type was requested.
pub fn negotiate(accept: &str) -> Result<ContentVersion, VersionError> {
    let mut any_vendor = false;
    let mut best: Option<u32> = None;

    for token in accept.split(',') {
        let media = token.split(';').next().unwrap_or("").trim();

        if media == "*/*" || media == "application/json" {
            // Non-vendor wildcard → default V1, done.
            return Ok(ContentVersion(1));
        }

        if let Some(version) = parse_vendor_version(media) {
            any_vendor = true;
            if SUPPORTED_VERSIONS.contains(&version) {
                match best {
                    None => best = Some(version),
                    Some(b) if version > b => best = Some(version),
                    _ => {}
                }
            }
        } else if !media.is_empty() {
            // Any other non-vendor, non-empty media type acts as a fallback.
            return Ok(ContentVersion(1));
        }
    }

    if any_vendor {
        match best {
            Some(v) => Ok(ContentVersion(v)),
            None => Err(VersionError::NotAcceptable),
        }
    } else {
        // No vendor tokens and no fallback type seen → absent / empty Accept.
        Ok(ContentVersion(1))
    }
}

/// Try to extract the version number from `application/vnd.nodedb.vN+json`.
fn parse_vendor_version(media: &str) -> Option<u32> {
    let after_prefix = media.strip_prefix(VENDOR_PREFIX)?;
    let version_str = after_prefix.strip_suffix(VENDOR_SUFFIX)?;
    version_str.parse::<u32>().ok()
}

/// Axum middleware that performs `Accept` header negotiation and:
///
/// * Inserts a [`ContentVersion`] extension on the request so handlers can
///   read the negotiated version.
/// * Returns `406 Not Acceptable` when the client only accepts unsupported
///   vendor versions.
/// * Does **not** mutate the response `Content-Type` — individual handlers
///   or a response wrapper are responsible for setting it.
pub async fn content_version_middleware(mut req: Request<Body>, next: Next) -> Response {
    let accept = req
        .headers()
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();

    match negotiate(&accept) {
        Ok(version) => {
            req.extensions_mut().insert(version);
            next.run(req).await
        }
        Err(e) => e.into_response(),
    }
}

/// Wrap a JSON body with the vendor `Content-Type` for the negotiated version.
///
/// Handlers that produce `application/json` responses should call this (or use
/// [`vnd_json_response`]) to stamp the correct vendor type onto the response.
pub fn stamp_content_type(mut response: Response, version: ContentVersion) -> Response {
    let ct = match version.0 {
        1 => CONTENT_TYPE_V1,
        // Future versions: add arms here.
        _ => CONTENT_TYPE_V1,
    };
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(ct),
    );
    response
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_vendor_v1_succeeds() {
        let result = negotiate("application/vnd.nodedb.v1+json");
        assert_eq!(result.unwrap(), ContentVersion(1));
    }

    #[test]
    fn accept_vendor_v99_rejected_with_406() {
        let result = negotiate("application/vnd.nodedb.v99+json");
        assert!(
            matches!(result, Err(VersionError::NotAcceptable)),
            "unsupported vendor version must produce NotAcceptable"
        );
    }

    #[test]
    fn accept_wildcard_returns_default_v1() {
        let result = negotiate("*/*");
        assert_eq!(result.unwrap(), ContentVersion(1));
    }

    #[test]
    fn accept_application_json_returns_default_v1() {
        let result = negotiate("application/json");
        assert_eq!(result.unwrap(), ContentVersion(1));
    }

    #[test]
    fn absent_accept_returns_default_v1() {
        let result = negotiate("");
        assert_eq!(result.unwrap(), ContentVersion(1));
    }

    #[test]
    fn mixed_accept_with_v1_vendor_succeeds() {
        // Client sends both a vendor type and a wildcard fallback.
        let result = negotiate("application/vnd.nodedb.v1+json, */*");
        assert_eq!(result.unwrap(), ContentVersion(1));
    }

    #[test]
    fn mixed_accept_with_v99_and_application_json_uses_fallback() {
        // Unsupported vendor + non-vendor fallback → use the non-vendor fallback (V1).
        let result = negotiate("application/vnd.nodedb.v99+json, application/json");
        assert_eq!(
            result.unwrap(),
            ContentVersion(1),
            "non-vendor media type acts as fallback, no 406"
        );
    }

    #[test]
    fn only_unsupported_vendor_types_produce_406() {
        // Strictly vendor-only Accept with no supported version → 406.
        let result = negotiate("application/vnd.nodedb.v99+json, application/vnd.nodedb.v100+json");
        assert!(
            matches!(result, Err(VersionError::NotAcceptable)),
            "all-unsupported-vendor Accept must produce NotAcceptable"
        );
    }

    #[test]
    fn parse_vendor_version_valid() {
        assert_eq!(
            parse_vendor_version("application/vnd.nodedb.v1+json"),
            Some(1)
        );
        assert_eq!(
            parse_vendor_version("application/vnd.nodedb.v42+json"),
            Some(42)
        );
    }

    #[test]
    fn parse_vendor_version_invalid() {
        assert_eq!(parse_vendor_version("application/json"), None);
        assert_eq!(parse_vendor_version("*/*"), None);
        assert_eq!(parse_vendor_version("application/vnd.nodedb.vX+json"), None);
    }
}
