//! Fixed Content-Type for all HTTP JSON responses: application/vnd.nodedb.v1+json.

use axum::http::HeaderValue;
use axum::response::Response;

pub const CONTENT_TYPE_V1: &str = "application/vnd.nodedb.v1+json; charset=utf-8";

/// Stamp the canonical NodeDB v1 vendor Content-Type onto any JSON response.
///
/// Used as an `axum::middleware::map_response` layer on all `/v1/` JSON routes.
/// Non-JSON routes (SSE, WebSocket, plain-text probes) are kept on a separate
/// sub-router that does not carry this layer and are therefore unaffected.
pub async fn stamp_content_type(mut response: Response) -> Response {
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(CONTENT_TYPE_V1),
    );
    response
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Response as HttpResponse;

    #[tokio::test]
    async fn stamp_inserts_vendor_content_type() {
        let response = HttpResponse::builder()
            .body(Body::empty())
            .expect("valid response");
        let stamped = stamp_content_type(response).await;
        let ct = stamped
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("Content-Type header must be present after stamp");
        assert_eq!(
            ct.to_str().unwrap(),
            CONTENT_TYPE_V1,
            "stamped Content-Type must match the v1 vendor constant"
        );
    }

    #[tokio::test]
    async fn stamp_overwrites_existing_content_type() {
        let response = HttpResponse::builder()
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .expect("valid response");
        let stamped = stamp_content_type(response).await;
        let ct = stamped
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("Content-Type header must be present");
        assert_eq!(ct.to_str().unwrap(), CONTENT_TYPE_V1);
    }
}
