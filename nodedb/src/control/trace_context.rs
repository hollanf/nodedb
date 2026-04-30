//! End-to-end trace propagation across all planes.
//!
//! Ensures `trace_id` flows from:
//! 1. Client request (HTTP header `traceparent` / `X-Trace-ID` or pgwire parameter)
//! 2. Control Plane (DataFusion planning, routing)
//! 3. SPSC bridge envelope (`Request.trace_id` field)
//! 4. Data Plane execution (CoreLoop logging)
//! 5. Network hops (cluster transport `ExecuteRequest`)
//!
//! If the client doesn't supply a trace ID, one is generated via
//! `TraceId::generate()`.

use nodedb_types::TraceId;

/// Generate a fresh, random `TraceId`.
pub fn generate_trace_id() -> TraceId {
    TraceId::generate()
}

/// Extract a `TraceId` from HTTP request headers.
///
/// Priority order:
/// 1. W3C `traceparent` (full 16-byte trace ID extracted from the 32-hex field).
/// 2. `X-Trace-ID` (32 hex chars interpreted as 16 bytes).
/// 3. Generate a fresh ID.
pub fn extract_from_headers(headers: &axum::http::HeaderMap) -> TraceId {
    // 1. W3C traceparent: "00-<32hex>-<16hex>-<2hex>"
    if let Some(val) = headers.get("traceparent")
        && let Ok(s) = val.to_str()
        && let Some((tid, _sid, _flags)) = TraceId::from_traceparent(s)
    {
        return tid;
    }

    // 2. X-Trace-ID: 32 lowercase hex chars.
    if let Some(val) = headers.get("x-trace-id")
        && let Ok(s) = val.to_str()
        && let Ok(tid) = s.parse::<TraceId>()
    {
        return tid;
    }

    // 3. No trace ID supplied — generate one.
    TraceId::generate()
}

/// Extract a `TraceId` from a pgwire startup parameter map.
///
/// PostgreSQL clients can pass custom parameters during connection:
/// `options=-c trace_id=<32 hex chars>`
pub fn extract_from_pgwire_params(params: &std::collections::HashMap<String, String>) -> TraceId {
    if let Some(val) = params.get("trace_id")
        && let Ok(tid) = val.parse::<TraceId>()
    {
        return tid;
    }
    TraceId::generate()
}

/// Create a tracing span with the `trace_id` attached via its `Display` form
/// (32 lowercase hex chars).
///
/// ```ignore
/// let span = trace_context::make_span(trace_id, "query");
/// async { ... }.instrument(span).await;
/// ```
pub fn make_span(trace_id: TraceId, operation: &str) -> tracing::Span {
    tracing::info_span!("op", trace_id = %trace_id, operation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_unique_ids() {
        let id1 = generate_trace_id();
        let id2 = generate_trace_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn generate_produces_nonzero_id() {
        let id = generate_trace_id();
        assert_ne!(id, TraceId::ZERO);
    }

    #[test]
    fn extract_from_traceparent_header() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        let id = extract_from_headers(&headers);
        assert_eq!(id.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn extract_from_x_trace_id_header() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-trace-id",
            "4bf92f3577b34da6a3ce929d0e0e4736".parse().unwrap(),
        );
        let id = extract_from_headers(&headers);
        assert_eq!(id.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn traceparent_takes_priority_over_x_trace_id() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        headers.insert(
            "x-trace-id",
            "aaaabbbbccccddddaaaabbbbccccdddd".parse().unwrap(),
        );
        let id = extract_from_headers(&headers);
        assert_eq!(id.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn extract_generates_when_missing() {
        let headers = axum::http::HeaderMap::new();
        let id = extract_from_headers(&headers);
        assert_ne!(id, TraceId::ZERO);
    }

    #[test]
    fn pgwire_param_extraction_roundtrips() {
        let tid = TraceId::generate();
        let mut params = std::collections::HashMap::new();
        params.insert("trace_id".into(), tid.to_string());
        let extracted = extract_from_pgwire_params(&params);
        assert_eq!(extracted, tid);
    }

    #[test]
    fn pgwire_param_generates_when_missing() {
        let params = std::collections::HashMap::new();
        let id = extract_from_pgwire_params(&params);
        assert_ne!(id, TraceId::ZERO);
    }
}
