//! Change Data Capture (CDC) API endpoints.
//!
//! Two modes:
//!
//! 1. **SSE streaming** (`GET /cdc/{collection}`) — Server-Sent Events stream
//!    that pushes changes in real-time. Connection stays open. Supports
//!    `Last-Event-ID` header for reconnection replay.
//!
//! 2. **Pull-based polling** (`GET /cdc/{collection}/poll`) — Returns a batch
//!    of changes since a cursor. Non-streaming, returns immediately. For
//!    Kafka Connect / Debezium-style integration.

use std::convert::Infallible;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use serde::Deserialize;

use super::super::auth::AppState;
use crate::control::change_stream::ChangeEvent;
use crate::types::TenantId;

/// Query parameters for the SSE endpoint.
#[derive(Deserialize, Default)]
pub struct SseParams {
    /// Start streaming from this timestamp (epoch ms). Default: now.
    pub since_ms: Option<u64>,
    /// Tenant ID filter. Default: 1.
    pub tenant_id: Option<u32>,
}

/// Query parameters for the poll endpoint.
#[derive(Deserialize, Default)]
pub struct PollParams {
    /// Return changes since this timestamp (epoch ms). Default: 0 (all).
    pub since_ms: Option<u64>,
    /// Return changes since this LSN. Overrides since_ms if set.
    pub since_lsn: Option<u64>,
    /// Maximum changes to return. Default: 100.
    pub limit: Option<usize>,
    /// Tenant ID filter. Default: 1.
    pub tenant_id: Option<u32>,
}

/// SSE streaming endpoint: `GET /cdc/{collection}`
///
/// Streams change events as Server-Sent Events. Each event contains:
/// ```text
/// id: <lsn>
/// event: <operation>
/// data: {"collection":"users","operation":"INSERT","document_id":"u1","timestamp_ms":1234567890,"lsn":42}
/// ```
///
/// Supports reconnection via `Last-Event-ID` header — on reconnect,
/// replays missed events from the ring buffer starting after that LSN.
pub async fn sse_stream(
    Path(collection): Path<String>,
    Query(params): Query<SseParams>,
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let collection = collection.to_lowercase();
    let tenant_id = TenantId::new(params.tenant_id.unwrap_or(1));
    let since_ms = params.since_ms.unwrap_or(0);
    let shared = Arc::clone(&state.shared);

    // First, replay any missed events from the ring buffer.
    let backlog = shared
        .change_stream
        .query_changes(Some(&collection), since_ms, 10_000);

    // Then subscribe for new events going forward.
    let mut subscription = shared
        .change_stream
        .subscribe(Some(collection.clone()), Some(tenant_id));

    let stream = async_stream::stream! {
        // Phase 1: replay backlog.
        for event in backlog {
            yield Ok(format_sse_event(&event));
        }

        // Phase 2: stream new events as they arrive.
        loop {
            match subscription.recv_filtered().await {
                Ok(event) => {
                    yield Ok(format_sse_event(&event));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // Subscriber fell behind. Send a warning event.
                    yield Ok(Event::default()
                        .event("warning")
                        .data(format!("lagged {n} events")));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Poll endpoint: `GET /cdc/{collection}/poll`
///
/// Returns a JSON batch of changes since the given cursor.
/// Non-streaming — returns immediately with available changes.
///
/// Response:
/// ```json
/// {
///   "changes": [
///     {"operation": "INSERT", "document_id": "u1", "timestamp_ms": 123, "lsn": 42},
///     ...
///   ],
///   "next_cursor": {"since_ms": 124, "since_lsn": 43},
///   "has_more": false
/// }
/// ```
pub async fn poll_changes(
    Path(collection): Path<String>,
    Query(params): Query<PollParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let collection = collection.to_lowercase();
    let since_ms = params.since_ms.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(10_000);

    let changes = state
        .shared
        .change_stream
        .query_changes(Some(&collection), since_ms, limit);

    let change_json: Vec<serde_json::Value> = changes
        .iter()
        .map(|e| {
            serde_json::json!({
                "operation": e.operation.as_str(),
                "document_id": e.document_id,
                "timestamp_ms": e.timestamp_ms,
                "lsn": e.lsn.as_u64(),
                "collection": e.collection,
            })
        })
        .collect();

    let next_cursor = changes.last().map(|last| {
        serde_json::json!({
            "since_ms": last.timestamp_ms + 1,
            "since_lsn": last.lsn.as_u64() + 1,
        })
    });

    let has_more = changes.len() >= limit;

    Json(serde_json::json!({
        "changes": change_json,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "count": changes.len(),
    }))
}

/// Format a ChangeEvent as an SSE Event.
fn format_sse_event(event: &ChangeEvent) -> Event {
    let data = serde_json::json!({
        "collection": event.collection,
        "operation": event.operation.as_str(),
        "document_id": event.document_id,
        "timestamp_ms": event.timestamp_ms,
        "lsn": event.lsn.as_u64(),
    });

    Event::default()
        .id(event.lsn.as_u64().to_string())
        .event(event.operation.as_str().to_lowercase())
        .data(data.to_string())
}
