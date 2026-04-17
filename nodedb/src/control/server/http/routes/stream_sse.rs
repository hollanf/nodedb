//! SSE streaming endpoint for change stream consumption.
//!
//! `GET /v1/streams/{stream}/events?group={group}&partition=3`
//!
//! Pushes events as Server-Sent Events in real-time. On each poll cycle,
//! reads new events from the buffer since the consumer group's committed
//! offset. The consumer should COMMIT OFFSET via SQL to advance the cursor.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use serde::Deserialize;
use sonic_rs;

use super::super::auth::{ApiError, AppState, ResolvedIdentity};
use crate::control::state::SharedState;
use crate::event::cdc::consume::{ConsumeError, ConsumeParams, consume_stream};

/// Query parameters.
#[derive(Deserialize, Default)]
pub struct SseParams {
    /// Consumer group name (required).
    pub group: Option<String>,
    /// Optional: stream from a specific partition only.
    pub partition: Option<u16>,
    /// Detected and rejected — callers must not supply `tenant_id` as a
    /// query parameter. Tenant is always sourced from the bearer token.
    pub tenant_id: Option<u32>,
}

/// Drop guard that deregisters a consumer from partition assignment
/// on ALL exit paths (normal close, error, panic, task cancellation).
struct ConsumerGuard {
    shared: Arc<SharedState>,
    tenant_id: u32,
    stream_name: String,
    group: String,
    consumer_id: String,
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        self.shared.consumer_assignments.leave(
            self.tenant_id,
            &self.stream_name,
            &self.group,
            &self.consumer_id,
        );
    }
}

/// `GET /v1/streams/{stream}/events`
pub async fn stream_events(
    identity: ResolvedIdentity,
    Path(stream_name): Path<String>,
    Query(params): Query<SseParams>,
    State(state): State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Reject any attempt to override the caller's tenant via query string.
    if params.tenant_id.is_some() {
        return Err(ApiError::Forbidden(
            "tenant_id must not be supplied as a query parameter; \
             tenant is determined from the bearer token"
                .into(),
        ));
    }

    let group = params.group.unwrap_or_default().to_lowercase();
    let tenant_id = identity.tenant_id().as_u32();
    let stream_name = stream_name.to_lowercase();
    let partition = params.partition;

    let stream = async_stream::stream! {
        if group.is_empty() {
            yield Ok(Event::default()
                .event("error")
                .data("missing 'group' query parameter"));
            return;
        }

        // Generate a unique consumer ID using process ID + atomic counter.
        let consumer_id = unique_consumer_id();

        // Register consumer and create a Drop guard for guaranteed cleanup.
        state.shared.consumer_assignments.join(
            tenant_id,
            &stream_name,
            &group,
            &consumer_id,
        );
        let _guard = ConsumerGuard {
            shared: Arc::clone(&state.shared),
            tenant_id,
            stream_name: stream_name.clone(),
            group: group.clone(),
            consumer_id,
        };

        loop {
            let consume_params = ConsumeParams {
                tenant_id,
                stream_name: &stream_name,
                group_name: &group,
                partition,
                limit: 100,
            };

            let result = match consume_stream(&state.shared, &consume_params) {
                Ok(r) => r,
                Err(ConsumeError::RemotePartition { leader_node, .. }) => {
                    match crate::event::cdc::consume::consume_remote(
                        &state.shared,
                        &consume_params,
                        leader_node,
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            yield Ok(Event::default()
                                .event("error")
                                .data(e.to_string()));
                            return;
                        }
                    }
                }
                Err(ConsumeError::BufferEmpty(_)) => {
                    // No events yet — wait and retry.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => {
                    yield Ok(Event::default()
                        .event("error")
                        .data(e.to_string()));
                    return; // _guard dropped here → leave() called.
                }
            };
            if !result.events.is_empty() {
                for event in &result.events {
                    let json = sonic_rs::to_string(event).unwrap_or_default();
                    yield Ok(Event::default()
                        .event("change")
                        .id(format!("{}:{}", event.partition, event.lsn))
                        .data(json));
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // _guard dropped here on any exit → leave() called.
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Generate a unique consumer ID using process ID + monotonic counter.
fn unique_consumer_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sse-{}-{seq}", std::process::id())
}
