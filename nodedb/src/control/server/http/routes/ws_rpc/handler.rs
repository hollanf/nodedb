//! WebSocket RPC upgrade handler and connection lifecycle.
//!
//! Accepts WebSocket connections at `/ws`. Clients send JSON requests
//! and receive JSON responses. Supports SQL query execution, live query
//! subscriptions, and ping/pong.
//!
//! Protocol:
//! ```json
//! // Request
//! {"id": 1, "method": "query", "params": {"sql": "SELECT * FROM users"}}
//! {"id": 2, "method": "ping"}
//! {"id": 3, "method": "live", "params": {"sql": "LIVE SELECT * FROM orders"}}
//! {"id": 4, "method": "auth", "params": {"session_id": "abc", "last_lsn": 42}}
//!
//! // Response
//! {"id": 1, "result": [...]}
//! {"id": 2, "result": "pong"}
//! ```

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use tracing::debug;

use crate::control::change_stream::LiveSubscriptionSet;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::process_message::process_message;

/// WebSocket upgrade handler.
///
/// Auth is resolved before the upgrade — if identity resolution fails the
/// HTTP handshake is rejected with 401/403 before any WebSocket state is
/// created. This is the only correct place to enforce auth: a post-upgrade
/// "reject the first message" approach still pins a tenant inside the handler.
pub async fn ws_handler(
    identity: ResolvedIdentity,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let tenant_id = identity.tenant_id();
    let trace_id = crate::control::trace_context::extract_from_headers(&headers);
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state, tenant_id, trace_id))
}

/// Handle a single WebSocket connection.
async fn handle_ws_connection(
    socket: WebSocket,
    state: AppState,
    tenant_id: TenantId,
    trace_id: nodedb_types::TraceId,
) {
    let (mut sender, mut receiver) = socket.split();
    let shared = Arc::clone(&state.shared);

    // Bounded channel for live notifications → WS sender.
    // 256 messages provides ~10s of buffer at 25 events/sec.
    let (live_tx, mut live_rx) = tokio::sync::mpsc::channel::<String>(256);

    let send_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(msg) = live_rx.recv() => {
                    if sender.send(Message::Text(msg.into())).await.is_err() {
                        debug!("WebSocket send failed; closing connection");
                        break;
                    }
                }
                else => break,
            }
        }
    });

    // Session ID is set only after successful auth inside process_message.
    let mut authenticated_session_id: Option<String> = None;

    // Connection-scoped live-subscription tasks. Dropping this set on
    // connection exit aborts every forwarder, which drops each captured
    // `Subscription` so `active_subscriptions` returns to 0.
    let mut live_set = LiveSubscriptionSet::new();

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                let (response, auth_session) = process_message(
                    &shared,
                    &state.query_ctx,
                    tenant_id,
                    trace_id,
                    &text,
                    &live_tx,
                    &mut live_set,
                )
                .await;

                // Record session_id only after process_message confirms auth.
                if let Some(sid) = auth_session {
                    authenticated_session_id = Some(sid);
                }

                if let Err(e) = live_tx.send(response).await {
                    debug!("response channel closed: {e}; dropping connection");
                    break;
                }
            }
            Message::Close(_) => break,
            Message::Ping(_) => {
                if let Err(e) = live_tx
                    .send(serde_json::json!({"pong": true}).to_string())
                    .await
                {
                    debug!("pong send failed: {e}; dropping connection");
                    break;
                }
            }
            _ => {}
        }
    }

    // Save session's last-seen LSN for reconnection replay.
    if let Some(sid) = &authenticated_session_id {
        save_ws_session(&shared, sid);
    }

    // Drop the live-subscription set BEFORE closing the channel. Aborting
    // each forwarder drops its `Subscription`, whose `Drop` decrements
    // `active_subscriptions`, so leaked counters can't outlive the socket.
    drop(live_set);

    drop(live_tx);
    let _ = send_handle.await;
    debug!("WebSocket RPC connection closed");
}

/// Save a WS session's last-seen LSN with bounded LRU eviction.
pub fn save_ws_session(shared: &SharedState, session_id: &str) {
    let current_lsn = shared.change_stream.last_lsn();
    let mut sessions = shared
        .ws_sessions
        .write()
        .unwrap_or_else(|p| p.into_inner());

    // Evict oldest sessions (by LSN) if at capacity.
    while sessions.len() >= shared.tuning.network.max_ws_sessions {
        if let Some(oldest_key) = sessions
            .iter()
            .min_by_key(|(_, lsn)| **lsn)
            .map(|(k, _)| k.clone())
        {
            sessions.remove(&oldest_key);
        } else {
            break;
        }
    }

    sessions.insert(session_id.to_string(), current_lsn);
    debug!(
        session_id,
        last_lsn = current_lsn,
        "WS session saved for reconnect"
    );
}
