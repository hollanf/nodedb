//! Health check endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

use super::super::auth::AppState;

/// GET /health — liveness check.
pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let cluster_version = {
        // Recover from poisoned mutex: health check must not fail due to prior panic.
        let vs = state
            .shared
            .cluster_version_state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        json!({
            "nodes": vs.node_count,
            "min_version": vs.min_version,
            "max_version": vs.max_version,
            "mixed_version": vs.is_mixed_version(),
            "compat_mode": crate::control::rolling_upgrade::should_compat_mode(&vs),
        })
    };
    let body = json!({
        "status": "ok",
        "node_id": state.shared.node_id,
        "cluster_version": cluster_version,
    });
    (StatusCode::OK, axum::Json(body))
}

/// GET /health/ready — readiness check (WAL recovered, cores initialized).
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let wal_ready = state.shared.wal.next_lsn().as_u64() > 0;
    let status = if wal_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = json!({
        "status": if wal_ready { "ready" } else { "not_ready" },
        "wal_lsn": state.shared.wal.next_lsn().as_u64(),
        "node_id": state.shared.node_id,
    });
    (status, axum::Json(body))
}
