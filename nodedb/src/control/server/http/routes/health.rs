//! Health check endpoints.
//!
//! | Endpoint          | Method | Purpose                     | k8s probe     |
//! |-------------------|--------|-----------------------------|---------------|
//! | `/health/live`    | GET    | Process alive (always 200)  | liveness      |
//! | `/healthz`        | GET    | Ready to serve traffic      | readiness     |
//! | `/health`         | GET    | Liveness with cluster info  | —             |
//! | `/health/ready`   | GET    | WAL recovered               | readiness alt |
//! | `/health/drain`   | POST   | Trigger graceful drain      | preStop hook  |

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

use super::super::auth::AppState;

/// GET /health/live — unconditional liveness probe.
///
/// Always returns 200. If this endpoint fails to respond, the
/// process is dead and should be restarted. No internal state is
/// checked — the mere ability to respond proves the event loop and
/// HTTP listener are alive.
pub async fn live() -> impl IntoResponse {
    (StatusCode::OK, axum::Json(json!({ "status": "alive" })))
}

/// GET /healthz — k8s-style readiness probe.
///
/// Returns `200 OK` when the node has reached `GatewayEnable`, is
/// serving traffic, and is NOT draining/decommissioned. Returns
/// `503 Service Unavailable` during startup, after startup failure,
/// or when the node is being decommissioned.
pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    // Check decommission state via the cluster observer (if present).
    if let Some(obs) = state.shared.cluster_observer.get() {
        let snap = obs.snapshot();
        let label = snap.lifecycle_label();
        if label == "draining" || label == "decommissioned" || label == "failed" {
            let body = json!({
                "status": "draining",
                "lifecycle": label,
                "node_id": state.shared.node_id,
            });
            return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
        }
    }
    let health = crate::control::startup::health::observe(&state.shared.startup);
    let (status, body) = crate::control::startup::health::to_http_response(&health);
    (status, axum::Json(body))
}

/// GET /health — liveness check.
pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    // Derive both the node count and version view from the live
    // cluster topology in one read. Single-node mode reports 1
    // via the view's fallback.
    let view = state.shared.cluster_version_view();
    let nodes = if view.node_count > 0 {
        view.node_count
    } else {
        1
    };
    let cluster_version = json!({
        "nodes": nodes,
        "min_version": view.min_version,
        "max_version": view.max_version,
        "mixed_version": view.is_mixed_version(),
        "compat_mode": crate::control::rolling_upgrade::should_compat_mode(&view),
    });
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

/// POST /health/drain — trigger graceful connection drain.
///
/// Signals the canonical `ShutdownWatch` so every background loop
/// begins its cooperative exit. Subsequent `/healthz` calls return
/// 503, which causes the k8s readiness probe to fail and the
/// service mesh to stop routing new connections to this node.
///
/// Designed for use as a k8s `preStop` hook:
///
/// ```yaml
/// lifecycle:
///   preStop:
///     httpGet:
///       path: /health/drain
///       port: http
/// ```
pub async fn drain(State(state): State<AppState>) -> impl IntoResponse {
    tracing::info!(node_id = state.shared.node_id, "drain requested via HTTP");
    state.shared.shutdown.signal();
    (
        StatusCode::OK,
        axum::Json(json!({
            "status": "draining",
            "node_id": state.shared.node_id,
        })),
    )
}
