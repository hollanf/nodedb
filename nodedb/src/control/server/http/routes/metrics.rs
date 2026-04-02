//! Prometheus-compatible metrics endpoint.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use super::super::auth::{ApiError, AppState, resolve_identity};

/// GET /metrics — Prometheus-format metrics.
///
/// Requires monitor role or superuser.
pub async fn metrics(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;

    if !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::Monitor)
    {
        return Err(ApiError::Forbidden(
            "monitor or superuser role required".into(),
        ));
    }

    let mut output = String::with_capacity(4096);

    // WAL metrics.
    let wal_lsn = state.shared.wal.next_lsn().as_u64();
    output.push_str("# HELP nodedb_wal_next_lsn Next WAL log sequence number.\n");
    output.push_str("# TYPE nodedb_wal_next_lsn gauge\n");
    output.push_str(&format!("nodedb_wal_next_lsn {wal_lsn}\n\n"));

    // Node ID.
    output.push_str("# HELP nodedb_node_id This node's cluster ID.\n");
    output.push_str("# TYPE nodedb_node_id gauge\n");
    output.push_str(&format!("nodedb_node_id {}\n\n", state.shared.node_id));

    // Tenant metrics — usage and quota limits.
    if let Ok(tenants) = state.shared.tenants.lock() {
        output.push_str("# HELP nodedb_tenant_active_requests Current in-flight requests.\n");
        output.push_str("# TYPE nodedb_tenant_active_requests gauge\n");
        output.push_str("# HELP nodedb_tenant_total_requests Total requests served.\n");
        output.push_str("# TYPE nodedb_tenant_total_requests counter\n");
        output
            .push_str("# HELP nodedb_tenant_rejected_requests Total requests rejected by quota.\n");
        output.push_str("# TYPE nodedb_tenant_rejected_requests counter\n");
        output.push_str("# HELP nodedb_tenant_active_connections Current active connections.\n");
        output.push_str("# TYPE nodedb_tenant_active_connections gauge\n");
        output.push_str(
            "# HELP nodedb_tenant_memory_used_bytes Current memory consumption in bytes.\n",
        );
        output.push_str("# TYPE nodedb_tenant_memory_used_bytes gauge\n");
        output.push_str("# HELP nodedb_tenant_memory_limit_bytes Memory quota limit in bytes.\n");
        output.push_str("# TYPE nodedb_tenant_memory_limit_bytes gauge\n");
        output.push_str(
            "# HELP nodedb_tenant_storage_used_bytes Current storage consumption in bytes.\n",
        );
        output.push_str("# TYPE nodedb_tenant_storage_used_bytes gauge\n");
        output.push_str("# HELP nodedb_tenant_storage_limit_bytes Storage quota limit in bytes.\n");
        output.push_str("# TYPE nodedb_tenant_storage_limit_bytes gauge\n");
        output.push_str("# HELP nodedb_tenant_qps_current Requests in current second window.\n");
        output.push_str("# TYPE nodedb_tenant_qps_current gauge\n");
        output.push_str("# HELP nodedb_tenant_qps_limit Maximum queries per second.\n");
        output.push_str("# TYPE nodedb_tenant_qps_limit gauge\n");

        for (tid, usage, quota) in tenants.iter_usage() {
            let t = tid.as_u32();
            output.push_str(&format!(
                "nodedb_tenant_active_requests{{tenant_id=\"{t}\"}} {}\n",
                usage.active_requests
            ));
            output.push_str(&format!(
                "nodedb_tenant_total_requests{{tenant_id=\"{t}\"}} {}\n",
                usage.total_requests
            ));
            output.push_str(&format!(
                "nodedb_tenant_rejected_requests{{tenant_id=\"{t}\"}} {}\n",
                usage.rejected_requests
            ));
            output.push_str(&format!(
                "nodedb_tenant_active_connections{{tenant_id=\"{t}\"}} {}\n",
                usage.active_connections
            ));
            output.push_str(&format!(
                "nodedb_tenant_memory_used_bytes{{tenant_id=\"{t}\"}} {}\n",
                usage.memory_bytes
            ));
            output.push_str(&format!(
                "nodedb_tenant_memory_limit_bytes{{tenant_id=\"{t}\"}} {}\n",
                quota.max_memory_bytes
            ));
            output.push_str(&format!(
                "nodedb_tenant_storage_used_bytes{{tenant_id=\"{t}\"}} {}\n",
                usage.storage_bytes
            ));
            output.push_str(&format!(
                "nodedb_tenant_storage_limit_bytes{{tenant_id=\"{t}\"}} {}\n",
                quota.max_storage_bytes
            ));
            output.push_str(&format!(
                "nodedb_tenant_qps_current{{tenant_id=\"{t}\"}} {}\n",
                usage.requests_this_second
            ));
            output.push_str(&format!(
                "nodedb_tenant_qps_limit{{tenant_id=\"{t}\"}} {}\n",
                quota.max_qps
            ));
        }
        output.push('\n');
    }

    // Audit metrics.
    if let Ok(audit) = state.shared.audit.lock() {
        output.push_str("# HELP nodedb_audit_total_entries Total audit entries ever recorded.\n");
        output.push_str("# TYPE nodedb_audit_total_entries counter\n");
        output.push_str(&format!(
            "nodedb_audit_total_entries {}\n\n",
            audit.total_recorded()
        ));
    }

    // Credential metrics.
    let user_count = state.shared.credentials.list_user_details().len();
    output.push_str("# HELP nodedb_users_active Number of active users.\n");
    output.push_str("# TYPE nodedb_users_active gauge\n");
    output.push_str(&format!("nodedb_users_active {user_count}\n\n"));

    // SystemMetrics (if available): contention, subscriptions, WAL fsync, etc.
    if let Some(ref sys_metrics) = state.shared.system_metrics {
        output.push_str(&sys_metrics.to_prometheus());
    }

    // Auth observability: method-specific counters, duration histograms, anomaly detection.
    output.push_str(&state.shared.auth_metrics.to_prometheus());

    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        output,
    ))
}
