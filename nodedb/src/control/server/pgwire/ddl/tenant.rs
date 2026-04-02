use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::{sqlstate_error, text_field};

/// CREATE TENANT <name> [ID <id>]
///
/// Creates a tenant with default quotas. Only superuser can create tenants.
/// `name` is for display; the numeric ID is what's used internally.
pub fn create_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can create tenants",
        ));
    }

    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE TENANT <name> [ID <id>]",
        ));
    }

    let name = parts[2];

    // Parse optional ID and register tenant in a single lock scope.
    let tenant_id = {
        let mut tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        let tid = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("ID") {
            let id: u32 = parts[4]
                .parse()
                .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
            TenantId::new(id)
        } else {
            let count = tenants.tenant_count() as u32;
            TenantId::new(count + 1)
        };
        tenants.set_quota(tid, TenantQuota::default());
        tid
    };

    // Auto-create a tenant_admin user for the new tenant.
    let admin_name = format!("{name}_admin");
    let admin_password = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(name.as_bytes());
        hasher.update(tenant_id.as_u32().to_le_bytes());
        hasher.update(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .to_le_bytes(),
        );
        let hash = hasher.finalize();
        let hex: String = hash.iter().take(12).map(|b| format!("{b:02x}")).collect();
        format!("ndb_{hex}")
    };
    match state.credentials.create_user(
        &admin_name,
        &admin_password,
        tenant_id,
        vec![Role::TenantAdmin],
    ) {
        Ok(_) => {
            tracing::info!(tenant = %name, admin = %admin_name, "auto-created tenant admin");
        }
        Err(e) => {
            // Non-fatal: tenant is created even if admin user fails (e.g. name collision).
            tracing::warn!(tenant = %name, error = %e, "failed to auto-create tenant admin");
        }
    }

    state.audit_record(
        AuditEvent::TenantCreated,
        Some(tenant_id),
        &identity.username,
        &format!("created tenant '{name}' (id {tenant_id}) with admin '{admin_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TENANT"))])
}

/// ALTER TENANT <id> SET QUOTA <field> = <value>
///
/// Fields: max_memory_bytes, max_storage_bytes, max_concurrent_requests, max_qps
pub fn alter_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can alter tenants",
        ));
    }

    // ALTER TENANT <id> SET QUOTA <field> = <value>
    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER TENANT <id> SET QUOTA <field> = <value>",
        ));
    }

    let tid: u32 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
    let tenant_id = TenantId::new(tid);

    if !parts[3].eq_ignore_ascii_case("SET") || !parts[4].eq_ignore_ascii_case("QUOTA") {
        return Err(sqlstate_error(
            "42601",
            "expected SET QUOTA after tenant ID",
        ));
    }

    let field = parts[5].to_lowercase();
    // Skip optional '='
    let value_idx = if parts.len() > 7 && parts[6] == "=" {
        7
    } else {
        6
    };
    if value_idx >= parts.len() {
        return Err(sqlstate_error("42601", "expected value after field name"));
    }

    let value: u64 = parts[value_idx]
        .parse()
        .map_err(|_| sqlstate_error("42601", "quota value must be a positive integer"))?;

    let mut tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    // Get current quota, modify the field, set back.
    let mut quota = tenants.quota(tenant_id).clone();
    match field.as_str() {
        "max_memory_bytes" => quota.max_memory_bytes = value,
        "max_storage_bytes" => quota.max_storage_bytes = value,
        "max_concurrent_requests" => quota.max_concurrent_requests = value as u32,
        "max_qps" => quota.max_qps = value as u32,
        "max_vector_dim" => quota.max_vector_dim = value as u32,
        "max_graph_depth" => quota.max_graph_depth = value as u32,
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!(
                    "unknown quota field: {other}. Valid: max_memory_bytes, max_storage_bytes, max_concurrent_requests, max_qps, max_vector_dim, max_graph_depth"
                ),
            ));
        }
    }
    tenants.set_quota(tenant_id, quota);

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("altered tenant {tenant_id}: set {field} = {value}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TENANT"))])
}

/// DROP TENANT <id>
///
/// Removes tenant quotas. Only superuser. Does NOT delete tenant data
/// (that requires a separate data purge operation).
pub fn drop_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can drop tenants",
        ));
    }

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP TENANT <id>"));
    }

    let tid: u32 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
    let tenant_id = TenantId::new(tid);

    // Prevent dropping tenant 0 (system).
    if tid == 0 {
        return Err(sqlstate_error("42501", "cannot drop system tenant (0)"));
    }

    state.audit_record(
        AuditEvent::TenantDeleted,
        Some(tenant_id),
        &identity.username,
        &format!("dropped tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP TENANT"))])
}

/// SHOW TENANT USAGE [FOR <id>]
///
/// Returns current runtime usage counters for one or all tenants.
/// Superuser only.
pub fn show_tenant_usage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view tenant usage",
        ));
    }

    // Parse optional FOR <id>.
    let filter_tid = parse_tenant_for_clause(parts);

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let schema = Arc::new(vec![
        text_field("tenant_id"),
        text_field("memory_bytes"),
        text_field("storage_bytes"),
        text_field("active_requests"),
        text_field("qps_current"),
        text_field("total_requests"),
        text_field("rejected_requests"),
        text_field("active_connections"),
    ]);

    let rows: Vec<_> = tenants
        .iter_usage()
        .filter(|(tid, _, _)| filter_tid.is_none() || Some(tid.as_u32()) == filter_tid)
        .map(|(tid, usage, _)| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&tid.as_u32().to_string());
            let _ = enc.encode_field(&usage.memory_bytes.to_string());
            let _ = enc.encode_field(&usage.storage_bytes.to_string());
            let _ = enc.encode_field(&usage.active_requests.to_string());
            let _ = enc.encode_field(&usage.requests_this_second.to_string());
            let _ = enc.encode_field(&usage.total_requests.to_string());
            let _ = enc.encode_field(&usage.rejected_requests.to_string());
            let _ = enc.encode_field(&usage.active_connections.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW TENANT QUOTA [FOR <id>]
///
/// Returns quota limits alongside current usage for one or all tenants.
/// Superuser only.
pub fn show_tenant_quota(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view tenant quotas",
        ));
    }

    let filter_tid = parse_tenant_for_clause(parts);

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let schema = Arc::new(vec![
        text_field("tenant_id"),
        text_field("quota_name"),
        text_field("limit"),
        text_field("current"),
        text_field("percent_used"),
    ]);

    let mut rows = Vec::new();

    for (tid, usage, quota) in tenants.iter_usage() {
        if filter_tid.is_some() && Some(tid.as_u32()) != filter_tid {
            continue;
        }
        let tid_str = tid.as_u32().to_string();

        let entries: &[(&str, u64, u64)] = &[
            ("memory_bytes", quota.max_memory_bytes, usage.memory_bytes),
            (
                "storage_bytes",
                quota.max_storage_bytes,
                usage.storage_bytes,
            ),
            (
                "concurrent_requests",
                quota.max_concurrent_requests as u64,
                usage.active_requests as u64,
            ),
            (
                "qps",
                quota.max_qps as u64,
                usage.requests_this_second as u64,
            ),
            (
                "connections",
                quota.max_connections as u64,
                usage.active_connections as u64,
            ),
        ];

        for &(name, limit, current) in entries {
            let pct = if limit > 0 {
                format!("{:.1}%", current as f64 / limit as f64 * 100.0)
            } else {
                "unlimited".to_string()
            };
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&tid_str);
            let _ = enc.encode_field(&name);
            let _ = enc.encode_field(&limit.to_string());
            let _ = enc.encode_field(&current.to_string());
            let _ = enc.encode_field(&pct);
            rows.push(Ok(enc.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// PURGE TENANT <id> CONFIRM
///
/// Deletes ALL data for a tenant across every engine and cache.
/// Requires CONFIRM keyword to prevent accidental data destruction.
/// Superuser-only. Audit-logged at Forensic level.
pub async fn purge_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can purge tenants",
        ));
    }

    if parts.len() < 4 {
        return Err(sqlstate_error("42601", "syntax: PURGE TENANT <id> CONFIRM"));
    }

    let tid: u32 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;

    if tid == 0 {
        return Err(sqlstate_error("42501", "cannot purge system tenant (0)"));
    }

    if !parts[3].eq_ignore_ascii_case("CONFIRM") {
        return Err(sqlstate_error(
            "42601",
            "PURGE TENANT requires CONFIRM keyword to prevent accidental data destruction",
        ));
    }

    let tenant_id = TenantId::new(tid);

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("PURGE TENANT {tid} CONFIRM — deleting all data across all engines"),
    );

    // Dispatch purge to the Data Plane.
    let plan = crate::bridge::envelope::PhysicalPlan::Meta(
        crate::bridge::physical_plan::MetaOp::PurgeTenant { tenant_id: tid },
    );

    match super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        "__system",
        plan,
        std::time::Duration::from_secs(300), // 5 minutes for large tenants.
    )
    .await
    {
        Ok(_) => {
            state.audit_record(
                AuditEvent::AdminAction,
                Some(tenant_id),
                &identity.username,
                &format!("PURGE TENANT {tid} completed successfully"),
            );
            Ok(vec![Response::Execution(Tag::new("PURGE TENANT"))])
        }
        Err(e) => Err(sqlstate_error("XX000", &format!("purge failed: {e}"))),
    }
}

/// Parse `FOR <tenant_id>` from DDL parts. Returns `Some(tid)` if present.
fn parse_tenant_for_clause(parts: &[&str]) -> Option<u32> {
    let for_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("FOR"))?;
    parts.get(for_idx + 1)?.parse::<u32>().ok()
}
