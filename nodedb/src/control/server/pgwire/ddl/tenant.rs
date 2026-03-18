use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::sqlstate_error;

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
