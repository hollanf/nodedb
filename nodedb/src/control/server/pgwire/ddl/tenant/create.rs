//! `CREATE TENANT <name> [ID <id>]` handler. Migrated to
//! `CatalogEntry::PutTenant` in phase 1k.6.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredTenant;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::types::sqlstate_error;

/// `CREATE TENANT <name> [ID <id>]`
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

    // Pick the tenant id under a short lock scope; do NOT mutate the
    // store yet — the post_apply side effect on every node seeds the
    // default quota when `PutTenant` commits.
    let tenant_id = {
        let tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("ID") {
            let id: u64 = parts[4]
                .parse()
                .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;
            TenantId::new(id)
        } else {
            let count = tenants.tenant_count() as u64;
            TenantId::new(count + 1)
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let stored = StoredTenant {
        tenant_id: tenant_id.as_u64(),
        name: name.to_string(),
        created_at: now,
        is_active: true,
    };

    let entry = CatalogEntry::PutTenant(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        // Single-node fallback: write redb + seed in-memory quota
        // ourselves since post_apply only runs on the raft path.
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_tenant(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        let mut tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        if !tenants.has_quota(tenant_id) {
            tenants.set_quota(tenant_id, TenantQuota::default());
        }
    }

    // Auto-create a tenant_admin user for the new tenant.
    let admin_name = format!("{name}_admin");
    let admin_password = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(name.as_bytes());
        hasher.update(tenant_id.as_u64().to_le_bytes());
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
