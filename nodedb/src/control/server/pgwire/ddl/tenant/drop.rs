//! `DROP TENANT <id>` handler. Migrated to `CatalogEntry::DeleteTenant`
//! in phase 1k.6.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::types::sqlstate_error;

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

    if tid == 0 {
        return Err(sqlstate_error("42501", "cannot drop system tenant (0)"));
    }

    let entry = CatalogEntry::DeleteTenant { tenant_id: tid };
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .delete_tenant(tid)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        let mut tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        tenants.remove_quota(tenant_id);
    }

    state.audit_record(
        AuditEvent::TenantDeleted,
        Some(tenant_id),
        &identity.username,
        &format!("dropped tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP TENANT"))])
}
