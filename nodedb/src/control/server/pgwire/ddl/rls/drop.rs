//! `DROP RLS POLICY` handler. Migrated to `CatalogEntry::DeleteRlsPolicy`
//! in phase 1k.6.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// `DROP RLS POLICY <name> ON <collection> [TENANT <id>]`
pub fn drop_rls_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser && !identity.roles.contains(&Role::TenantAdmin) {
        return Err(sqlstate_error("42501", "permission denied"));
    }

    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP RLS POLICY <name> ON <collection>",
        ));
    }

    let name = parts[3];
    let collection = parts[5];

    let tenant_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| identity.tenant_id.as_u64());

    if !state.rls.policy_exists(tenant_id, collection, name) {
        return Err(sqlstate_error(
            "42704",
            &format!("RLS policy '{name}' not found on '{collection}'"),
        ));
    }

    let entry = CatalogEntry::DeleteRlsPolicy {
        tenant_id,
        collection: collection.to_string(),
        name: name.to_string(),
    };
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .delete_rls_policy(tenant_id, collection, name)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state
            .rls
            .install_replicated_drop_policy(tenant_id, collection, name);
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("RLS policy '{name}' dropped from '{collection}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP RLS POLICY"))])
}
