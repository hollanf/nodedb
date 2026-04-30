//! `GRANT/REVOKE <perm> ON <object> TO/FROM <grantee>` handlers.
//!
//! Migrated to `CatalogEntry::{PutPermission, DeletePermission}`
//! in phase 1l.6.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::permission::{format_permission, function_target, parse_permission};
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

fn propose_grant(
    state: &SharedState,
    target: &str,
    grantee: &str,
    perm: Permission,
    granted_by: &str,
) -> PgWireResult<()> {
    let stored = state
        .permissions
        .prepare_permission(target, grantee, perm, granted_by);
    let entry = CatalogEntry::PutPermission(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_permission(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state.permissions.install_replicated_permission(&stored);
    }
    Ok(())
}

fn propose_revoke(
    state: &SharedState,
    target: &str,
    grantee: &str,
    perm: Permission,
) -> PgWireResult<()> {
    let perm_str = format_permission(perm);
    let entry = CatalogEntry::DeletePermission {
        target: target.to_string(),
        grantee: grantee.to_string(),
        permission: perm_str.clone(),
    };
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .delete_permission(target, grantee, &perm_str)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state
            .permissions
            .install_replicated_revoke(target, grantee, &perm_str);
    }
    Ok(())
}

/// `GRANT <perm> ON <collection|FUNCTION name> TO <grantee>`
///
/// Called with typed fields from the AST router.
pub fn grant_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    perm_str: &str,
    target_type: &str,
    target_name: &str,
    grantee: &str,
) -> PgWireResult<Vec<Response>> {
    let (target, object_desc) = if target_type.eq_ignore_ascii_case("FUNCTION") {
        let func_name = target_name.to_lowercase();
        let t = function_target(identity.tenant_id, &func_name);
        (t, format!("function '{func_name}'"))
    } else {
        let t = format!("collection:{}:{target_name}", identity.tenant_id.as_u64());
        (t, format!("collection '{target_name}'"))
    };

    require_admin(identity, "grant permissions")?;

    let perms = if perm_str.eq_ignore_ascii_case("ALL") {
        vec![
            Permission::Read,
            Permission::Write,
            Permission::Create,
            Permission::Drop,
            Permission::Alter,
        ]
    } else {
        let perm = parse_permission(perm_str)
            .ok_or_else(|| sqlstate_error("42601", &format!("unknown permission: {perm_str}")))?;
        vec![perm]
    };

    for perm in &perms {
        propose_grant(state, &target, grantee, *perm, &identity.username)?;
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted {perm_str} on {object_desc} to '{grantee}'"),
    );

    Ok(vec![Response::Execution(Tag::new("GRANT"))])
}

/// `REVOKE <perm> ON <collection|FUNCTION name> FROM <grantee>`
///
/// Called with typed fields from the AST router.
pub fn revoke_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    perm_str: &str,
    target_type: &str,
    target_name: &str,
    grantee: &str,
) -> PgWireResult<Vec<Response>> {
    let (target, object_desc) = if target_type.eq_ignore_ascii_case("FUNCTION") {
        let func_name = target_name.to_lowercase();
        let t = function_target(identity.tenant_id, &func_name);
        (t, format!("function '{func_name}'"))
    } else {
        let t = format!("collection:{}:{target_name}", identity.tenant_id.as_u64());
        (t, format!("collection '{target_name}'"))
    };

    require_admin(identity, "revoke permissions")?;

    let perm = parse_permission(perm_str)
        .ok_or_else(|| sqlstate_error("42601", &format!("unknown permission: {perm_str}")))?;

    propose_revoke(state, &target, grantee, perm)?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked {perm_str} on {object_desc} from '{grantee}'"),
    );

    Ok(vec![Response::Execution(Tag::new("REVOKE"))])
}
