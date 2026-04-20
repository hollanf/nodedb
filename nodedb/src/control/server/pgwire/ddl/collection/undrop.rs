//! `UNDROP COLLECTION <name>` — restore a soft-deleted collection.
//!
//! Valid only while the collection's retention window has not elapsed
//! (the redb row still exists with `is_active = false`). Flips
//! `is_active` back to `true` via a fresh `CatalogEntry::PutCollection`,
//! so the applier and every downstream cache observe the restore
//! through the normal catalog-change stream.
//!
//! Authorization matches `ALTER COLLECTION OWNER TO`: preserved owner,
//! superuser, or tenant_admin. If the preserved-owner user no longer
//! exists, only superuser / tenant_admin may undrop; the restore is
//! audit-logged with an `owner_user_missing` marker.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

pub fn undrop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: UNDROP COLLECTION <name>"));
    }

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error(
            "XX000",
            "UNDROP COLLECTION requires a persistent system catalog",
        ));
    };

    // Look up the soft-deleted record. Three distinct failures:
    //   - row absent: retention already expired or never existed.
    //   - row present + active: nothing to undrop.
    //   - row present + inactive: candidate for restore.
    let mut stored = match catalog.get_collection(tenant_id.as_u32(), name) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return Err(sqlstate_error(
                "42P01",
                &format!(
                    "collection '{name}' not found (retention window elapsed or never existed)"
                ),
            ));
        }
        Err(e) => return Err(sqlstate_error("XX000", &e.to_string())),
    };
    if stored.is_active {
        return Err(sqlstate_error(
            "42P07",
            &format!("collection '{name}' is already active"),
        ));
    }

    // Authorization: preserved owner OR admin.
    let preserved_owner = state.permissions.get_owner("collection", tenant_id, name);
    let is_preserved_owner = preserved_owner.as_deref() == Some(&identity.username);
    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);

    if !is_preserved_owner && !is_admin {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only the preserved owner, superuser, or tenant_admin may UNDROP",
        ));
    }

    // If the preserved-owner user no longer exists, only admin may restore.
    let owner_user_missing = preserved_owner
        .as_deref()
        .is_some_and(|u| state.credentials.get_user(u).is_none());
    if owner_user_missing && !is_admin {
        return Err(sqlstate_error(
            "42501",
            "preserved-owner user no longer exists — only superuser or tenant_admin may UNDROP",
        ));
    }

    // Propose the restore as a PutCollection through the metadata raft
    // group. Fresh entry carries `is_active = true` and the preserved
    // owner (already present on `stored`).
    stored.is_active = true;
    let entry =
        crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0 {
        // Single-node fallback: write directly.
        catalog
            .put_collection(&stored)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    let action = if owner_user_missing {
        format!("undropped collection '{name}' (owner_user_missing=true)")
    } else {
        format!("undropped collection '{name}'")
    };
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &action,
    );

    Ok(vec![Response::Execution(Tag::new("UNDROP COLLECTION"))])
}
