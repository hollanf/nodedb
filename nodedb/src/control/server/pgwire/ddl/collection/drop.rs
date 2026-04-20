//! DROP COLLECTION DDL.
//!
//! Supported forms (tokens are case-insensitive):
//!
//! - `DROP COLLECTION <n>` — soft-delete (flip `is_active`).
//! - `DROP COLLECTION <n> PURGE` — hard-delete via
//!   `CatalogEntry::PurgeCollection`. Requires admin.
//! - `DROP COLLECTION <n> CASCADE` / `... PURGE CASCADE` /
//!   `... CASCADE FORCE` — accept the keyword; the recursive dependent
//!   enumeration lives in the apply path Until the enumerator lands,
//!   handlers reject with a clear "dependents must be dropped
//!   individually" message rather than silently succeeding.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// Flags parsed off the `DROP COLLECTION <name> [IF EXISTS] [PURGE] [CASCADE [FORCE]]`
/// trailing tokens.
#[derive(Debug, Default, Clone, Copy)]
struct DropFlags {
    purge: bool,
    cascade: bool,
    cascade_force: bool,
}

fn parse_drop_flags(parts: &[&str]) -> DropFlags {
    let mut f = DropFlags::default();
    let upper: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();
    for (i, tok) in upper.iter().enumerate() {
        match tok.as_str() {
            "PURGE" => f.purge = true,
            "CASCADE" => {
                f.cascade = true;
                if upper.get(i + 1).map(String::as_str) == Some("FORCE") {
                    f.cascade_force = true;
                }
            }
            "FORCE" if i > 0 && upper[i - 1] == "CASCADE" => {
                // already handled above
            }
            _ => {}
        }
    }
    f
}

/// DROP COLLECTION <name> [PURGE] [CASCADE [FORCE]]
///
/// Marks collection as inactive (or hard-deletes on PURGE).
/// Requires owner or admin.
pub fn drop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP COLLECTION <name>"));
    }

    let flags = parse_drop_flags(parts);

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // CASCADE is accepted by the parser, but the dependent-enumeration
    // pass lives in the apply layer and is not yet landed. Rejecting
    // here with a precise message beats silently "succeeding" while
    // orphaning triggers/RLS/MVs.
    if flags.cascade {
        return Err(sqlstate_error(
            "0A000",
            "DROP COLLECTION ... CASCADE is not yet supported — drop dependents (triggers, \
             RLS policies, materialized views, change streams, schedules) individually first",
        ));
    }
    let _ = flags.cascade_force; // same gate

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("collection", tenant_id, name)
        .as_deref()
        == Some(&identity.username);

    let is_admin = identity.is_superuser
        || identity.has_role(&crate::control::security::identity::Role::TenantAdmin);

    if !is_owner && !is_admin {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only owner, superuser, or tenant_admin can drop collections",
        ));
    }

    // PURGE requires admin — it bypasses the retention safety net,
    // which an owner alone should not be able to invoke.
    if flags.purge && !is_admin {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser or tenant_admin may DROP COLLECTION ... PURGE",
        ));
    }

    // Verify the collection exists (read from the legacy redb — the
    // replicated cache observes the same entries via the applier).
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), name) {
            Ok(Some(coll)) if coll.is_active => {}
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{name}' does not exist"),
                ));
            }
        }
    }

    // Propose the drop through the metadata raft group. The applier
    // on every node decodes the entry, performs the appropriate
    // mutation, and (for PurgeCollection) triggers the async
    // storage-reclaim dispatch on every node symmetrically.
    let entry = if flags.purge {
        crate::control::catalog_entry::CatalogEntry::PurgeCollection {
            tenant_id: tenant_id.as_u32(),
            name: name.to_string(),
        }
    } else {
        crate::control::catalog_entry::CatalogEntry::DeactivateCollection {
            tenant_id: tenant_id.as_u32(),
            name: name.to_string(),
        }
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
    {
        // Single-node / no-cluster fallback: apply the catalog mutation
        // directly, matching what the applier would have done on a
        // clustered deployment.
        if flags.purge {
            catalog
                .delete_collection(tenant_id.as_u32(), name)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        } else if let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), name) {
            coll.is_active = false;
            catalog
                .put_collection(&coll)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
    }

    // Cascade: drop implicit sequences (SERIAL/BIGSERIAL fields create {coll}_{field}_seq).
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(seqs) = catalog.load_sequences_for_tenant(tenant_id.as_u32())
    {
        let prefix = format!("{name}_");
        let suffix = "_seq";
        for seq in &seqs {
            if seq.name.starts_with(&prefix) && seq.name.ends_with(suffix) {
                catalog
                    .delete_sequence(tenant_id.as_u32(), &seq.name)
                    .map_err(|e| {
                        sqlstate_error(
                            "XX000",
                            &format!("failed to drop sequence '{}': {e}", seq.name),
                        )
                    })?;
                // Best-effort: registry removal is non-critical since catalog
                // is the source of truth and the sequence won't be reloaded.
                let _ = state
                    .sequence_registry
                    .remove(tenant_id.as_u32(), &seq.name);
            }
        }
    }

    let action = if flags.purge {
        format!("purged collection '{name}'")
    } else {
        format!("dropped collection '{name}'")
    };
    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &action,
    );

    Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))])
}
