//! `CREATE RLS POLICY` handler — proposes
//! `CatalogEntry::PutRlsPolicy` through the metadata raft group so
//! every node's applier installs the policy into both
//! `SystemCatalog` redb and the in-memory `RlsPolicyStore`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredRlsPolicy;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::security::rls::RlsPolicy;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;
use super::parse::parse_create_rls_policy;

/// `CREATE RLS POLICY <name> ON <collection> FOR <read|write|all>
///     USING (<predicate>) [RESTRICTIVE] [TENANT <id>] [ON DENY ...]`
pub fn create_rls_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser && !identity.roles.contains(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser or tenant_admin",
        ));
    }

    let parsed = parse_create_rls_policy(parts, identity.tenant_id.as_u32())?;

    // Pre-check duplicate so the proposing node fails fast with a
    // clean SQLSTATE instead of going through raft only to be a
    // silent overwrite.
    if state
        .rls
        .policy_exists(parsed.tenant_id, &parsed.collection, &parsed.name)
    {
        return Err(sqlstate_error(
            "42710",
            &format!(
                "RLS policy '{}' already exists on '{}'",
                parsed.name, parsed.collection
            ),
        ));
    }

    let policy = RlsPolicy {
        name: parsed.name.clone(),
        collection: parsed.collection.clone(),
        tenant_id: parsed.tenant_id,
        policy_type: parsed.policy_type,
        predicate: parsed.predicate,
        compiled_predicate: parsed.compiled_predicate,
        mode: parsed.mode,
        on_deny: parsed.on_deny,
        enabled: true,
        created_by: identity.username.clone(),
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    let stored = StoredRlsPolicy::from_runtime(&policy)
        .map_err(|e| sqlstate_error("XX000", &format!("rls serialize: {e}")))?;

    let entry = CatalogEntry::PutRlsPolicy(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_rls_policy(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state.rls.install_replicated_policy(policy);
    }

    let mode_str = if parsed.is_restrictive {
        " RESTRICTIVE"
    } else {
        ""
    };
    state.audit_record(
        AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "RLS policy '{}' created on '{}' for {}{}",
            parsed.name, parsed.collection, parsed.policy_type_label, mode_str
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE RLS POLICY"))])
}
