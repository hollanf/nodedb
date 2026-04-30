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
use super::parse::compile_rls_predicate;

/// Parsed `CREATE RLS POLICY` request.
#[derive(Clone, Copy)]
pub struct CreateRlsPolicyRequest<'a> {
    pub name: &'a str,
    pub collection: &'a str,
    pub policy_type_raw: &'a str,
    pub predicate_raw: &'a str,
    pub is_restrictive: bool,
    pub on_deny_raw: Option<&'a str>,
    pub tenant_id_override: Option<u64>,
}

/// `CREATE RLS POLICY <name> ON <collection> FOR <read|write|all>
///     USING (<predicate>) [RESTRICTIVE] [TENANT <id>] [ON DENY ...]`
///
/// All fields are pre-parsed by the `nodedb-sql` AST layer; this handler
/// only performs predicate compilation and catalog mutation.
pub fn create_rls_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateRlsPolicyRequest<'_>,
) -> PgWireResult<Vec<Response>> {
    let CreateRlsPolicyRequest {
        name,
        collection,
        policy_type_raw,
        predicate_raw,
        is_restrictive,
        on_deny_raw,
        tenant_id_override,
    } = *req;
    if !identity.is_superuser && !identity.roles.contains(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser or tenant_admin",
        ));
    }

    let tenant_id = tenant_id_override.unwrap_or(identity.tenant_id.as_u64());

    let policy_type_label = policy_type_raw.to_uppercase();
    let policy_type = match policy_type_label.as_str() {
        "READ" => crate::control::security::rls::PolicyType::Read,
        "WRITE" => crate::control::security::rls::PolicyType::Write,
        "ALL" => crate::control::security::rls::PolicyType::All,
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!("invalid policy type: {other}. Expected READ, WRITE, or ALL"),
            ));
        }
    };

    let mode = if is_restrictive {
        crate::control::security::predicate::PolicyMode::Restrictive
    } else {
        crate::control::security::predicate::PolicyMode::Permissive
    };

    let compiled = compile_rls_predicate(predicate_raw, on_deny_raw)?;

    // Pre-check duplicate so the proposing node fails fast with a
    // clean SQLSTATE instead of going through raft only to be a
    // silent overwrite.
    if state.rls.policy_exists(tenant_id, collection, name) {
        return Err(sqlstate_error(
            "42710",
            &format!("RLS policy '{}' already exists on '{}'", name, collection),
        ));
    }

    let policy = RlsPolicy {
        name: name.to_string(),
        collection: collection.to_string(),
        tenant_id,
        policy_type,
        predicate: compiled.predicate,
        compiled_predicate: compiled.compiled_predicate,
        mode,
        on_deny: compiled.on_deny,
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

    let mode_str = if is_restrictive { " RESTRICTIVE" } else { "" };
    state.audit_record(
        AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "RLS policy '{}' created on '{}' for {}{}",
            name, collection, policy_type_label, mode_str
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE RLS POLICY"))])
}
