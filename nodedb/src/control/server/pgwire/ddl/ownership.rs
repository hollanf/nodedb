use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

/// ALTER COLLECTION <name> OWNER TO <user>
///
/// Transfer ownership of a collection. Requires:
/// - Current owner, OR
/// - Superuser / tenant_admin
pub fn alter_collection_owner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // ALTER COLLECTION <name> OWNER TO <user>
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER COLLECTION <name> OWNER TO <user>",
        ));
    }

    let collection = parts[2];
    if !parts[3].eq_ignore_ascii_case("OWNER") || !parts[4].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error(
            "42601",
            "expected OWNER TO after collection name",
        ));
    }
    let new_owner = parts[5];

    // Check authorization: current owner or admin.
    let current_owner = state
        .permissions
        .get_owner("collection", identity.tenant_id, collection);

    let is_current_owner = current_owner
        .as_ref()
        .is_some_and(|o| o == &identity.username);

    if !is_current_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only the current owner, superuser, or tenant_admin can transfer ownership",
        ));
    }

    // Verify new owner exists.
    if state.credentials.get_user(new_owner).is_none() {
        return Err(sqlstate_error(
            "42704",
            &format!("user '{new_owner}' not found"),
        ));
    }

    let catalog = state.credentials.catalog();
    state
        .permissions
        .set_owner(
            "collection",
            identity.tenant_id,
            collection,
            new_owner,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "transferred ownership of collection '{collection}' from '{}' to '{new_owner}'",
            current_owner.unwrap_or_else(|| "<none>".to_string())
        ),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}
