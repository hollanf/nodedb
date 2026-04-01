//! `ALTER FUNCTION ... OWNER TO` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `ALTER FUNCTION <name> OWNER TO <new_owner>`
pub fn alter_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "alter functions")?;

    // ALTER FUNCTION <name> OWNER TO <new_owner>
    if parts.len() < 6
        || !parts[3].eq_ignore_ascii_case("OWNER")
        || !parts[4].eq_ignore_ascii_case("TO")
    {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER FUNCTION <name> OWNER TO <new_owner>",
        ));
    }

    let name = parts[2].to_lowercase();
    let new_owner = parts[5].trim_end_matches(';').to_string();

    let tenant_id = identity.tenant_id.as_u32();
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let mut func = catalog
        .get_function(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42883", &format!("function '{name}' does not exist")))?;

    let old_owner = func.owner.clone();
    func.owner = new_owner.clone();
    catalog
        .put_function(&func)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER FUNCTION {name} OWNER TO {new_owner} (was: {old_owner})"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER FUNCTION"))])
}
