use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{require_admin, sqlstate_error};

/// CREATE ROLE <name> [INHERIT <parent>]
pub fn create_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create roles")?;

    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE ROLE <name> [INHERIT <parent>]",
        ));
    }

    let name = parts[2];
    let parent = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("INHERIT") {
        Some(parts[4])
    } else {
        None
    };

    let catalog = state.credentials.catalog();
    state
        .roles
        .create_role(name, identity.tenant_id, parent, catalog.as_ref())
        .map_err(|e| sqlstate_error("42710", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "created role '{name}'{}",
            parent.map_or(String::new(), |p| format!(" inheriting from '{p}'"))
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE ROLE"))])
}

/// ALTER ROLE <name> SET INHERIT <parent>
pub fn alter_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "alter roles")?;

    // ALTER ROLE <name> SET INHERIT <parent>
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER ROLE <name> SET INHERIT <parent>",
        ));
    }

    let name = parts[2];
    if !parts[3].eq_ignore_ascii_case("SET") || !parts[4].eq_ignore_ascii_case("INHERIT") {
        return Err(sqlstate_error(
            "42601",
            "expected SET INHERIT after role name",
        ));
    }
    let parent = parts[5];

    // Drop and re-create with new parent (simple approach for immutable-role stores).
    let catalog = state.credentials.catalog();
    let old_role = state
        .roles
        .get_role(name)
        .ok_or_else(|| sqlstate_error("42704", &format!("role '{name}' not found")))?;

    state
        .roles
        .drop_role(name, catalog.as_ref())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state
        .roles
        .create_role(name, old_role.tenant_id, Some(parent), catalog.as_ref())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("altered role '{name}': set inherit '{parent}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER ROLE"))])
}

/// DROP ROLE <name>
pub fn drop_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop roles")?;

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP ROLE <name>"));
    }

    let name = parts[2];
    let catalog = state.credentials.catalog();
    let dropped = state
        .roles
        .drop_role(name, catalog.as_ref())
        .map_err(|e| sqlstate_error("42704", &e.to_string()))?;

    if dropped {
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped role '{name}'"),
        );
        Ok(vec![Response::Execution(Tag::new("DROP ROLE"))])
    } else {
        Err(sqlstate_error(
            "42704",
            &format!("role '{name}' does not exist"),
        ))
    }
}
