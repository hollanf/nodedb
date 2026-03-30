use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Permission, Role};
use crate::control::security::permission::parse_permission;
use crate::control::state::SharedState;

use super::super::types::{parse_role, require_admin, sqlstate_error};

/// GRANT ROLE <role> TO <user>
/// GRANT <permission> ON <collection> TO <user_or_role>
pub fn handle_grant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT ROLE <role> TO <user> | GRANT <perm> ON <collection> TO <grantee>",
        ));
    }

    if parts[1].eq_ignore_ascii_case("ROLE") {
        return grant_role(state, identity, parts);
    }

    // GRANT <permission> ON <collection> TO <grantee>
    grant_permission(state, identity, parts)
}

/// REVOKE ROLE <role> FROM <user>
/// REVOKE <permission> ON <collection> FROM <user_or_role>
pub fn handle_revoke(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE ROLE <role> FROM <user> | REVOKE <perm> ON <collection> FROM <grantee>",
        ));
    }

    if parts[1].eq_ignore_ascii_case("ROLE") {
        return revoke_role(state, identity, parts);
    }

    revoke_permission(state, identity, parts)
}

// ── GRANT/REVOKE ROLE ───────────────────────────────────────────────

fn grant_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "grant roles")?;

    let role = parse_role(parts[2]);

    if matches!(role, Role::Superuser) && !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "only superuser can grant superuser role",
        ));
    }

    if !parts[3].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error("42601", "expected TO after role name"));
    }
    let username = parts[4];

    state
        .credentials
        .add_role(username, role.clone())
        .map_err(|e| sqlstate_error("42704", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted role '{role}' to user '{username}'"),
    );

    Ok(vec![Response::Execution(Tag::new("GRANT"))])
}

fn revoke_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "revoke roles")?;

    let role = parse_role(parts[2]);

    if !parts[3].eq_ignore_ascii_case("FROM") {
        return Err(sqlstate_error("42601", "expected FROM after role name"));
    }
    let username = parts[4];

    if username == identity.username && matches!(role, Role::Superuser) {
        return Err(sqlstate_error(
            "42501",
            "cannot revoke your own superuser role",
        ));
    }

    state
        .credentials
        .remove_role(username, &role)
        .map_err(|e| sqlstate_error("42704", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked role '{role}' from user '{username}'"),
    );

    Ok(vec![Response::Execution(Tag::new("REVOKE"))])
}

// ── GRANT/REVOKE <permission> ON <collection> ───────────────────────

/// GRANT <perm> ON <collection> TO <grantee>
/// GRANT EXECUTE ON FUNCTION <name> TO <grantee>
fn grant_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT <perm> ON <collection|FUNCTION name> TO <grantee>",
        ));
    }

    let perm_str = parts[1];
    if !parts[2].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after permission"));
    }

    // Detect ON FUNCTION <name> TO <grantee>  (7 parts)
    // vs     ON <collection> TO <grantee>     (6 parts)
    let (target, object_desc) = if parts[3].eq_ignore_ascii_case("FUNCTION") {
        if parts.len() < 7 {
            return Err(sqlstate_error(
                "42601",
                "syntax: GRANT <perm> ON FUNCTION <name> TO <grantee>",
            ));
        }
        let func_name = parts[4].to_lowercase();
        let target =
            crate::control::security::permission::function_target(identity.tenant_id, &func_name);
        (target, format!("function '{func_name}'"))
    } else {
        let collection = parts[3];
        let target = format!("collection:{}:{collection}", identity.tenant_id.as_u32());
        (target, format!("collection '{collection}'"))
    };

    // Find TO keyword (position varies based on ON FUNCTION vs ON collection).
    let to_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TO"))
        .ok_or_else(|| sqlstate_error("42601", "expected TO <grantee>"))?;
    if to_idx + 1 >= parts.len() {
        return Err(sqlstate_error("42601", "expected grantee after TO"));
    }
    let grantee = parts[to_idx + 1];

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

    let catalog = state.credentials.catalog();

    for perm in &perms {
        state
            .permissions
            .grant(
                &target,
                grantee,
                *perm,
                &identity.username,
                catalog.as_ref(),
            )
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted {perm_str} on {object_desc} to '{grantee}'"),
    );

    Ok(vec![Response::Execution(Tag::new("GRANT"))])
}

/// REVOKE <perm> ON <collection> FROM <grantee>
/// REVOKE EXECUTE ON FUNCTION <name> FROM <grantee>
fn revoke_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE <perm> ON <collection|FUNCTION name> FROM <grantee>",
        ));
    }

    let perm_str = parts[1];
    if !parts[2].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after permission"));
    }

    // Detect ON FUNCTION <name> FROM <grantee> vs ON <collection> FROM <grantee>.
    let (target, object_desc) = if parts[3].eq_ignore_ascii_case("FUNCTION") {
        if parts.len() < 7 {
            return Err(sqlstate_error(
                "42601",
                "syntax: REVOKE <perm> ON FUNCTION <name> FROM <grantee>",
            ));
        }
        let func_name = parts[4].to_lowercase();
        let target =
            crate::control::security::permission::function_target(identity.tenant_id, &func_name);
        (target, format!("function '{func_name}'"))
    } else {
        let collection = parts[3];
        let target = format!("collection:{}:{collection}", identity.tenant_id.as_u32());
        (target, format!("collection '{collection}'"))
    };

    let from_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| sqlstate_error("42601", "expected FROM <grantee>"))?;
    if from_idx + 1 >= parts.len() {
        return Err(sqlstate_error("42601", "expected grantee after FROM"));
    }
    let grantee = parts[from_idx + 1];

    require_admin(identity, "revoke permissions")?;

    let perm = parse_permission(perm_str)
        .ok_or_else(|| sqlstate_error("42601", &format!("unknown permission: {perm_str}")))?;

    let catalog = state.credentials.catalog();

    state
        .permissions
        .revoke(&target, grantee, perm, catalog.as_ref())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked {perm_str} on {object_desc} from '{grantee}'"),
    );

    Ok(vec![Response::Execution(Tag::new("REVOKE"))])
}
