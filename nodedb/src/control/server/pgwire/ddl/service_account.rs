use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::types::{parse_role, require_admin, sqlstate_error};

/// CREATE SERVICE ACCOUNT <name> [ROLE <role>] [TENANT <id>]
///
/// Creates a service account — a non-interactive identity that can only
/// authenticate via API keys. No password, no pgwire login.
pub fn create_service_account(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create service accounts")?;

    // CREATE SERVICE ACCOUNT <name> [ROLE <role>] [TENANT <id>]
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE SERVICE ACCOUNT <name> [ROLE <role>] [TENANT <id>]",
        ));
    }

    let name = parts[3];

    // Parse optional ROLE and TENANT.
    let mut role = Role::ReadWrite;
    let mut tenant_id = identity.tenant_id;
    let mut i = 4;
    while i < parts.len() {
        match parts[i].to_uppercase().as_str() {
            "ROLE" if i + 1 < parts.len() => {
                role = parse_role(parts[i + 1]);
                i += 2;
            }
            "TENANT" if i + 1 < parts.len() => {
                if !identity.is_superuser {
                    return Err(sqlstate_error("42501", "only superuser can assign tenants"));
                }
                let tid: u64 = parts[i + 1]
                    .parse()
                    .map_err(|_| sqlstate_error("42601", "TENANT must be a numeric ID"))?;
                tenant_id = crate::types::TenantId::new(tid);
                i += 2;
            }
            _ => i += 1,
        }
    }

    state
        .credentials
        .create_service_account(name, tenant_id, vec![role])
        .map_err(|e| sqlstate_error("42710", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(tenant_id),
        &identity.username,
        &format!("created service account '{name}' in tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new(
        "CREATE SERVICE ACCOUNT",
    ))])
}

/// DROP SERVICE ACCOUNT <name>
pub fn drop_service_account(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop service accounts")?;

    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP SERVICE ACCOUNT <name>",
        ));
    }

    let name = parts[3];

    // Verify it's actually a service account.
    let user = state
        .credentials
        .get_user(name)
        .ok_or_else(|| sqlstate_error("42704", &format!("service account '{name}' not found")))?;
    if !user.is_service_account {
        return Err(sqlstate_error(
            "42809",
            &format!("'{name}' is a user, not a service account. Use DROP USER instead."),
        ));
    }

    let dropped = state
        .credentials
        .deactivate_user(name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    if dropped {
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped service account '{name}'"),
        );
        Ok(vec![Response::Execution(Tag::new("DROP SERVICE ACCOUNT"))])
    } else {
        Err(sqlstate_error(
            "42704",
            &format!("service account '{name}' not found"),
        ))
    }
}
