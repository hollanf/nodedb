use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // User management.
    // CREATE USER and ALTER USER are fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP USER ") {
        return Some(super::super::user::drop_user(state, identity, parts));
    }

    // Service accounts.
    if upper.starts_with("CREATE SERVICE ACCOUNT ") {
        return Some(super::super::service_account::create_service_account(
            state, identity, parts,
        ));
    }
    if upper.starts_with("DROP SERVICE ACCOUNT ") {
        return Some(super::super::service_account::drop_service_account(
            state, identity, parts,
        ));
    }

    // System-level settings (ALTER SYSTEM SET ...).
    if upper.starts_with("ALTER SYSTEM ") {
        return Some(super::super::system_ddl::alter_system(
            state, identity, parts,
        ));
    }

    // Tenant management.
    if upper.starts_with("CREATE TENANT ") {
        return Some(super::super::tenant::create_tenant(state, identity, parts));
    }
    if upper.starts_with("ALTER TENANT ") {
        return Some(super::super::tenant::alter_tenant(state, identity, parts));
    }
    if upper.starts_with("DROP TENANT ") {
        return Some(super::super::tenant::drop_tenant(state, identity, parts));
    }
    if upper.starts_with("PURGE TENANT ") {
        return Some(super::super::tenant::purge_tenant(state, identity, parts).await);
    }
    if upper.starts_with("SHOW TENANT USAGE") {
        return Some(super::super::tenant::show_tenant_usage(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW TENANT QUOTA") {
        return Some(super::super::tenant::show_tenant_quota(
            state, identity, parts,
        ));
    }

    // GRANT / REVOKE.
    if upper.starts_with("GRANT ") {
        return Some(super::super::grant::handle_grant(state, identity, parts));
    }
    // `REVOKE API KEY`, `REVOKE SCOPE`, `REVOKE DELEGATION` are routed
    // by the admin dispatcher — exclude them here so permission-grant
    // revocations don't swallow them first.
    if upper.starts_with("REVOKE ")
        && !upper.starts_with("REVOKE API KEY")
        && !upper.starts_with("REVOKE SCOPE")
        && !upper.starts_with("REVOKE DELEGATION")
    {
        return Some(super::super::grant::handle_revoke(state, identity, parts));
    }

    // Role management.
    if upper.starts_with("CREATE ROLE ") {
        return Some(super::super::role::create_role(state, identity, parts));
    }
    if upper.starts_with("ALTER ROLE ") {
        return Some(super::super::role::alter_role(state, identity, parts));
    }
    if upper.starts_with("DROP ROLE ") {
        return Some(super::super::role::drop_role(state, identity, parts));
    }

    None
}
