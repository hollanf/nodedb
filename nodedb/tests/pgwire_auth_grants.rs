//! GRANT / REVOKE role over the pgwire DDL path, plus the readonly guard
//! that covers the same surface.

mod common;

use common::pgwire_auth_helpers::{assert_readonly_denied, ddl_err, ddl_ok, make_state, superuser};
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use nodedb::types::TenantId;

#[tokio::test]
async fn grant_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER grace WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE readwrite TO grace").await;

    let user = state.credentials.get_user("grace").unwrap();
    assert!(user.roles.contains(&Role::ReadOnly));
    assert!(user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn revoke_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER heidi WITH PASSWORD 'pass' ROLE readwrite",
    )
    .await;
    ddl_ok(&state, &su, "REVOKE ROLE readwrite FROM heidi").await;

    let user = state.credentials.get_user("heidi").unwrap();
    assert!(!user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn grant_superuser_requires_superuser() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER ivan WITH PASSWORD 'pass'").await;

    let admin = AuthenticatedIdentity {
        user_id: 50,
        username: "ta".into(),
        tenant_id: TenantId::new(1),
        auth_method: AuthMethod::Trust,
        roles: vec![Role::TenantAdmin],
        is_superuser: false,
    };
    let err = ddl_err(&state, &admin, "GRANT ROLE superuser TO ivan").await;
    assert!(err.contains("only superuser"), "{err}");
}

#[tokio::test]
async fn revoke_own_superuser_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "REVOKE ROLE superuser FROM nodedb").await;
    assert!(err.contains("cannot revoke your own superuser"), "{err}");
}

#[tokio::test]
async fn readonly_cannot_grant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER target WITH PASSWORD 'pass'").await;

    assert_readonly_denied(&state, "GRANT ROLE superuser TO target").await;
}
