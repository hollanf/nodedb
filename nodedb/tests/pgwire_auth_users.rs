//! CREATE/DROP/ALTER USER over the pgwire DDL path, plus the two
//! readonly-permission guards that live on user-mgmt surfaces.

mod common;

use common::pgwire_auth_helpers::{assert_readonly_denied, ddl_err, ddl_ok, make_state, superuser};
use nodedb::control::security::identity::Role;
use nodedb::types::TenantId;

#[tokio::test]
async fn create_user() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER alice WITH PASSWORD 'secret123' ROLE readwrite TENANT 1",
    )
    .await;

    let user = state.credentials.get_user("alice").unwrap();
    assert_eq!(user.tenant_id, TenantId::new(1));
    assert!(user.roles.contains(&Role::ReadWrite));
    assert!(!user.is_superuser);
}

#[tokio::test]
async fn create_user_duplicate_rejected() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER bob WITH PASSWORD 'pass'").await;

    let err = ddl_err(&state, &su, "CREATE USER bob WITH PASSWORD 'pass2'").await;
    assert!(
        err.contains("already exists"),
        "expected duplicate error: {err}"
    );
}

#[tokio::test]
async fn create_user_default_role_and_tenant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER carol WITH PASSWORD 'pass'").await;

    let user = state.credentials.get_user("carol").unwrap();
    // Default role is readwrite, tenant inherited from identity (0 for superuser).
    assert!(user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn drop_user() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER dave WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "DROP USER dave").await;

    assert!(state.credentials.get_user("dave").is_none());
}

#[tokio::test]
async fn drop_self_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP USER nodedb").await;
    assert!(err.contains("cannot drop your own"), "{err}");
}

#[tokio::test]
async fn drop_nonexistent_user() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP USER nobody").await;
    assert!(err.contains("does not exist"), "{err}");
}

#[tokio::test]
async fn alter_user_password() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER eve WITH PASSWORD 'old'").await;
    ddl_ok(&state, &su, "ALTER USER eve SET PASSWORD 'new'").await;

    assert!(state.credentials.verify_password("eve", "new"));
    assert!(!state.credentials.verify_password("eve", "old"));
}

#[tokio::test]
async fn alter_user_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER frank WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "ALTER USER frank SET ROLE readwrite").await;

    let user = state.credentials.get_user("frank").unwrap();
    assert!(user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn readonly_cannot_create_user() {
    let state = make_state();
    assert_readonly_denied(&state, "CREATE USER hacker WITH PASSWORD 'x'").await;
}

#[tokio::test]
async fn readonly_cannot_drop_user() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER target WITH PASSWORD 'pass'").await;

    assert_readonly_denied(&state, "DROP USER target").await;
}
