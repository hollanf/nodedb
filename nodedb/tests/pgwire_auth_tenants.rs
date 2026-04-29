//! CREATE/DROP TENANT over the pgwire DDL path, plus the readonly /
//! non-superuser guards on the tenant surface.

mod common;

use common::pgwire_auth_helpers::{assert_readonly_denied, ddl_err, ddl_ok, make_state, superuser};
use nodedb::control::security::audit::AuditEvent;

#[tokio::test]
async fn create_tenant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    assert!(!events.is_empty());
    assert!(events.last().unwrap().detail.contains("acme"));
}

#[tokio::test]
async fn drop_system_tenant_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP TENANT 0").await;
    assert!(err.contains("cannot drop system tenant"), "{err}");
}

#[tokio::test]
async fn tenant_ops_require_superuser() {
    let state = make_state();
    assert_readonly_denied(&state, "CREATE TENANT evil").await;
}

#[tokio::test]
async fn show_tenants_requires_superuser() {
    let state = make_state();
    assert_readonly_denied(&state, "SHOW TENANTS").await;
}
