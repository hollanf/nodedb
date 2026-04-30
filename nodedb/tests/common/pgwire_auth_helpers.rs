//! Shared fixtures for `pgwire_auth_*` integration tests.
//!
//! Each split test file needs: a minimal `SharedState`, two canonical
//! identities (superuser + readonly), and two DDL runners (expect ok /
//! expect err). Keeping them here avoids copy-paste drift across files.

#![allow(dead_code)]

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use nodedb::control::server::pgwire::ddl;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb::wal::WalManager;

/// Create a minimal `SharedState` (no Data Plane needed for DDL tests).
pub fn make_state() -> Arc<SharedState> {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    SharedState::new(dispatcher, wal)
}

/// Superuser identity for DDL tests.
pub fn superuser() -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: "nodedb".into(),
        tenant_id: TenantId::new(0),
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

/// Readonly identity for permission tests.
pub fn readonly_user() -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 99,
        username: "viewer".into(),
        tenant_id: TenantId::new(1),
        auth_method: AuthMethod::Trust,
        roles: vec![Role::ReadOnly],
        is_superuser: false,
    }
}

/// Run DDL, expect success.
pub async fn ddl_ok(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) {
    let result = ddl::dispatch(state, identity, sql).await;
    assert!(result.is_some(), "DDL not recognized: {sql}");
    result
        .unwrap()
        .unwrap_or_else(|e| panic!("DDL failed: {sql}: {e}"));
}

/// Run DDL, expect error; return the error string for assertions.
pub async fn ddl_err(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) -> String {
    let result = ddl::dispatch(state, identity, sql).await;
    assert!(result.is_some(), "DDL not recognized: {sql}");
    let err = result.unwrap().unwrap_err();
    err.to_string()
}

/// Run DDL as a readonly identity and assert it is denied with `permission denied`.
pub async fn assert_readonly_denied(state: &SharedState, sql: &str) {
    let viewer = readonly_user();
    let err = ddl_err(state, &viewer, sql).await;
    assert!(err.contains("permission denied"), "{err}");
}
