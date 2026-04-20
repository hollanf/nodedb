//! Audit-log emission by DDL + catalog-backed audit persistence across
//! restart cycles.

mod common;

use std::sync::Arc;

use common::pgwire_auth_helpers::{ddl_ok, make_state, superuser};
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::audit::AuditEvent;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;

#[tokio::test]
async fn audit_records_create_and_drop() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER audit_test WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "DROP USER audit_test").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::PrivilegeChange);
    assert!(
        events.len() >= 2,
        "expected at least 2 PrivilegeChange events, got {}",
        events.len()
    );
    assert!(events.iter().any(|e| e.detail.contains("created")));
    assert!(events.iter().any(|e| e.detail.contains("dropped")));
}

#[tokio::test]
async fn audit_records_grant_revoke() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER karl WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE readwrite TO karl").await;
    ddl_ok(&state, &su, "REVOKE ROLE readonly FROM karl").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::PrivilegeChange);
    assert!(events.iter().any(|e| e.detail.contains("granted")));
    assert!(events.iter().any(|e| e.detail.contains("revoked")));
}

#[test]
fn audit_flush_persists_to_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let catalog_path = dir.path().join("system.redb");

    let auth_config = nodedb::config::auth::AuthConfig::default();
    let (dispatcher, _sides) = Dispatcher::new(1, 64);
    let state = SharedState::open(
        dispatcher,
        wal,
        &catalog_path,
        &auth_config,
        nodedb_types::config::TuningConfig::default(),
        nodedb::bridge::quiesce::CollectionQuiesce::new(),
    )
    .unwrap();

    state.audit_record(AuditEvent::AuthSuccess, None, "test", "user logged in");
    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(TenantId::new(1)),
        "test",
        "granted role",
    );

    state.flush_audit_log();

    let catalog = state.credentials.catalog().as_ref().unwrap();
    let count = catalog.audit_entry_count().unwrap();
    assert_eq!(count, 2, "expected 2 persisted audit entries");

    let max_seq = catalog.load_audit_max_seq().unwrap();
    assert!(max_seq >= 2);
}

#[test]
fn audit_sequence_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let catalog_path = dir.path().join("system.redb");
    let auth_config = nodedb::config::auth::AuthConfig::default();

    {
        let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::open(
            dispatcher,
            wal,
            &catalog_path,
            &auth_config,
            nodedb_types::config::TuningConfig::default(),
            nodedb::bridge::quiesce::CollectionQuiesce::new(),
        )
        .unwrap();

        state.audit_record(AuditEvent::AuthSuccess, None, "src", "event1");
        state.audit_record(AuditEvent::AuthSuccess, None, "src", "event2");
        state.flush_audit_log();
    }

    {
        let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::open(
            dispatcher,
            wal,
            &catalog_path,
            &auth_config,
            nodedb_types::config::TuningConfig::default(),
            nodedb::bridge::quiesce::CollectionQuiesce::new(),
        )
        .unwrap();

        state.audit_record(AuditEvent::AdminAction, None, "src", "event3");
        state.flush_audit_log();

        let catalog = state.credentials.catalog().as_ref().unwrap();
        let count = catalog.audit_entry_count().unwrap();
        assert_eq!(
            count, 3,
            "expected 3 total persisted audit entries across restarts"
        );

        let max_seq = catalog.load_audit_max_seq().unwrap();
        assert!(max_seq >= 3, "sequence should be >= 3, got {max_seq}");
    }
}
