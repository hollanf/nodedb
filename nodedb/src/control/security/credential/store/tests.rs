//! Unit tests for `CredentialStore` — in-memory + persistent +
//! reload-after-mutation coverage.

use super::CredentialStore;
use crate::control::security::identity::Role;
use crate::types::TenantId;

#[test]
fn in_memory_create_and_verify() {
    let store = CredentialStore::new();
    store.bootstrap_superuser("nodedb", "secret").unwrap();
    assert!(store.verify_password("nodedb", "secret"));
    assert!(!store.verify_password("nodedb", "wrong"));
}

#[test]
fn persistent_create_and_reload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");

    {
        let store = CredentialStore::open(&path).unwrap();
        store
            .create_user("alice", "pass123", TenantId::new(1), vec![Role::ReadWrite])
            .unwrap();
        store.bootstrap_superuser("nodedb", "secret").unwrap();
    }

    {
        let store = CredentialStore::open(&path).unwrap();
        let alice = store.get_user("alice").unwrap();
        assert_eq!(alice.tenant_id, TenantId::new(1));
        assert!(alice.roles.contains(&Role::ReadWrite));
        assert!(store.verify_password("alice", "pass123"));

        let admin = store.get_user("nodedb").unwrap();
        assert!(admin.is_superuser);
    }
}

#[test]
fn persistent_deactivate_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");

    {
        let store = CredentialStore::open(&path).unwrap();
        store
            .create_user("bob", "pass", TenantId::new(1), vec![Role::ReadOnly])
            .unwrap();
        store.deactivate_user("bob").unwrap();
    }

    {
        let store = CredentialStore::open(&path).unwrap();
        assert!(store.get_user("bob").is_none());
    }
}

#[test]
fn persistent_role_changes_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");

    {
        let store = CredentialStore::open(&path).unwrap();
        store
            .create_user("carol", "pass", TenantId::new(1), vec![Role::ReadOnly])
            .unwrap();
        store.add_role("carol", Role::ReadWrite).unwrap();
        store.remove_role("carol", &Role::ReadOnly).unwrap();
    }

    {
        let store = CredentialStore::open(&path).unwrap();
        let carol = store.get_user("carol").unwrap();
        assert!(carol.roles.contains(&Role::ReadWrite));
        assert!(!carol.roles.contains(&Role::ReadOnly));
    }
}

#[test]
fn persistent_password_change_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");

    {
        let store = CredentialStore::open(&path).unwrap();
        store
            .create_user("dave", "old_pass", TenantId::new(1), vec![Role::ReadWrite])
            .unwrap();
        store.update_password("dave", "new_pass").unwrap();
    }

    {
        let store = CredentialStore::open(&path).unwrap();
        assert!(store.verify_password("dave", "new_pass"));
        assert!(!store.verify_password("dave", "old_pass"));
    }
}

#[test]
fn user_id_counter_persists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");

    let first_id;
    {
        let store = CredentialStore::open(&path).unwrap();
        first_id = store
            .create_user("u1", "p", TenantId::new(1), vec![])
            .unwrap();
        store
            .create_user("u2", "p", TenantId::new(1), vec![])
            .unwrap();
    }

    {
        let store = CredentialStore::open(&path).unwrap();
        let next_id = store
            .create_user("u3", "p", TenantId::new(1), vec![])
            .unwrap();
        assert!(next_id > first_id + 1);
    }
}

// ── T4-C: credential expiry / rotation tests ────────────────────────────────

/// Decode a pre-T4-C `StoredUser` msgpack blob (no `must_change_password` or
/// `password_changed_at` fields) and assert that the new fields default
/// correctly: `must_change_password = false`, `password_changed_at = created_at`.
#[test]
fn backward_compat_stored_user_defaults() {
    use crate::control::security::catalog::StoredUser;
    use crate::control::security::credential::record::UserRecord;

    // Build a minimal msgpack map matching the pre-T4-C StoredUser layout.
    // Fields present: user_id, username, tenant_id, password_hash, scram_salt,
    // scram_salted_password, roles, is_superuser, is_active.
    // Absent (legacy defaults): is_service_account, created_at, updated_at,
    // password_expires_at, must_change_password, password_changed_at.
    let created_at_epoch: u64 = 1_700_000_000;
    let mut buf = Vec::new();
    // 9 fields (all that existed before T4-C)
    rmpv::encode::write_value(
        &mut buf,
        &rmpv::Value::Map(vec![
            (
                rmpv::Value::String("user_id".into()),
                rmpv::Value::Integer(rmpv::Integer::from(42u64)),
            ),
            (
                rmpv::Value::String("username".into()),
                rmpv::Value::String("legacy_user".into()),
            ),
            (
                rmpv::Value::String("tenant_id".into()),
                rmpv::Value::Integer(rmpv::Integer::from(1u32)),
            ),
            (
                rmpv::Value::String("password_hash".into()),
                rmpv::Value::String("$argon2id$fake_hash".into()),
            ),
            (
                rmpv::Value::String("scram_salt".into()),
                rmpv::Value::Binary(vec![1, 2, 3]),
            ),
            (
                rmpv::Value::String("scram_salted_password".into()),
                rmpv::Value::Binary(vec![4, 5, 6]),
            ),
            (
                rmpv::Value::String("roles".into()),
                rmpv::Value::Array(vec![rmpv::Value::String("read_write".into())]),
            ),
            (
                rmpv::Value::String("is_superuser".into()),
                rmpv::Value::Boolean(false),
            ),
            (
                rmpv::Value::String("is_active".into()),
                rmpv::Value::Boolean(true),
            ),
            (
                rmpv::Value::String("created_at".into()),
                rmpv::Value::Integer(rmpv::Integer::from(created_at_epoch)),
            ),
        ]),
    )
    .unwrap();

    let stored: StoredUser = zerompk::from_msgpack(&buf).unwrap();
    assert!(
        !stored.must_change_password,
        "must_change_password should default to false"
    );
    assert_eq!(
        stored.password_changed_at, 0,
        "password_changed_at should default to 0 in StoredUser"
    );

    let record = UserRecord::from_stored(stored);
    assert!(!record.must_change_password);
    // from_stored maps password_changed_at=0 → created_at
    assert_eq!(
        record.password_changed_at, created_at_epoch,
        "password_changed_at should fall back to created_at for pre-T4-C records"
    );
}

/// After `update_password`, `password_changed_at` must be updated to now and
/// `must_change_password` must be cleared regardless of prior state.
#[test]
fn update_password_clears_must_change_and_sets_changed_at() {
    let store = CredentialStore::new();
    store
        .create_user("eve", "old", TenantId::new(1), vec![Role::ReadWrite])
        .unwrap();

    // Force must_change_password = true.
    store.set_must_change_password("eve", true).unwrap();
    {
        let users = store.users.read().unwrap();
        assert!(users["eve"].must_change_password);
    }

    let before = super::super::super::time::now_secs();
    store.update_password("eve", "new").unwrap();
    let after = super::super::super::time::now_secs();

    let users = store.users.read().unwrap();
    let rec = &users["eve"];
    assert!(
        !rec.must_change_password,
        "must_change_password should be cleared on password update"
    );
    assert!(
        rec.password_changed_at >= before && rec.password_changed_at <= after + 1,
        "password_changed_at should be set to current time on update"
    );
}

/// `get_scram_credentials` must return `None` when the password is expired
/// (past expiry and no grace period).
#[test]
fn scram_blocks_expired_account_no_grace() {
    let store = CredentialStore::new();
    store
        .create_user("frank", "pass", TenantId::new(1), vec![Role::ReadWrite])
        .unwrap();

    // Set expiry in the past.
    {
        let mut users = store.users.write().unwrap();
        users.get_mut("frank").unwrap().password_expires_at = 1; // epoch + 1 second
    }
    // grace = 0 (default)
    assert!(
        store.get_scram_credentials("frank").is_none(),
        "expired account with no grace should be blocked from SCRAM auth"
    );
}

/// `get_scram_credentials` must return `Some(creds)` with a non-empty warning
/// when within the grace period.
#[test]
fn scram_allows_expired_account_within_grace_with_warning() {
    let mut store = CredentialStore::new();
    store.password_expiry_grace_days = 30; // 30-day grace period
    store
        .create_user(
            "grace_user",
            "pass",
            TenantId::new(1),
            vec![Role::ReadWrite],
        )
        .unwrap();

    // Set expiry 1 second in the past (within 30-day grace).
    {
        let mut users = store.users.write().unwrap();
        users.get_mut("grace_user").unwrap().password_expires_at =
            super::super::super::time::now_secs() - 1;
    }

    let creds = store
        .get_scram_credentials("grace_user")
        .expect("account within grace period should be allowed");
    assert!(
        creds.warning.is_some(),
        "grace-period login should carry a warning"
    );
    let w = creds.warning.unwrap();
    assert!(
        w.contains("grace") || w.contains("expired"),
        "warning should mention expiry or grace: {w}"
    );
}

/// `get_scram_credentials` blocks when `must_change_password` is set and grace = 0.
#[test]
fn scram_blocks_must_change_password_no_grace() {
    let store = CredentialStore::new();
    store
        .create_user("hank", "pass", TenantId::new(1), vec![Role::ReadWrite])
        .unwrap();
    store.set_must_change_password("hank", true).unwrap();
    // grace_days = 0 (default)
    assert!(
        store.get_scram_credentials("hank").is_none(),
        "must_change_password with no grace should block SCRAM auth"
    );
}

/// `verify_password_with_status` must return `(false, _)` when the password
/// is expired past the grace period — even when the password is correct.
/// This pins the security fix on the HTTP/native auth path.
#[test]
fn verify_password_blocks_expired_account() {
    let store = CredentialStore::new();
    store
        .create_user(
            "ivan",
            "correct_pass",
            TenantId::new(1),
            vec![Role::ReadWrite],
        )
        .unwrap();

    // Set expiry in the past, grace = 0.
    {
        let mut users = store.users.write().unwrap();
        users.get_mut("ivan").unwrap().password_expires_at = 1;
    }

    let (ok, _warn) = store.verify_password_with_status("ivan", "correct_pass");
    assert!(
        !ok,
        "verify_password_with_status must deny login for expired account even with correct password"
    );
}

/// `verify_password_with_status` emits a warning when within grace period.
#[test]
fn verify_password_grace_period_emits_warning() {
    let mut store = CredentialStore::new();
    store.password_expiry_grace_days = 7;
    store
        .create_user("judy", "pass", TenantId::new(1), vec![Role::ReadWrite])
        .unwrap();
    {
        let mut users = store.users.write().unwrap();
        users.get_mut("judy").unwrap().password_expires_at =
            super::super::super::time::now_secs() - 1;
    }
    let (ok, warn) = store.verify_password_with_status("judy", "pass");
    assert!(ok, "grace-period login should succeed");
    assert!(warn.is_some(), "grace-period login should carry a warning");
}
