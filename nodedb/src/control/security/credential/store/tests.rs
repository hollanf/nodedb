//! Unit tests for `CredentialStore` — in-memory + persistent +
//! reload-after-mutation coverage.

use super::CredentialStore;
use crate::control::security::identity::Role;
use crate::types::TenantId;

#[test]
fn in_memory_create_and_verify() {
    let store = CredentialStore::new();
    store.bootstrap_superuser("admin", "secret").unwrap();
    assert!(store.verify_password("admin", "secret"));
    assert!(!store.verify_password("admin", "wrong"));
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
        store.bootstrap_superuser("admin", "secret").unwrap();
    }

    {
        let store = CredentialStore::open(&path).unwrap();
        let alice = store.get_user("alice").unwrap();
        assert_eq!(alice.tenant_id, TenantId::new(1));
        assert!(alice.roles.contains(&Role::ReadWrite));
        assert!(store.verify_password("alice", "pass123"));

        let admin = store.get_user("admin").unwrap();
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
