//! System catalog — redb-backed persistent storage for auth metadata.
//!
//! Stores users, roles, and permissions in `{data_dir}/system.redb`.
//! Lives on the Control Plane (Send + Sync). Uses redb's ACID transactions
//! for crash-safe writes, same technology as the sparse engine.

use std::path::Path;

use redb::{Database, TableDefinition};
use tracing::info;

/// Table: username (string) -> MessagePack-serialized user record.
pub(super) const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.users");

/// Table: key_id (string) -> MessagePack-serialized API key record.
pub(super) const API_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.api_keys");

/// Table: tenant_id (string) -> MessagePack-serialized tenant record.
pub(super) const TENANTS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.tenants");

/// Table: seq (u64 as big-endian bytes) -> MessagePack-serialized audit entry.
pub(super) const AUDIT_LOG: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.audit_log");

/// Table: role_name -> MessagePack-serialized custom role record.
pub(super) const ROLES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.roles");

/// Table: "target:role_or_user" -> MessagePack-serialized permission grant.
pub(super) const PERMISSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.permissions");

/// Table: "{object_type}:{tenant_id}:{object_name}" -> owner username.
pub(super) const OWNERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.owners");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized collection metadata.
pub(super) const COLLECTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.collections");

/// Table: metadata key -> value bytes (counters, config).
pub(super) const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.metadata");

pub fn catalog_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Key format: "{object_type}:{tenant_id}:{object_name}"
pub fn owner_key(object_type: &str, tenant_id: u32, object_name: &str) -> String {
    format!("{object_type}:{tenant_id}:{object_name}")
}

/// Serializable user record for redb storage.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct StoredUser {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: u32,
    pub password_hash: String,
    pub scram_salt: Vec<u8>,
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<String>,
    pub is_superuser: bool,
    pub is_active: bool,
    /// True if this is a service account (no password, API key auth only).
    #[serde(default)]
    pub is_service_account: bool,
    /// Unix timestamp (seconds) when the user was created.
    #[serde(default)]
    pub created_at: u64,
    /// Unix timestamp (seconds) when the user was last modified.
    #[serde(default)]
    pub updated_at: u64,
    /// Unix timestamp (seconds) when the password expires. 0 = no expiry.
    #[serde(default)]
    pub password_expires_at: u64,
    /// MD5 hash for pgwire MD5 auth: `md5(password + username)` as hex.
    #[serde(default)]
    pub md5_hash: String,
}

/// Serializable API key record for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredApiKey {
    /// Unique key identifier (used as prefix in the token).
    pub key_id: String,
    /// SHA-256 hash of the secret portion.
    pub secret_hash: Vec<u8>,
    /// User this key belongs to.
    pub username: String,
    pub user_id: u64,
    pub tenant_id: u32,
    /// Unix timestamp (seconds) when the key expires. 0 = no expiry.
    pub expires_at: u64,
    /// Whether this key has been revoked.
    pub is_revoked: bool,
    /// Unix timestamp (seconds) when the key was created.
    pub created_at: u64,
    /// Permission scope restriction. Empty = inherit all user permissions.
    /// Format: ["read:collection_name", "write:collection_name", ...]
    #[serde(default)]
    pub scope: Vec<String>,
}

/// Serializable tenant record for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredTenant {
    pub tenant_id: u32,
    pub name: String,
    pub created_at: u64,
    pub is_active: bool,
}

/// Serializable audit entry for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredAuditEntry {
    pub seq: u64,
    pub timestamp_us: u64,
    pub event: String,
    pub tenant_id: Option<u32>,
    pub source: String,
    pub detail: String,
    /// SHA-256 hash of the previous entry (hex). Empty for first entry.
    #[serde(default)]
    pub prev_hash: String,
}

/// Serializable custom role for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredRole {
    pub name: String,
    pub tenant_id: u32,
    /// Parent role name for inheritance. Empty = no parent.
    pub parent: String,
    pub created_at: u64,
}

/// Serializable permission grant for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredPermission {
    /// What the grant applies to: "cluster", "tenant:1", "collection:1:users"
    pub target: String,
    /// Who receives the grant: role name or "user:username"
    pub grantee: String,
    /// Permission type: "read", "write", "create", "drop", "alter", "admin", "monitor"
    pub permission: String,
    pub granted_by: String,
    pub granted_at: u64,
}

/// Serializable ownership record.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredOwner {
    /// "collection", "index"
    pub object_type: String,
    /// Object name (e.g. collection name).
    pub object_name: String,
    pub tenant_id: u32,
    pub owner_username: String,
}

/// Persistent system catalog backed by redb.
/// Serializable collection metadata for redb storage.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredCollection {
    pub tenant_id: u32,
    pub name: String,
    pub owner: String,
    pub created_at: u64,
    /// Optional field type declarations. Empty = schemaless.
    /// Format: [("field_name", "type_name"), ...]
    #[serde(default)]
    pub fields: Vec<(String, String)>,
    pub is_active: bool,
}

pub struct SystemCatalog {
    pub(super) db: Database,
}

impl SystemCatalog {
    /// Open or create the system catalog at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| catalog_err("open", e))?;

        // Ensure tables exist.
        let write_txn = db.begin_write().map_err(|e| catalog_err("init txn", e))?;
        {
            let _ = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("init users table", e))?;
            let _ = write_txn
                .open_table(API_KEYS)
                .map_err(|e| catalog_err("init api_keys table", e))?;
            let _ = write_txn
                .open_table(ROLES)
                .map_err(|e| catalog_err("init roles table", e))?;
            let _ = write_txn
                .open_table(PERMISSIONS)
                .map_err(|e| catalog_err("init permissions table", e))?;
            let _ = write_txn
                .open_table(OWNERS)
                .map_err(|e| catalog_err("init owners table", e))?;
            let _ = write_txn
                .open_table(TENANTS)
                .map_err(|e| catalog_err("init tenants table", e))?;
            let _ = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("init audit_log table", e))?;
            let _ = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("init collections table", e))?;
            let _ = write_txn
                .open_table(METADATA)
                .map_err(|e| catalog_err("init metadata table", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("init commit", e))?;

        info!(path = %path.display(), "system catalog opened");

        Ok(Self { db })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_and_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "alice".into(),
            tenant_id: 1,
            password_hash: "$argon2id$test".into(),
            scram_salt: vec![1, 2, 3, 4],
            scram_salted_password: vec![5, 6, 7, 8],
            roles: vec!["readwrite".into()],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            md5_hash: String::new(),
        };

        catalog.put_user(&user).unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].username, "alice");
        assert_eq!(loaded[0].tenant_id, 1);
    }

    #[test]
    fn delete_user() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "bob".into(),
            tenant_id: 1,
            password_hash: "hash".into(),
            scram_salt: vec![],
            scram_salted_password: vec![],
            roles: vec![],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            md5_hash: String::new(),
        };

        catalog.put_user(&user).unwrap();
        catalog.delete_user("bob").unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn next_user_id_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            assert_eq!(catalog.load_next_user_id().unwrap(), 1); // Default.
            catalog.save_next_user_id(42).unwrap();
        }

        // Reopen — ID should persist.
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.load_next_user_id().unwrap(), 42);
    }

    #[test]
    fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .put_user(&StoredUser {
                    user_id: 5,
                    username: "persistent".into(),
                    tenant_id: 3,
                    password_hash: "hash".into(),
                    scram_salt: vec![1],
                    scram_salted_password: vec![2],
                    roles: vec!["readonly".into(), "monitor".into()],
                    is_superuser: false,
                    is_active: true,
                    is_service_account: false,
                    created_at: 0,
                    updated_at: 0,
                    password_expires_at: 0,
                    md5_hash: String::new(),
                })
                .unwrap();
        }

        // Reopen — user should survive.
        let catalog = SystemCatalog::open(&path).unwrap();
        let users = catalog.load_all_users().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "persistent");
        assert_eq!(users[0].user_id, 5);
        assert_eq!(users[0].roles, vec!["readonly", "monitor"]);
    }
}
