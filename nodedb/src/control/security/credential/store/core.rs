//! `CredentialStore` struct + constructors + private helpers.
//!
//! Other concerns (crud, auth, list, replication) live in sibling
//! files under `store/` and extend this struct via their own `impl`
//! blocks. The struct fields are `pub(super)` so those siblings can
//! reach them without leaking internals beyond `credential`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;
use tracing::info;

use crate::types::TenantId;

use super::super::super::catalog::SystemCatalog;
use super::super::super::identity::Role;
use super::super::super::time::now_secs;
use super::super::hash::{
    compute_md5_hash, compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::super::lockout::LoginAttemptTracker;
use super::super::record::UserRecord;

/// Credential store with in-memory cache and redb persistence.
///
/// Reads hit the in-memory cache (fast). Writes go to redb first
/// (ACID), then update the cache. On startup, all records are
/// loaded from redb.
///
/// Lives on the Control Plane (`Send + Sync`).
pub struct CredentialStore {
    pub(in crate::control::security::credential) users: RwLock<HashMap<String, UserRecord>>,
    pub(in crate::control::security::credential) next_user_id: RwLock<u64>,
    pub(in crate::control::security::credential) catalog: Option<SystemCatalog>,
    /// Failed login tracking (in-memory only — clears on restart).
    pub(in crate::control::security::credential) login_attempts:
        RwLock<HashMap<String, LoginAttemptTracker>>,
    /// Max failed logins before lockout (0 = disabled).
    pub(in crate::control::security::credential) max_failed_logins: u32,
    /// Lockout duration.
    pub(in crate::control::security::credential) lockout_duration: std::time::Duration,
    /// Password expiry in seconds (0 = no expiry).
    pub(in crate::control::security::credential) password_expiry_secs: u64,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

pub(in crate::control::security::credential) fn read_lock<T>(
    lock: &RwLock<T>,
) -> crate::Result<std::sync::RwLockReadGuard<'_, T>> {
    lock.read().map_err(|e| {
        tracing::error!("credential store read lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

pub(in crate::control::security::credential) fn write_lock<T>(
    lock: &RwLock<T>,
) -> crate::Result<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write().map_err(|e| {
        tracing::error!("credential store write lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

impl CredentialStore {
    /// Create an in-memory-only credential store (for tests).
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            next_user_id: RwLock::new(1),
            catalog: None,
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
        }
    }

    /// Open a persistent credential store backed by redb.
    ///
    /// `path` is the system catalog file (e.g. `{data_dir}/system.redb`).
    /// Loads all existing users into the in-memory cache.
    pub fn open(path: &Path) -> crate::Result<Self> {
        let catalog = SystemCatalog::open(path)?;

        let stored_users = catalog.load_all_users()?;
        let next_id = catalog.load_next_user_id()?;

        let mut users = HashMap::with_capacity(stored_users.len());
        for stored in stored_users {
            let record = UserRecord::from_stored(stored);
            users.insert(record.username.clone(), record);
        }

        let count = users.len();
        if count > 0 {
            info!(count, "loaded users from system catalog");
        }

        Ok(Self {
            users: RwLock::new(users),
            next_user_id: RwLock::new(next_id),
            catalog: Some(catalog),
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
            password_expiry_secs: 0,
        })
    }

    /// Persist a user record to the catalog (if persistent).
    /// Automatically updates `updated_at` timestamp.
    pub(in crate::control::security::credential) fn persist_user(
        &self,
        record: &mut UserRecord,
    ) -> crate::Result<()> {
        record.updated_at = now_secs();
        if let Some(ref catalog) = self.catalog {
            catalog.put_user(&record.to_stored())?;
        }
        Ok(())
    }

    /// Persist the next_user_id counter (if persistent).
    pub(in crate::control::security::credential) fn persist_next_id(
        &self,
        id: u64,
    ) -> crate::Result<()> {
        if let Some(ref catalog) = self.catalog {
            catalog.save_next_user_id(id)?;
        }
        Ok(())
    }

    /// Compute password expiry timestamp from current config.
    pub(in crate::control::security::credential) fn compute_expiry(&self) -> u64 {
        if self.password_expiry_secs > 0 {
            now_secs() + self.password_expiry_secs
        } else {
            0
        }
    }

    pub(in crate::control::security::credential) fn alloc_user_id(&self) -> crate::Result<u64> {
        let mut next = write_lock(&self.next_user_id)?;
        let id = *next;
        *next += 1;
        self.persist_next_id(*next)?;
        Ok(id)
    }

    /// Bootstrap the superuser from config. Called once on startup.
    /// If the user already exists (loaded from catalog), updates
    /// the password.
    pub fn bootstrap_superuser(&self, username: &str, password: &str) -> crate::Result<()> {
        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;

        let mut users = write_lock(&self.users)?;

        if let Some(existing) = users.get_mut(username) {
            // User exists from catalog — update password and
            // ensure active + superuser.
            existing.password_hash = password_hash;
            existing.scram_salt = salt;
            existing.scram_salted_password = scram_salted_password;
            existing.is_superuser = true;
            existing.is_active = true;
            if !existing.roles.contains(&Role::Superuser) {
                existing.roles.push(Role::Superuser);
            }
            self.persist_user(existing)?;
        } else {
            let user_id = self.alloc_user_id()?;
            let mut record = UserRecord {
                user_id,
                username: username.to_string(),
                tenant_id: TenantId::new(0),
                password_hash,
                scram_salt: salt,
                scram_salted_password,
                roles: vec![Role::Superuser],
                is_superuser: true,
                is_active: true,
                is_service_account: false,
                created_at: now_secs(),
                updated_at: now_secs(),
                password_expires_at: self.compute_expiry(),
                md5_hash: compute_md5_hash(username, password),
            };
            self.persist_user(&mut record)?;
            users.insert(username.to_string(), record);
        }

        Ok(())
    }
}
