//! Cluster replication hooks for [`CredentialStore`].
//!
//! Powers the `CatalogEntry::PutUser` / `DeactivateUser` pipeline.
//! Every method here is called from a specific point in the
//! replicated-DDL flow:
//!
//! - [`CredentialStore::prepare_user`] builds a complete
//!   [`StoredUser`] with fresh Argon2 hash + SCRAM salt +
//!   `user_id`, without touching the in-memory map or redb. The
//!   leader calls this before proposing the entry through raft.
//! - [`CredentialStore::prepare_user_update`] overlays changes on
//!   an existing user for `ALTER USER SET PASSWORD / SET ROLE`.
//! - [`CredentialStore::install_replicated_user`] upserts a
//!   `StoredUser` (computed on another node) into the in-memory
//!   cache, bumping `next_user_id` to stay ahead of replicated ids.
//! - [`CredentialStore::install_replicated_deactivate`] marks the
//!   in-memory record inactive for `CatalogEntry::DeactivateUser`.
//!
//! Password hashing + scram salt generation must happen on the
//! leader because followers cannot reproduce a random salt;
//! followers accept the leader's fully-computed `StoredUser`
//! verbatim.

use crate::types::TenantId;

use super::super::super::catalog::StoredUser;
use super::super::super::identity::Role;
use super::super::super::time::now_secs;
use super::super::hash::{
    compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::super::record::UserRecord;
use super::core::{CredentialStore, read_lock};

impl CredentialStore {
    /// Build a `StoredUser` ready for replication via
    /// `CatalogEntry::PutUser`. Allocates a user_id, hashes the
    /// password (Argon2 + SCRAM salt), but does NOT insert
    /// into the in-memory map or write to redb — the applier does
    /// that on every node after the raft commit.
    pub fn prepare_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<StoredUser> {
        {
            let users = read_lock(&self.users)?;
            if users.contains_key(username) {
                return Err(crate::Error::BadRequest {
                    detail: format!("user '{username}' already exists"),
                });
            }
        }

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;
        let user_id = self.alloc_user_id()?;
        let is_superuser = roles.contains(&Role::Superuser);
        let now = now_secs();

        Ok(StoredUser {
            user_id,
            username: username.to_string(),
            tenant_id: tenant_id.as_u32(),
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles: roles.iter().map(|r| r.to_string()).collect(),
            is_superuser,
            is_active: true,
            is_service_account: false,
            created_at: now,
            updated_at: now,
            password_expires_at: self.compute_expiry(),
        })
    }

    /// Build an updated `StoredUser` from an existing user with
    /// specific fields replaced. Used by `ALTER USER SET PASSWORD`
    /// and `ALTER USER SET ROLE`. Returns the updated record
    /// ready for propose.
    pub fn prepare_user_update(
        &self,
        username: &str,
        new_password: Option<&str>,
        new_roles: Option<Vec<Role>>,
    ) -> crate::Result<StoredUser> {
        let users = read_lock(&self.users)?;
        let existing = users
            .get(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !existing.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        let mut stored = existing.to_stored();
        drop(users);

        if let Some(pw) = new_password {
            let salt = generate_scram_salt();
            stored.scram_salted_password = compute_scram_salted_password(pw, &salt);
            stored.scram_salt = salt;
            stored.password_hash = hash_password_argon2(pw)?;
            stored.password_expires_at = self.compute_expiry();
        }
        if let Some(roles) = new_roles {
            stored.is_superuser = roles.contains(&Role::Superuser);
            stored.roles = roles.iter().map(|r| r.to_string()).collect();
        }
        stored.updated_at = now_secs();
        Ok(stored)
    }

    /// Install a replicated `StoredUser` into the in-memory cache.
    /// Called by the production `MetadataCommitApplier` post-apply
    /// hook after the applier has written the record to local
    /// redb. Never errors — a poisoned lock falls through to
    /// in-place recovery so a single bad user write doesn't stall
    /// raft.
    pub fn install_replicated_user(&self, stored: &StoredUser) {
        let record = UserRecord::from_stored(stored.clone());
        let mut users = self.users.write().unwrap_or_else(|p| p.into_inner());
        users.insert(stored.username.clone(), record);
        let mut next = self.next_user_id.write().unwrap_or_else(|p| p.into_inner());
        if stored.user_id + 1 > *next {
            *next = stored.user_id + 1;
        }
    }

    /// Mark a replicated user as inactive in the in-memory cache.
    /// Symmetric partner to `install_replicated_user` for the
    /// `CatalogEntry::DeactivateUser` variant.
    pub fn install_replicated_deactivate(&self, username: &str) {
        let mut users = self.users.write().unwrap_or_else(|p| p.into_inner());
        if let Some(record) = users.get_mut(username) {
            record.is_active = false;
        }
    }
}
