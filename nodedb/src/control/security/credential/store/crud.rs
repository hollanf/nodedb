//! User CRUD operations: create, deactivate, update password/roles.

use crate::types::TenantId;

use super::super::super::identity::Role;
use super::super::super::time::now_secs;
use super::super::hash::{
    compute_scram_salted_password, generate_scram_salt, hash_password_argon2,
};
use super::super::record::UserRecord;
use super::core::{CredentialStore, write_lock};

impl CredentialStore {
    /// Create a new user. Returns the user_id.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<u64> {
        let mut users = write_lock(&self.users)?;
        if users.contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' already exists"),
            });
        }

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;
        let user_id = self.alloc_user_id()?;

        let is_superuser = roles.contains(&Role::Superuser);
        let now = now_secs();
        let mut record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id,
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles,
            is_superuser,
            is_active: true,
            is_service_account: false,
            created_at: now,
            updated_at: now,
            password_expires_at: self.compute_expiry(),
            must_change_password: false,
            password_changed_at: now,
        };

        self.persist_user(&mut record)?;
        users.insert(username.to_string(), record);
        Ok(user_id)
    }

    /// Create a service account. No password — can only authenticate
    /// via API keys. Returns the user_id.
    pub fn create_service_account(
        &self,
        name: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<u64> {
        let mut users = write_lock(&self.users)?;
        if users.contains_key(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("user or service account '{name}' already exists"),
            });
        }

        let user_id = self.alloc_user_id()?;
        let is_superuser = roles.contains(&Role::Superuser);
        let now = now_secs();
        let mut record = UserRecord {
            user_id,
            username: name.to_string(),
            tenant_id,
            password_hash: String::new(),
            scram_salt: Vec::new(),
            scram_salted_password: Vec::new(),
            roles,
            is_superuser,
            is_active: true,
            is_service_account: true,
            created_at: now,
            updated_at: now,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: now,
        };

        self.persist_user(&mut record)?;
        users.insert(name.to_string(), record);
        Ok(user_id)
    }

    /// Deactivate a user (soft delete). Persists the change.
    pub fn deactivate_user(&self, username: &str) -> crate::Result<bool> {
        let mut users = write_lock(&self.users)?;
        if let Some(record) = users.get_mut(username) {
            record.is_active = false;
            self.persist_user(record)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Update a user's password. Recomputes both Argon2 hash and
    /// SCRAM credentials.
    pub fn update_password(&self, username: &str, password: &str) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        let salt = generate_scram_salt();
        record.scram_salted_password = compute_scram_salted_password(password, &salt);
        record.scram_salt = salt;
        record.password_hash = hash_password_argon2(password)?;
        record.password_expires_at = self.compute_expiry();
        record.must_change_password = false;
        record.password_changed_at = now_secs();
        self.persist_user(record)?;
        Ok(())
    }

    /// Mark a user as requiring a password change on next login.
    pub fn set_must_change_password(&self, username: &str, required: bool) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        record.must_change_password = required;
        self.persist_user(record)?;
        Ok(())
    }

    /// Set password expiry to 0 (never expires) for a user.
    pub fn set_password_never_expires(&self, username: &str) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        record.password_expires_at = 0;
        self.persist_user(record)?;
        Ok(())
    }

    /// Set a specific password expiry timestamp for a user.
    pub fn set_password_expires_at(&self, username: &str, expires_at: u64) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        record.password_expires_at = expires_at;
        self.persist_user(record)?;
        Ok(())
    }

    /// Replace all roles for a user.
    pub fn update_roles(&self, username: &str, roles: Vec<Role>) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        record.is_superuser = roles.contains(&Role::Superuser);
        record.roles = roles;
        self.persist_user(record)?;
        Ok(())
    }

    /// Add a role to a user (if not already present).
    pub fn add_role(&self, username: &str, role: Role) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.roles.contains(&role) {
            record.roles.push(role.clone());
            if matches!(role, Role::Superuser) {
                record.is_superuser = true;
            }
        }
        self.persist_user(record)?;
        Ok(())
    }

    /// Remove a role from a user.
    pub fn remove_role(&self, username: &str, role: &Role) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        record.roles.retain(|r| r != role);
        if matches!(role, Role::Superuser) {
            record.is_superuser = false;
        }
        self.persist_user(record)?;
        Ok(())
    }
}
