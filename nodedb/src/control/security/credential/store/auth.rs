//! Authentication lookups: password verification, SCRAM credential
//! exports, identity-building.

use super::super::super::identity::{AuthMethod, AuthenticatedIdentity};
use super::super::super::time::now_secs;
use super::super::hash::{hash_password_argon2, verify_argon2};
use super::super::record::UserRecord;
use super::core::{CredentialStore, read_lock};

impl CredentialStore {
    /// Look up a user by username. Returns None if not found or
    /// inactive.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let users = read_lock(&self.users).ok()?;
        users.get(username).filter(|u| u.is_active).cloned()
    }

    /// Get the SCRAM salt and salted password for pgwire SCRAM
    /// auth. Returns None for service accounts (no pgwire login)
    /// or expired passwords.
    pub fn get_scram_credentials(&self, username: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        let users = read_lock(&self.users).ok()?;
        users
            .get(username)
            .filter(|u| u.is_active && !u.is_service_account)
            .filter(|u| {
                if u.password_expires_at > 0 && now_secs() >= u.password_expires_at {
                    tracing::warn!(username = u.username, "password expired, login denied");
                    return false;
                }
                true
            })
            .map(|u| (u.scram_salt.clone(), u.scram_salted_password.clone()))
    }

    /// Verify a cleartext password against the stored Argon2 hash.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => {
                let _ = hash_password_argon2(password);
                return false;
            }
        };
        match users.get(username).filter(|u| u.is_active) {
            Some(record) => verify_argon2(&record.password_hash, password),
            None => {
                let _ = hash_password_argon2(password);
                false
            }
        }
    }

    /// Build an `AuthenticatedIdentity` for a verified user.
    pub fn to_identity(&self, username: &str, method: AuthMethod) -> Option<AuthenticatedIdentity> {
        self.get_user(username).map(|record| AuthenticatedIdentity {
            user_id: record.user_id,
            username: record.username,
            tenant_id: record.tenant_id,
            auth_method: method,
            roles: record.roles,
            is_superuser: record.is_superuser,
        })
    }
}
