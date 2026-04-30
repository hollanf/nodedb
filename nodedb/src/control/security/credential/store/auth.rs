//! Authentication lookups: password verification, SCRAM credential
//! exports, identity-building.

use super::super::super::identity::{AuthMethod, AuthenticatedIdentity};
use super::super::super::time::now_secs;
use super::super::hash::{hash_password_argon2, verify_argon2};
use super::super::record::UserRecord;
use super::core::{CredentialStore, read_lock};

/// Result of a `get_scram_credentials` call, carrying an optional warning
/// string when the login is allowed but the password has entered the grace period.
pub struct ScramCredentials {
    pub salt: Vec<u8>,
    pub salted_password: Vec<u8>,
    /// Non-empty when the account is in expiry grace period or `must_change_password`
    /// is set (login allowed, but the client should be told to change their password).
    pub warning: Option<String>,
}

impl CredentialStore {
    /// Look up a user by username. Returns None if not found or
    /// inactive.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let users = read_lock(&self.users).ok()?;
        users.get(username).filter(|u| u.is_active).cloned()
    }

    /// Get the SCRAM salt and salted password for pgwire SCRAM auth.
    ///
    /// Returns `None` for service accounts, unknown users, or hard-expired
    /// accounts (past expiry with no grace period). Returns `Some` with a
    /// non-empty warning when in grace period or `must_change_password` is set.
    pub fn get_scram_credentials(&self, username: &str) -> Option<ScramCredentials> {
        let users = read_lock(&self.users).ok()?;
        let u = users
            .get(username)
            .filter(|u| u.is_active && !u.is_service_account)?;

        let now = now_secs();
        let grace_secs = self.password_expiry_grace_days as u64 * 86400;

        // Expired with no grace: hard block.
        if u.password_expires_at > 0
            && now >= u.password_expires_at
            && (grace_secs == 0 || now >= u.password_expires_at + grace_secs)
        {
            tracing::warn!(username = u.username, "password expired, login denied");
            return None;
        }

        // must_change_password with no grace: hard block.
        if u.must_change_password && grace_secs == 0 {
            tracing::warn!(
                username = u.username,
                "must_change_password set with no grace period, login denied"
            );
            return None;
        }

        // Compute warning if in grace period or must_change_password is set.
        let warning = if u.must_change_password {
            Some("password change required: please change your password".to_string())
        } else if u.password_expires_at > 0
            && now >= u.password_expires_at
            && grace_secs > 0
            && now < u.password_expires_at + grace_secs
        {
            let days_left = (u.password_expires_at + grace_secs).saturating_sub(now) / 86400 + 1;
            Some(format!(
                "password expired: grace period ends in {days_left} day(s), please change your password"
            ))
        } else {
            None
        };

        Some(ScramCredentials {
            salt: u.scram_salt.clone(),
            salted_password: u.scram_salted_password.clone(),
            warning,
        })
    }

    /// Verify a cleartext password against the stored Argon2 hash.
    ///
    /// Also enforces `password_expires_at` and `must_change_password`
    /// (same policy as `get_scram_credentials`) so that all auth paths
    /// honour the expiry policy.
    ///
    /// Returns `(verified, warning)` where `warning` is non-empty when
    /// the login is permitted but the password is in grace period or
    /// `must_change_password` is set.
    pub fn verify_password_with_status(
        &self,
        username: &str,
        password: &str,
    ) -> (bool, Option<String>) {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => {
                let _ = hash_password_argon2(password);
                return (false, None);
            }
        };
        let record = match users.get(username).filter(|u| u.is_active) {
            Some(r) => r,
            None => {
                let _ = hash_password_argon2(password);
                return (false, None);
            }
        };

        // Constant-time hash check always runs to prevent timing oracle.
        let hash_ok = verify_argon2(&record.password_hash, password);

        let now = now_secs();
        let grace_secs = self.password_expiry_grace_days as u64 * 86400;

        // Expired past grace: deny regardless of correct password.
        if record.password_expires_at > 0
            && now >= record.password_expires_at
            && (grace_secs == 0 || now >= record.password_expires_at + grace_secs)
        {
            tracing::warn!(username, "password expired, login denied");
            return (false, None);
        }

        // must_change_password with no grace: deny.
        if record.must_change_password && grace_secs == 0 {
            tracing::warn!(username, "must_change_password set, login denied");
            return (false, None);
        }

        if !hash_ok {
            return (false, None);
        }

        // Login allowed — compute warning.
        let warning = if record.must_change_password {
            Some("password change required: please change your password".to_string())
        } else if record.password_expires_at > 0
            && now >= record.password_expires_at
            && grace_secs > 0
            && now < record.password_expires_at + grace_secs
        {
            let days_left =
                (record.password_expires_at + grace_secs).saturating_sub(now) / 86400 + 1;
            Some(format!(
                "password expired: grace period ends in {days_left} day(s), please change your password"
            ))
        } else {
            None
        };

        (true, warning)
    }

    /// Verify a cleartext password. Convenience wrapper; ignores warning.
    /// Internal paths that need the warning should call `verify_password_with_status`.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        self.verify_password_with_status(username, password).0
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
