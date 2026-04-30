//! Authentication lookups: password verification, SCRAM credential
//! exports, identity-building.

use super::super::super::identity::{AuthMethod, AuthenticatedIdentity};
use super::super::super::time::now_secs;
use super::super::hash::{VerifyOutcome, hash_password_argon2, verify_argon2_with_rehash};
use super::super::record::UserRecord;
use super::core::{CredentialStore, read_lock, write_lock};

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
    /// On a successful match, transparently rehashes the stored password if
    /// the stored Argon2 parameters are strictly weaker than the configured
    /// ones.  Write-back failure is non-fatal (logged as a warning).  If the
    /// stored PHC string is unparseable the login is denied.
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
                // Timing oracle mitigation: run a dummy hash even on lock failure.
                let _ = hash_password_argon2(password, &self.argon2_config);
                return (false, None);
            }
        };
        let record = match users.get(username).filter(|u| u.is_active) {
            Some(r) => r,
            None => {
                // Timing oracle mitigation: run a dummy hash for unknown users.
                let _ = hash_password_argon2(password, &self.argon2_config);
                return (false, None);
            }
        };

        // Constant-time verify + rehash decision; runs before policy checks so
        // the timing profile is the same for expired and valid accounts.
        let stored_hash = record.password_hash.clone();
        let outcome = verify_argon2_with_rehash(&stored_hash, password, &self.argon2_config);

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

        // Evaluate verify outcome.
        let rehash_hash = match outcome {
            VerifyOutcome::Ok { rehash } => rehash,
            VerifyOutcome::WrongPassword => return (false, None),
            // Unparseable stored PHC is a data integrity error — deny login.
            VerifyOutcome::BadStoredHash => {
                tracing::error!(
                    username,
                    "stored password hash is not a valid PHC string; login denied"
                );
                return (false, None);
            }
        };

        // Drop read lock before acquiring write lock for rehash write-back.
        drop(users);

        // Perform write-back if a rehash was computed.
        if let Some(new_hash) = rehash_hash {
            self.apply_rehash(username, new_hash);
        }

        // Re-acquire read lock to compute the warning (record reference was dropped).
        let warning = self.compute_login_warning(username, now, grace_secs);

        (true, warning)
    }

    /// Write the new password hash back to the in-memory store and catalog.
    ///
    /// Failure is non-fatal: a warning is logged and login continues.
    fn apply_rehash(&self, username: &str, new_hash: String) {
        let mut users = match write_lock(&self.users) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    username,
                    error = %e,
                    "rehash write-back: could not acquire write lock; skipping"
                );
                return;
            }
        };
        let record = match users.get_mut(username) {
            Some(r) => r,
            None => {
                tracing::warn!(
                    username,
                    "rehash write-back: user vanished between read and write; skipping"
                );
                return;
            }
        };
        record.password_hash = new_hash;
        if let Err(e) = self.persist_user(record) {
            tracing::warn!(
                username,
                error = %e,
                "rehash write-back: catalog persist failed; in-memory hash updated, \
                 catalog will be reconciled on next password change"
            );
        } else {
            tracing::debug!(username, "password hash upgraded to current Argon2 params");
        }
    }

    /// Compute the login warning string (grace period / must_change_password).
    /// Re-reads the record under read lock; if the lock or user is unavailable,
    /// returns `None` (warning loss is acceptable compared to failing the login).
    fn compute_login_warning(&self, username: &str, now: u64, grace_secs: u64) -> Option<String> {
        let users = read_lock(&self.users).ok()?;
        let record = users.get(username)?;

        if record.must_change_password {
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
        }
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
