//! Login lockout enforcement.

use std::time::Instant;

use crate::types::TenantId;

use super::store::{CredentialStore, read_lock, write_lock};

/// Tracks failed login attempts for lockout enforcement.
#[derive(Debug, Clone)]
pub(super) struct LoginAttemptTracker {
    /// Number of consecutive failed attempts.
    pub(super) failed_count: u32,
    /// When the lockout expires (if locked out).
    pub(super) locked_until: Option<Instant>,
}

impl CredentialStore {
    /// Configure lockout policy, password expiry and grace period.
    /// Called after construction from server config.
    pub fn set_lockout_policy(
        &mut self,
        max_failed: u32,
        lockout_secs: u64,
        password_expiry_days: u32,
    ) {
        self.set_lockout_policy_with_grace(max_failed, lockout_secs, password_expiry_days, 0);
    }

    /// Configure lockout policy with all expiry knobs.
    pub fn set_lockout_policy_with_grace(
        &mut self,
        max_failed: u32,
        lockout_secs: u64,
        password_expiry_days: u32,
        password_expiry_grace_days: u32,
    ) {
        self.max_failed_logins = max_failed;
        self.lockout_duration = std::time::Duration::from_secs(lockout_secs);
        self.password_expiry_secs = if password_expiry_days > 0 {
            password_expiry_days as u64 * 86400
        } else {
            0
        };
        self.password_expiry_grace_days = password_expiry_grace_days;
    }

    /// Check if a user is currently locked out.
    pub fn check_lockout(&self, username: &str) -> crate::Result<()> {
        if self.max_failed_logins == 0 {
            return Ok(());
        }

        let attempts = match read_lock(&self.login_attempts) {
            Ok(a) => a,
            Err(_) => {
                tracing::error!(
                    "login_attempts lock poisoned in check_lockout, allowing access as fallback"
                );
                return Ok(());
            }
        };

        if let Some(tracker) = attempts.get(username)
            && let Some(locked_until) = tracker.locked_until
            && Instant::now() < locked_until
        {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: TenantId::new(0),
                resource: format!(
                    "user '{username}' is locked out ({} failed attempts)",
                    tracker.failed_count
                ),
            });
        }

        Ok(())
    }

    /// Record a failed login attempt. May trigger lockout.
    pub fn record_login_failure(&self, username: &str) {
        if self.max_failed_logins == 0 {
            return;
        }

        let mut attempts = match write_lock(&self.login_attempts) {
            Ok(a) => a,
            Err(_) => {
                tracing::error!("login_attempts lock poisoned in record_login_failure");
                return;
            }
        };

        let tracker = attempts
            .entry(username.to_string())
            .or_insert(LoginAttemptTracker {
                failed_count: 0,
                locked_until: None,
            });

        tracker.failed_count += 1;

        if tracker.failed_count >= self.max_failed_logins {
            tracker.locked_until = Some(Instant::now() + self.lockout_duration);
            tracing::warn!(
                username,
                failed_count = tracker.failed_count,
                lockout_secs = self.lockout_duration.as_secs(),
                "user locked out due to failed login attempts"
            );
        }
    }

    /// Reset failed login counter on successful authentication.
    pub fn record_login_success(&self, username: &str) {
        if self.max_failed_logins == 0 {
            return;
        }

        let mut attempts = match write_lock(&self.login_attempts) {
            Ok(a) => a,
            Err(_) => {
                tracing::error!("login_attempts lock poisoned in record_login_success");
                return;
            }
        };

        attempts.remove(username);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lockout_after_threshold() {
        let mut store = CredentialStore::new();
        store.set_lockout_policy(3, 300, 0);

        store.record_login_failure("alice");
        store.record_login_failure("alice");
        assert!(store.check_lockout("alice").is_ok());

        store.record_login_failure("alice");
        assert!(store.check_lockout("alice").is_err());
    }

    #[test]
    fn login_success_resets_counter() {
        let mut store = CredentialStore::new();
        store.set_lockout_policy(3, 300, 0);

        store.record_login_failure("bob");
        store.record_login_failure("bob");
        store.record_login_success("bob");
        store.record_login_failure("bob");
        assert!(store.check_lockout("bob").is_ok());
    }

    #[test]
    fn lockout_disabled_when_zero() {
        let store = CredentialStore::new();
        // max_failed_logins = 0 means disabled
        for _ in 0..100 {
            store.record_login_failure("charlie");
        }
        assert!(store.check_lockout("charlie").is_ok());
    }
}
