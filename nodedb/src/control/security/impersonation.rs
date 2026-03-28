//! Impersonation & delegation: admin acts as user, user delegates to another.
//!
//! Impersonation: `IMPERSONATE AUTH USER 'x'` — admin assumes user's identity.
//! Audit trail shows "admin_alice impersonating user_123". Time-limited (default 1h).
//!
//! Delegation: `DELEGATE AUTH USER 'b' AS AUTH USER 'a' SCOPES '...'` — user 'a'
//! grants user 'b' a subset of their scopes for a limited time.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::info;

/// An active impersonation session.
#[derive(Debug, Clone)]
pub struct Impersonation {
    /// Admin who initiated the impersonation.
    pub admin_user_id: String,
    pub admin_username: String,
    /// User being impersonated.
    pub target_user_id: String,
    pub target_username: String,
    /// When impersonation started.
    pub started_at: u64,
    /// When impersonation expires (auto-stop).
    pub expires_at: u64,
}

/// A delegation grant: user A delegates scopes to user B.
#[derive(Debug, Clone)]
pub struct Delegation {
    /// User granting the delegation.
    pub delegator_user_id: String,
    /// User receiving the delegation.
    pub delegate_user_id: String,
    /// Scopes delegated (must be subset of delegator's scopes).
    pub scopes: Vec<String>,
    /// When delegation expires.
    pub expires_at: u64,
    /// Reason for delegation (audit trail).
    pub reason: String,
    pub created_at: u64,
}

/// Impersonation and delegation store.
pub struct ImpersonationStore {
    /// admin_user_id → active impersonation.
    active_impersonations: RwLock<HashMap<String, Impersonation>>,
    /// "{delegator}:{delegate}" → delegation.
    delegations: RwLock<HashMap<String, Delegation>>,
    /// Default impersonation timeout in seconds.
    default_timeout_secs: u64,
}

impl ImpersonationStore {
    pub fn new(default_timeout_secs: u64) -> Self {
        Self {
            active_impersonations: RwLock::new(HashMap::new()),
            delegations: RwLock::new(HashMap::new()),
            default_timeout_secs,
        }
    }

    /// Start impersonating a user. Returns `Err` if already impersonating.
    pub fn start_impersonation(
        &self,
        admin_user_id: &str,
        admin_username: &str,
        target_user_id: &str,
        target_username: &str,
    ) -> crate::Result<()> {
        let now = now_secs();
        let imp = Impersonation {
            admin_user_id: admin_user_id.into(),
            admin_username: admin_username.into(),
            target_user_id: target_user_id.into(),
            target_username: target_username.into(),
            started_at: now,
            expires_at: now + self.default_timeout_secs,
        };

        let mut imps = self
            .active_impersonations
            .write()
            .unwrap_or_else(|p| p.into_inner());
        if imps.contains_key(admin_user_id) {
            return Err(crate::Error::BadRequest {
                detail: "already impersonating another user — stop first".into(),
            });
        }
        info!(
            admin = %admin_username,
            target = %target_username,
            timeout_secs = self.default_timeout_secs,
            "impersonation started"
        );
        imps.insert(admin_user_id.into(), imp);
        Ok(())
    }

    /// Stop impersonation.
    pub fn stop_impersonation(&self, admin_user_id: &str) -> bool {
        let mut imps = self
            .active_impersonations
            .write()
            .unwrap_or_else(|p| p.into_inner());
        imps.remove(admin_user_id).is_some()
    }

    /// Get active impersonation for an admin (if any and not expired).
    pub fn get_impersonation(&self, admin_user_id: &str) -> Option<Impersonation> {
        let imps = self
            .active_impersonations
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let imp = imps.get(admin_user_id)?;
        if now_secs() >= imp.expires_at {
            return None; // Expired.
        }
        Some(imp.clone())
    }

    /// Create a delegation.
    pub fn delegate(
        &self,
        delegator_user_id: &str,
        delegate_user_id: &str,
        scopes: Vec<String>,
        expires_secs: u64,
        reason: &str,
    ) -> crate::Result<()> {
        let now = now_secs();
        let deleg = Delegation {
            delegator_user_id: delegator_user_id.into(),
            delegate_user_id: delegate_user_id.into(),
            scopes,
            expires_at: now + expires_secs,
            reason: reason.into(),
            created_at: now,
        };

        let key = format!("{delegator_user_id}:{delegate_user_id}");
        let mut delegations = self.delegations.write().unwrap_or_else(|p| p.into_inner());
        delegations.insert(key, deleg);
        info!(
            delegator = %delegator_user_id,
            delegate = %delegate_user_id,
            "delegation created"
        );
        Ok(())
    }

    /// Revoke a delegation.
    pub fn revoke_delegation(&self, delegator_user_id: &str, delegate_user_id: &str) -> bool {
        let key = format!("{delegator_user_id}:{delegate_user_id}");
        let mut delegations = self.delegations.write().unwrap_or_else(|p| p.into_inner());
        delegations.remove(&key).is_some()
    }

    /// Get effective delegated scopes for a user.
    pub fn delegated_scopes(&self, delegate_user_id: &str) -> Vec<String> {
        let now = now_secs();
        let delegations = self.delegations.read().unwrap_or_else(|p| p.into_inner());
        let mut all_scopes = Vec::new();
        for deleg in delegations.values() {
            if deleg.delegate_user_id == delegate_user_id && now < deleg.expires_at {
                all_scopes.extend(deleg.scopes.iter().cloned());
            }
        }
        all_scopes
    }

    /// List all active delegations.
    pub fn list_delegations(&self) -> Vec<Delegation> {
        let now = now_secs();
        let delegations = self.delegations.read().unwrap_or_else(|p| p.into_inner());
        delegations
            .values()
            .filter(|d| now < d.expires_at)
            .cloned()
            .collect()
    }
}

impl Default for ImpersonationStore {
    fn default() -> Self {
        Self::new(3600) // 1 hour default timeout.
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn impersonation_lifecycle() {
        let store = ImpersonationStore::new(3600);
        store
            .start_impersonation("admin_1", "alice_admin", "user_42", "bob")
            .unwrap();

        let imp = store.get_impersonation("admin_1").unwrap();
        assert_eq!(imp.target_username, "bob");
        assert_eq!(imp.admin_username, "alice_admin");

        // Can't impersonate twice.
        assert!(
            store
                .start_impersonation("admin_1", "alice_admin", "user_99", "carol")
                .is_err()
        );

        store.stop_impersonation("admin_1");
        assert!(store.get_impersonation("admin_1").is_none());
    }

    #[test]
    fn delegation_lifecycle() {
        let store = ImpersonationStore::new(3600);
        store
            .delegate(
                "user_a",
                "user_b",
                vec!["profile:read".into()],
                3600,
                "vacation cover",
            )
            .unwrap();

        let scopes = store.delegated_scopes("user_b");
        assert_eq!(scopes, vec!["profile:read"]);

        store.revoke_delegation("user_a", "user_b");
        assert!(store.delegated_scopes("user_b").is_empty());
    }

    #[test]
    fn delegation_list() {
        let store = ImpersonationStore::new(3600);
        store
            .delegate("a", "b", vec!["s1".into()], 3600, "r1")
            .unwrap();
        store
            .delegate("c", "d", vec!["s2".into()], 3600, "r2")
            .unwrap();

        assert_eq!(store.list_delegations().len(), 2);
    }
}
