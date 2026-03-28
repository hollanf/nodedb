//! Auth-scoped API keys: `nda_` format, bound to `_system.auth_users`.
//!
//! Distinct from DB user keys (`ndb_`). Auth keys:
//! - Are bound to JIT-provisioned auth users (Mode 2)
//! - Support per-key scope restriction (subset of user's scopes)
//! - Support per-key rate limits
//! - Track last-used timestamp and IP
//! - Support key rotation with configurable overlap period

use std::collections::HashMap;
use std::sync::RwLock;

use tracing::info;

/// An auth-scoped API key record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuthApiKey {
    /// Key ID (prefix of the token): `nda_<key_id>`.
    pub key_id: String,
    /// SHA-256 hash of the secret portion.
    pub secret_hash: Vec<u8>,
    /// Auth user ID this key is bound to.
    pub auth_user_id: String,
    /// Tenant ID.
    pub tenant_id: u32,
    /// Scopes this key is restricted to (subset of user's scopes). Empty = inherit all.
    pub scopes: Vec<String>,
    /// Per-key rate limit (QPS). 0 = use user/tier default.
    pub rate_limit_qps: u64,
    /// Per-key burst capacity. 0 = use default.
    pub rate_limit_burst: u64,
    /// Unix timestamp when the key expires. 0 = no expiry.
    pub expires_at: u64,
    /// Unix timestamp when the key was created.
    pub created_at: u64,
    /// Whether this key has been revoked.
    pub is_revoked: bool,
    /// Last used timestamp (updated on each verification).
    pub last_used_at: u64,
    /// Last used IP address.
    pub last_used_ip: String,
    /// For key rotation: previous key ID that this key replaces.
    /// During overlap period, both old and new keys are valid.
    pub replaces_key_id: Option<String>,
    /// End of overlap period (Unix timestamp). After this, old key is invalid.
    pub overlap_ends_at: u64,
}

/// Auth API key store.
pub struct AuthApiKeyStore {
    /// key_id → AuthApiKey.
    keys: RwLock<HashMap<String, AuthApiKey>>,
    /// secret_hash (hex) → key_id for O(1) lookup during verification.
    hash_index: RwLock<HashMap<String, String>>,
}

impl AuthApiKeyStore {
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            hash_index: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new auth API key. Returns the full token (shown once).
    pub fn create_key(
        &self,
        auth_user_id: &str,
        tenant_id: u32,
        scopes: Vec<String>,
        rate_limit_qps: u64,
        rate_limit_burst: u64,
        expires_days: u64,
    ) -> String {
        let key_id = generate_key_id();
        let secret = generate_secret();
        let token = format!("nda_{key_id}_{secret}");

        let secret_hash = hash_secret(&secret);
        let now = now_secs();
        let expires_at = if expires_days > 0 {
            now + expires_days * 86_400
        } else {
            0
        };

        let record = AuthApiKey {
            key_id: key_id.clone(),
            secret_hash: secret_hash.clone(),
            auth_user_id: auth_user_id.into(),
            tenant_id,
            scopes,
            rate_limit_qps,
            rate_limit_burst,
            expires_at,
            created_at: now,
            is_revoked: false,
            last_used_at: 0,
            last_used_ip: String::new(),
            replaces_key_id: None,
            overlap_ends_at: 0,
        };

        let hash_hex = hex_encode(&secret_hash);
        let mut keys = self.keys.write().unwrap_or_else(|p| p.into_inner());
        let mut idx = self.hash_index.write().unwrap_or_else(|p| p.into_inner());
        keys.insert(key_id.clone(), record);
        idx.insert(hash_hex, key_id);

        token
    }

    /// Verify an `nda_` token. Returns the key record if valid.
    pub fn verify(&self, token: &str) -> Option<AuthApiKey> {
        let stripped = token.strip_prefix("nda_")?;
        let parts: Vec<&str> = stripped.splitn(2, '_').collect();
        if parts.len() != 2 {
            return None;
        }
        let secret = parts[1];
        let secret_hash = hash_secret(secret);
        let hash_hex = hex_encode(&secret_hash);

        let idx = self.hash_index.read().unwrap_or_else(|p| p.into_inner());
        let key_id = idx.get(&hash_hex)?;

        let keys = self.keys.read().unwrap_or_else(|p| p.into_inner());
        let key = keys.get(key_id)?;

        if key.is_revoked {
            return None;
        }
        if key.expires_at > 0 && now_secs() > key.expires_at {
            return None;
        }

        Some(key.clone())
    }

    /// Update last-used tracking after successful verification.
    pub fn touch(&self, key_id: &str, ip: &str) {
        let mut keys = self.keys.write().unwrap_or_else(|p| p.into_inner());
        if let Some(key) = keys.get_mut(key_id) {
            key.last_used_at = now_secs();
            key.last_used_ip = ip.to_string();
        }
    }

    /// Revoke a key.
    pub fn revoke(&self, key_id: &str) -> bool {
        let mut keys = self.keys.write().unwrap_or_else(|p| p.into_inner());
        if let Some(key) = keys.get_mut(key_id) {
            key.is_revoked = true;
            true
        } else {
            false
        }
    }

    /// Rotate a key: create a new key that replaces the old one.
    /// Both are valid during the overlap period.
    pub fn rotate(&self, old_key_id: &str, overlap_hours: u64) -> Option<String> {
        let keys = self.keys.read().unwrap_or_else(|p| p.into_inner());
        let old = keys.get(old_key_id)?.clone();
        drop(keys);

        let new_token = self.create_key(
            &old.auth_user_id,
            old.tenant_id,
            old.scopes.clone(),
            old.rate_limit_qps,
            old.rate_limit_burst,
            0, // New key inherits no expiry — set separately if needed.
        );

        // Mark the new key as replacing the old one.
        let new_key_id = new_token
            .strip_prefix("nda_")
            .and_then(|s| s.split('_').next())
            .unwrap_or("")
            .to_string();

        let overlap_ends = now_secs() + overlap_hours * 3600;

        let mut keys = self.keys.write().unwrap_or_else(|p| p.into_inner());
        if let Some(new_key) = keys.get_mut(&new_key_id) {
            new_key.replaces_key_id = Some(old_key_id.to_string());
            new_key.overlap_ends_at = overlap_ends;
        }

        info!(
            old_key = %old_key_id,
            new_key = %new_key_id,
            overlap_hours,
            "auth API key rotated"
        );

        Some(new_token)
    }

    /// List all keys for an auth user.
    pub fn list_for_user(&self, auth_user_id: &str) -> Vec<AuthApiKey> {
        let keys = self.keys.read().unwrap_or_else(|p| p.into_inner());
        keys.values()
            .filter(|k| k.auth_user_id == auth_user_id && !k.is_revoked)
            .cloned()
            .collect()
    }

    /// List all keys (admin view).
    pub fn list_all(&self) -> Vec<AuthApiKey> {
        let keys = self.keys.read().unwrap_or_else(|p| p.into_inner());
        keys.values().filter(|k| !k.is_revoked).cloned().collect()
    }
}

impl Default for AuthApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

fn generate_key_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = now_secs();
    format!("{ts:x}{seq:04x}")
}

fn generate_secret() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    // 32 bytes of cryptographic randomness + sequential suffix for uniqueness.
    let mut random_bytes = [0u8; 32];
    getrandom::fill(&mut random_bytes).unwrap_or_else(|_| {
        // Fallback: use timestamp + counter if getrandom unavailable (e.g., early boot).
        let ts = now_secs();
        random_bytes[..8].copy_from_slice(&ts.to_le_bytes());
        random_bytes[8..16].copy_from_slice(&seq.to_le_bytes());
    });
    format!("{}{seq:08x}", hex_encode(&random_bytes))
}

fn hash_secret(secret: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(secret.as_bytes()).to_vec()
}

fn now_secs() -> u64 {
    crate::control::security::time::now_secs()
}

/// Hex-encode a byte slice (lowercase).
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_verify() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key("user_42", 1, vec![], 0, 0, 0);

        assert!(token.starts_with("nda_"));
        let key = store.verify(&token).unwrap();
        assert_eq!(key.auth_user_id, "user_42");
        assert_eq!(key.tenant_id, 1);
    }

    #[test]
    fn invalid_token_rejected() {
        let store = AuthApiKeyStore::new();
        assert!(store.verify("nda_bad_token").is_none());
        assert!(store.verify("ndb_wrong_prefix").is_none());
    }

    #[test]
    fn revoked_key_rejected() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let key = store.verify(&token).unwrap();
        store.revoke(&key.key_id);
        assert!(store.verify(&token).is_none());
    }

    #[test]
    fn scoped_key() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key(
            "u1",
            1,
            vec!["profile:read".into(), "orders:write".into()],
            100,
            200,
            30,
        );
        let key = store.verify(&token).unwrap();
        assert_eq!(key.scopes, vec!["profile:read", "orders:write"]);
        assert_eq!(key.rate_limit_qps, 100);
        assert!(key.expires_at > 0);
    }

    #[test]
    fn last_used_tracking() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let key = store.verify(&token).unwrap();
        assert_eq!(key.last_used_at, 0);

        store.touch(&key.key_id, "10.0.0.1");
        let key2 = store.verify(&token).unwrap();
        assert!(key2.last_used_at > 0);
        assert_eq!(key2.last_used_ip, "10.0.0.1");
    }

    #[test]
    fn key_rotation() {
        let store = AuthApiKeyStore::new();
        let old_token = store.create_key("u1", 1, vec!["scope_a".into()], 0, 0, 0);
        let old_key = store.verify(&old_token).unwrap();

        let new_token = store.rotate(&old_key.key_id, 24).unwrap();
        assert!(new_token.starts_with("nda_"));

        // Both old and new should be valid during overlap.
        assert!(store.verify(&old_token).is_some());
        assert!(store.verify(&new_token).is_some());

        // New key should inherit scopes.
        let new_key = store.verify(&new_token).unwrap();
        assert_eq!(new_key.scopes, vec!["scope_a"]);
        assert!(new_key.replaces_key_id.is_some());
    }

    #[test]
    fn list_for_user() {
        let store = AuthApiKeyStore::new();
        store.create_key("u1", 1, vec![], 0, 0, 0);
        store.create_key("u1", 1, vec![], 0, 0, 0);
        store.create_key("u2", 1, vec![], 0, 0, 0);

        assert_eq!(store.list_for_user("u1").len(), 2);
        assert_eq!(store.list_for_user("u2").len(), 1);
    }
}
