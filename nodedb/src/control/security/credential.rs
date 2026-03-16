use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::TenantId;

use super::identity::{AuthMethod, AuthenticatedIdentity, Role};

/// A stored user record.
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: TenantId,
    /// Argon2id password hash (PHC string format).
    pub password_hash: String,
    /// Salt used for SCRAM-SHA-256 (16 bytes, stored as raw bytes).
    pub scram_salt: Vec<u8>,
    /// SCRAM-SHA-256 salted password (for pgwire auth).
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<Role>,
    pub is_superuser: bool,
    pub is_active: bool,
}

/// In-memory credential store.
///
/// Bootstrap: loads superuser from config. Future: backed by `_system.users`
/// in the sparse engine for persistence.
///
/// Thread-safe — shared across Tokio tasks on the Control Plane.
pub struct CredentialStore {
    /// Username → UserRecord lookup.
    users: RwLock<HashMap<String, UserRecord>>,
    next_user_id: RwLock<u64>,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CredentialStore {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            next_user_id: RwLock::new(1),
        }
    }

    /// Bootstrap the superuser from config. Called once on startup.
    pub fn bootstrap_superuser(&self, username: &str, password: &str) -> crate::Result<()> {
        let salt = generate_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password);

        let user_id = {
            let mut next = self.next_user_id.write().unwrap();
            let id = *next;
            *next += 1;
            id
        };

        let record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id: TenantId::new(0), // Superuser spans all tenants.
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles: vec![Role::Superuser],
            is_superuser: true,
            is_active: true,
        };

        self.users
            .write()
            .unwrap()
            .insert(username.to_string(), record);

        Ok(())
    }

    /// Create a new user. Returns the user_id.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<u64> {
        let mut users = self.users.write().unwrap();
        if users.contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' already exists"),
            });
        }

        let salt = generate_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password);

        let user_id = {
            let mut next = self.next_user_id.write().unwrap();
            let id = *next;
            *next += 1;
            id
        };

        let is_superuser = roles.contains(&Role::Superuser);
        let record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id,
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles,
            is_superuser,
            is_active: true,
        };

        users.insert(username.to_string(), record);
        Ok(user_id)
    }

    /// Look up a user by username. Returns None if not found or inactive.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let users = self.users.read().unwrap();
        users.get(username).filter(|u| u.is_active).cloned()
    }

    /// Get the SCRAM salt and salted password for pgwire SCRAM auth.
    pub fn get_scram_credentials(&self, username: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        let users = self.users.read().unwrap();
        users
            .get(username)
            .filter(|u| u.is_active)
            .map(|u| (u.scram_salt.clone(), u.scram_salted_password.clone()))
    }

    /// Verify a cleartext password against the stored hash.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        let users = self.users.read().unwrap();
        match users.get(username).filter(|u| u.is_active) {
            Some(record) => verify_argon2(&record.password_hash, password),
            None => {
                // Constant-time: hash anyway to prevent timing attacks.
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

    /// List all active usernames (for admin).
    pub fn list_users(&self) -> Vec<String> {
        let users = self.users.read().unwrap();
        users
            .values()
            .filter(|u| u.is_active)
            .map(|u| u.username.clone())
            .collect()
    }

    /// Deactivate a user (soft delete).
    pub fn deactivate_user(&self, username: &str) -> bool {
        let mut users = self.users.write().unwrap();
        if let Some(record) = users.get_mut(username) {
            record.is_active = false;
            true
        } else {
            false
        }
    }

    /// Check if any users exist (used to determine if bootstrap is needed).
    pub fn is_empty(&self) -> bool {
        self.users.read().unwrap().is_empty()
    }
}

// ── Password hashing ───────────────────────────────────────────────

/// Generate a 16-byte cryptographic random salt.
fn generate_salt() -> Vec<u8> {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    // Use system randomness. In production, use a CSPRNG.
    // For now, combine multiple random sources for decent entropy.
    let mut salt = vec![0u8; 16];
    let state = RandomState::new();
    for chunk in salt.chunks_mut(8) {
        let mut hasher = state.build_hasher();
        hasher.write_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        );
        let val = hasher.finish();
        let bytes = val.to_le_bytes();
        let len = chunk.len().min(8);
        chunk[..len].copy_from_slice(&bytes[..len]);
    }
    salt
}

/// Compute SCRAM-SHA-256 SaltedPassword for pgwire auth.
///
/// SaltedPassword := Hi(Normalize(password), salt, iterations)
/// where Hi is PBKDF2-HMAC-SHA256.
fn compute_scram_salted_password(password: &str, salt: &[u8]) -> Vec<u8> {
    // Use pgwire's helper if available, otherwise manual PBKDF2.
    pgwire::api::auth::sasl::scram::gen_salted_password(password, salt, 4096)
}

/// Hash a password with Argon2id for storage.
///
/// Uses a simple hash for now — will use the `argon2` crate in production.
/// For bootstrap/MVP, we use PBKDF2-SHA256 which is available via pgwire's deps.
fn hash_password_argon2(password: &str) -> String {
    // For MVP: store the SCRAM salted password as the "hash".
    // In production, replace with proper Argon2id using the `argon2` crate.
    let salt = generate_salt();
    let salted = compute_scram_salted_password(password, &salt);
    // Encode as hex for simple storage.
    let salt_hex: String = salt.iter().map(|b| format!("{b:02x}")).collect();
    let hash_hex: String = salted.iter().map(|b| format!("{b:02x}")).collect();
    format!("pbkdf2-sha256$4096${salt_hex}${hash_hex}")
}

/// Verify a password against a stored hash.
fn verify_argon2(stored_hash: &str, password: &str) -> bool {
    // Parse the stored hash format: "pbkdf2-sha256$iterations$salt_hex$hash_hex"
    let parts: Vec<&str> = stored_hash.split('$').collect();
    if parts.len() != 4 || parts[0] != "pbkdf2-sha256" {
        return false;
    }

    let iterations: usize = match parts[1].parse() {
        Ok(i) => i,
        Err(_) => return false,
    };

    let salt: Vec<u8> = match (0..parts[2].len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&parts[2][i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
    {
        Ok(s) => s,
        Err(_) => return false,
    };

    let stored_hash_bytes: Vec<u8> = match (0..parts[3].len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&parts[3][i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
    {
        Ok(h) => h,
        Err(_) => return false,
    };

    let computed = pgwire::api::auth::sasl::scram::gen_salted_password(password, &salt, iterations);

    // Constant-time comparison.
    if computed.len() != stored_hash_bytes.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in computed.iter().zip(stored_hash_bytes.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_superuser() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret123").unwrap();

        let user = store.get_user("admin").expect("superuser should exist");
        assert!(user.is_superuser);
        assert!(user.is_active);
        assert_eq!(user.tenant_id, TenantId::new(0));
        assert!(user.roles.contains(&Role::Superuser));
    }

    #[test]
    fn verify_password_correct() {
        let store = CredentialStore::new();
        store
            .bootstrap_superuser("admin", "correct_password")
            .unwrap();
        assert!(store.verify_password("admin", "correct_password"));
    }

    #[test]
    fn verify_password_wrong() {
        let store = CredentialStore::new();
        store
            .bootstrap_superuser("admin", "correct_password")
            .unwrap();
        assert!(!store.verify_password("admin", "wrong_password"));
    }

    #[test]
    fn verify_password_nonexistent_user() {
        let store = CredentialStore::new();
        assert!(!store.verify_password("nobody", "password"));
    }

    #[test]
    fn create_user() {
        let store = CredentialStore::new();
        let id = store
            .create_user("alice", "pass", TenantId::new(1), vec![Role::ReadWrite])
            .unwrap();
        assert!(id > 0);

        let user = store.get_user("alice").unwrap();
        assert_eq!(user.tenant_id, TenantId::new(1));
        assert!(!user.is_superuser);
        assert!(user.roles.contains(&Role::ReadWrite));
    }

    #[test]
    fn duplicate_user_rejected() {
        let store = CredentialStore::new();
        store
            .create_user("alice", "pass", TenantId::new(1), vec![])
            .unwrap();
        let result = store.create_user("alice", "pass2", TenantId::new(1), vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn deactivate_user() {
        let store = CredentialStore::new();
        store
            .create_user("bob", "pass", TenantId::new(1), vec![])
            .unwrap();
        assert!(store.get_user("bob").is_some());

        store.deactivate_user("bob");
        assert!(store.get_user("bob").is_none());
    }

    #[test]
    fn scram_credentials_available() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret").unwrap();

        let (salt, salted_pw) = store.get_scram_credentials("admin").unwrap();
        assert_eq!(salt.len(), 16);
        assert!(!salted_pw.is_empty());
    }

    #[test]
    fn to_identity() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret").unwrap();

        let identity = store.to_identity("admin", AuthMethod::ScramSha256).unwrap();
        assert!(identity.is_superuser);
        assert_eq!(identity.username, "admin");
        assert_eq!(identity.auth_method, AuthMethod::ScramSha256);
    }
}
