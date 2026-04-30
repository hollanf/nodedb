//! `KeyProvider` trait — the unwrap/rotate abstraction over all KMS backends.

use zeroize::Zeroizing;

use crate::Result;

/// Abstraction over key unwrapping and rotation for all KMS backends.
///
/// Implementations must hold no plaintext key bytes beyond the scope of
/// `unwrap_key`. The returned `Zeroizing` wrapper ensures the key bytes
/// are wiped from memory when the caller drops the value.
#[async_trait::async_trait]
pub trait KeyProvider {
    /// Unwrap/load the plaintext 32-byte encryption key.
    ///
    /// Called once at startup. The bytes are wrapped in `Zeroizing` so they
    /// are automatically wiped when the caller drops the value. The caller
    /// should pass them immediately to `nodedb_wal::secure_mem::SecureKey`
    /// for mlock protection.
    async fn unwrap_key(&self) -> Result<Zeroizing<[u8; 32]>>;

    /// Rotate to a new key. The semantics depend on the backend:
    ///
    /// - `File`: re-read the file from disk (key file has been updated externally).
    /// - `Vault`: call `rewrap/{key_name}` with the old ciphertext blob, persist
    ///   the new ciphertext blob, and return the new plaintext key.
    /// - `AwsKms`: encrypt a new random DEK with KMS `Encrypt`, persist the new
    ///   ciphertext blob, and return the new plaintext key.
    async fn rotate(&self) -> Result<Zeroizing<[u8; 32]>>;
}
