pub mod aws_kms;
pub mod file;
pub mod vault;

pub use aws_kms::AwsKmsProvider;
pub use file::FileKeyProvider;
pub use vault::VaultKeyProvider;

use std::path::PathBuf;

use crate::Result;

/// How the server obtains its 32-byte AES-256 encryption key.
///
/// All variants implement `KeyProvider` and load the plaintext key into
/// mlocked memory via `nodedb_wal::secure_mem`. No variant returns a stub.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KeyDerivation {
    /// Load a raw 32-byte key from a local file.
    ///
    /// The file must contain exactly 32 bytes. This is the simplest mode,
    /// suitable for development or environments with an external secret manager
    /// that delivers the key file.
    File {
        /// Path to the 32-byte key file.
        key_path: PathBuf,
    },

    /// Unwrap the encryption key via HashiCorp Vault transit engine.
    ///
    /// ## Chosen flow
    ///
    /// On first init the operator calls `vault write transit/datakey/plaintext/<key_name>`
    /// and stores the returned `ciphertext` blob in `<data_dir>/.wal-key.wrapped`.
    ///
    /// At startup NodeDB reads the wrapped blob from disk, sends it to
    /// `POST {addr}/v1/{mount}/decrypt/{key_name}` with the Vault token,
    /// and extracts the `plaintext` field (base64 of 32 bytes) as the DEK.
    ///
    /// Key rotation: call `POST {addr}/v1/{mount}/rewrap/{key_name}` with
    /// the old ciphertext blob and write the new ciphertext to `.wal-key.wrapped`.
    Vault {
        /// Vault server address, e.g. `https://vault.example.com:8200`.
        addr: String,
        /// Path to a file containing the Vault token.
        token_path: PathBuf,
        /// Name of the transit key in Vault.
        key_name: String,
        /// Mount path of the transit engine, e.g. `transit`.
        mount: String,
        /// Path to the wrapped DEK ciphertext blob stored on disk.
        /// Default: `<data_dir>/.wal-key.wrapped`
        ciphertext_blob_path: PathBuf,
    },

    /// Unwrap the encryption key via AWS KMS.
    ///
    /// ## Chosen flow
    ///
    /// On first init the operator encrypts the plaintext DEK with
    /// `aws kms encrypt --key-id <key_id> --plaintext fileb://<32-byte-file>`
    /// and stores the ciphertext blob at `ciphertext_blob_path`.
    ///
    /// At startup NodeDB reads the ciphertext blob, calls KMS `Decrypt`,
    /// and holds the plaintext 32-byte key in mlocked memory.
    ///
    /// Key rotation: generate a new random DEK, call KMS `Encrypt`, write
    /// the new ciphertext blob, then call `rotate()` on the provider to
    /// get the new key.
    AwsKms {
        /// AWS KMS key ID or ARN.
        key_id: String,
        /// AWS region, e.g. `us-east-1`.
        region: String,
        /// Path to the KMS-encrypted DEK ciphertext blob.
        ciphertext_blob_path: PathBuf,
    },
}

impl KeyDerivation {
    /// Build a boxed `KeyProvider` for this derivation config.
    ///
    /// This is called once at startup. The returned provider's `unwrap_key()`
    /// is then called to obtain the 32-byte plaintext key.
    pub async fn into_provider(self) -> Result<Box<dyn KeyProvider + Send + Sync>> {
        match self {
            KeyDerivation::File { key_path } => Ok(Box::new(FileKeyProvider { key_path })),
            KeyDerivation::Vault {
                addr,
                token_path,
                key_name,
                mount,
                ciphertext_blob_path,
            } => Ok(Box::new(VaultKeyProvider::new(
                addr,
                token_path,
                key_name,
                mount,
                ciphertext_blob_path,
            ))),
            KeyDerivation::AwsKms {
                key_id,
                region,
                ciphertext_blob_path,
            } => Ok(Box::new(
                AwsKmsProvider::new(key_id, region, ciphertext_blob_path).await?,
            )),
        }
    }
}

/// Abstraction over key unwrapping and rotation for all KMS backends.
///
/// Implementations must hold no plaintext key bytes beyond the scope of
/// `unwrap_key`. The caller is responsible for mlock'ing the returned key.
#[async_trait::async_trait]
pub trait KeyProvider {
    /// Unwrap/load the plaintext 32-byte encryption key.
    ///
    /// Called once at startup. The returned bytes should be treated as secret
    /// — the caller passes them to `nodedb_wal::secure_mem::SecureKey`.
    async fn unwrap_key(&self) -> Result<[u8; 32]>;

    /// Rotate to a new key. The semantics depend on the backend:
    ///
    /// - `File`: re-read the file from disk (key file has been updated externally).
    /// - `Vault`: call `rewrap/{key_name}` with the old ciphertext blob, persist
    ///   the new ciphertext blob, and return the new plaintext key.
    /// - `AwsKms`: encrypt a new random DEK with KMS `Encrypt`, persist the new
    ///   ciphertext blob, and return the new plaintext key.
    async fn rotate(&self) -> Result<[u8; 32]>;
}
