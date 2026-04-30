use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Encryption at rest settings (WAL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionSettings {
    /// Path to the 32-byte AES-256-GCM key file used to encrypt WAL records.
    /// Generate with: `head -c 32 /dev/urandom > /etc/nodedb/keys/wal.key`
    pub key_path: PathBuf,
}

/// Per-backup encryption settings.
///
/// When present, each backup envelope is encrypted with a per-backup DEK
/// that is itself wrapped by this backup KEK (AES-256-GCM). The backup KEK
/// MUST be distinct from the WAL KEK — a shared key is detected at startup
/// and surfaced as a [`tracing::warn!`].
///
/// Configure via the `[backup_encryption]` TOML section:
/// ```toml
/// [backup_encryption]
/// key_path = "/etc/nodedb/keys/backup.key"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupEncryptionSettings {
    /// Path to the 32-byte AES-256-GCM key file used to wrap per-backup DEKs.
    /// Generate with: `head -c 32 /dev/urandom > /etc/nodedb/keys/backup.key`
    pub key_path: PathBuf,
}

/// Client-facing TLS settings (distinct from inter-node mTLS in mtls.rs).
///
/// When this section is present, TLS is enabled for all protocols by default.
/// Use per-protocol flags to selectively disable TLS on individual listeners.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSettings {
    /// Path to server certificate (PEM).
    pub cert_path: PathBuf,
    /// Path to server private key (PEM).
    pub key_path: PathBuf,
    /// Certificate hot-reload check interval (seconds). The server watches
    /// cert/key files for mtime changes and atomically swaps the TLS config.
    /// Default: 3600 (1 hour). Set to 0 to disable hot rotation.
    #[serde(default)]
    pub cert_reload_interval_secs: Option<u64>,

    /// Enable TLS on the native protocol listener (port 6433). Default: true.
    #[serde(default = "default_true")]
    pub native: bool,
    /// Enable TLS on the pgwire listener (port 6432). Default: true.
    #[serde(default = "default_true")]
    pub pgwire: bool,
    /// Enable TLS on the HTTP listener (port 6480). Default: true.
    #[serde(default = "default_true")]
    pub http: bool,
    /// Enable TLS on the RESP listener (port 6381). Default: true.
    #[serde(default = "default_true")]
    pub resp: bool,
    /// Enable TLS on the ILP listener (port 8086). Default: true.
    #[serde(default = "default_true")]
    pub ilp: bool,
}

fn default_true() -> bool {
    true
}
