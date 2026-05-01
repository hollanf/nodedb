//! AES-256-GCM SEGC envelope for columnar segment at-rest encryption.
//!
//! Plaintext segments start with the 4-byte magic `NDBS`. Encrypted segments
//! use the `SEGC` envelope from [`nodedb_wal::crypto`]; `NDBS` and `SEGC`
//! never collide, so sniffing the first 4 bytes unambiguously distinguishes
//! the two formats.
//!
//! See [`nodedb_wal::crypto::encrypt_segment_envelope`] for the framing.

use nodedb_wal::crypto::{WalEncryptionKey, decrypt_segment_envelope, encrypt_segment_envelope};

use crate::error::ColumnarError;

/// Magic bytes identifying an encrypted columnar segment.
///
/// Distinct from `NDBS` (plaintext segments), `SEGV` (vector checkpoints),
/// `WALP` (WAL segments), and `RKSPT\0` (spatial checkpoints).
pub(crate) const SEGC_MAGIC: [u8; 4] = *b"SEGC";

/// Encrypt `plaintext` (a complete plaintext segment buffer) and return the
/// SEGC envelope.
pub(crate) fn encrypt_segment(
    key: &WalEncryptionKey,
    plaintext: &[u8],
) -> Result<Vec<u8>, ColumnarError> {
    encrypt_segment_envelope(key, &SEGC_MAGIC, plaintext)
        .map_err(|e| ColumnarError::EncryptionFailed(e.to_string()))
}

/// Decrypt an encrypted columnar segment blob (starting at byte 0 = `SEGC`).
///
/// Returns the inner plaintext segment bytes (starting with `NDBS`).
pub(crate) fn decrypt_segment(
    key: &WalEncryptionKey,
    blob: &[u8],
) -> Result<Vec<u8>, ColumnarError> {
    decrypt_segment_envelope(key, &SEGC_MAGIC, blob)
        .map_err(|e| ColumnarError::DecryptionFailed(e.to_string()))
}
