//! AES-256-GCM SEGT envelope for timeseries columnar segment files.
//!
//! Plaintext segment files (`.col`, `.sym`, `schema.json`, `sparse_index.bin`,
//! `partition.meta`) have no existing magic header, so a file that starts
//! with `SEGT` is encrypted; any other first-four-byte pattern is plaintext.
//!
//! See [`nodedb_wal::crypto::encrypt_segment_envelope`] for the framing.

use nodedb_wal::crypto::{WalEncryptionKey, decrypt_segment_envelope, encrypt_segment_envelope};

use super::error::SegmentError;

/// Magic bytes identifying an encrypted columnar segment file.
pub const SEGT_MAGIC: [u8; 4] = *b"SEGT";

/// Encrypt `plaintext` (a raw file body) and return the SEGT envelope.
pub fn encrypt_file(key: &WalEncryptionKey, plaintext: &[u8]) -> Result<Vec<u8>, SegmentError> {
    encrypt_segment_envelope(key, &SEGT_MAGIC, plaintext)
        .map_err(|e| SegmentError::EncryptionFailed(e.to_string()))
}

/// Decrypt an encrypted columnar segment file blob (first bytes = `SEGT`).
pub fn decrypt_file(key: &WalEncryptionKey, blob: &[u8]) -> Result<Vec<u8>, SegmentError> {
    decrypt_segment_envelope(key, &SEGT_MAGIC, blob)
        .map_err(|e| SegmentError::DecryptionFailed(e.to_string()))
}

/// Sniff the first 4 bytes of a file to detect whether it is encrypted.
///
/// Returns `true` if encrypted (`SEGT`), `false` if plaintext (anything else).
/// Returns `Err` if the blob is too short to sniff.
pub fn is_encrypted(blob: &[u8]) -> Result<bool, SegmentError> {
    if blob.len() < 4 {
        return Err(SegmentError::Corrupt(
            "file too short to sniff magic".into(),
        ));
    }
    Ok(blob[..4] == SEGT_MAGIC)
}
