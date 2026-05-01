//! AES-256-GCM SEGA envelope for array segment at-rest encryption.
//!
//! Plaintext segments start with the 4-byte magic `NDAS` (the existing
//! `SegmentHeader` magic). Encrypted segments use the `SEGA` envelope from
//! [`nodedb_wal::crypto`]. `NDAS` and `SEGA` never collide, so sniffing the
//! first 4 bytes unambiguously distinguishes the two formats.
//!
//! See [`nodedb_wal::crypto::encrypt_segment_envelope`] for the framing.

use nodedb_wal::crypto::{WalEncryptionKey, decrypt_segment_envelope, encrypt_segment_envelope};

use crate::error::ArrayError;

/// Magic bytes identifying an encrypted array segment.
///
/// Distinct from `NDAS` (plaintext), `SEGV` (vector), `SEGC` (columnar),
/// `WALP` (WAL pages), and `RKSPT\0` (spatial).
pub(crate) const SEGA_MAGIC: [u8; 4] = *b"SEGA";

/// Plaintext segment magic (first 4 bytes of `HEADER_MAGIC`).
const NDAS_MAGIC: [u8; 4] = *b"NDAS";

/// Encrypt `plaintext` (a complete plaintext segment buffer) and return the
/// SEGA envelope.
pub(crate) fn encrypt_segment(
    key: &WalEncryptionKey,
    plaintext: &[u8],
) -> Result<Vec<u8>, ArrayError> {
    encrypt_segment_envelope(key, &SEGA_MAGIC, plaintext).map_err(|e| {
        ArrayError::EncryptionFailed {
            detail: e.to_string(),
        }
    })
}

/// Decrypt an encrypted array segment blob (starting at byte 0 = `SEGA`).
///
/// Returns the inner plaintext segment bytes (starting with `NDAS`).
pub(crate) fn decrypt_segment(key: &WalEncryptionKey, blob: &[u8]) -> Result<Vec<u8>, ArrayError> {
    decrypt_segment_envelope(key, &SEGA_MAGIC, blob).map_err(|e| ArrayError::DecryptionFailed {
        detail: e.to_string(),
    })
}

/// Sniff the first 4 bytes to determine if `blob` is a plaintext (`NDAS`)
/// or encrypted (`SEGA`) segment. Returns `true` if encrypted.
///
/// Returns an error if the magic is neither `NDAS` nor `SEGA`.
pub(crate) fn detect_encryption(blob: &[u8]) -> Result<bool, ArrayError> {
    if blob.len() < 4 {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("segment too short to sniff magic: {} bytes", blob.len()),
        });
    }
    let magic = &blob[..4];
    if magic == NDAS_MAGIC {
        Ok(false)
    } else if magic == SEGA_MAGIC {
        Ok(true)
    } else {
        Err(ArrayError::SegmentCorruption {
            detail: format!(
                "unrecognised segment magic: {magic:02x?} \
                 (expected NDAS or SEGA)"
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_wal::crypto::SEGMENT_ENVELOPE_PREAMBLE_SIZE;

    use super::*;

    fn test_kek() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    #[test]
    fn array_segment_encrypt_decrypt_roundtrip_g13d() {
        let kek = test_kek();
        let plaintext = b"NDAS\0\0\0\x01some-tile-data-padding-padding-padding";
        let encrypted = encrypt_segment(&kek, plaintext).unwrap();
        assert_eq!(&encrypted[..4], b"SEGA");
        let decrypted = decrypt_segment(&kek, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn array_segment_detect_encryption_ndas_g13d() {
        let blob = b"NDAS rest of bytes here";
        assert!(!detect_encryption(blob).unwrap());
    }

    #[test]
    fn array_segment_detect_encryption_sega_g13d() {
        let blob = b"SEGA rest of bytes here";
        assert!(detect_encryption(blob).unwrap());
    }

    #[test]
    fn array_segment_detect_encryption_unknown_magic_g13d() {
        let blob = b"XXXX rest of bytes here";
        assert!(detect_encryption(blob).is_err());
    }

    #[test]
    fn array_segment_tampered_ciphertext_rejected_g13d() {
        let kek = test_kek();
        let plaintext = b"NDAS\0\0\0\x01some-payload-bytes-padding-padding-extra";
        let mut encrypted = encrypt_segment(&kek, plaintext).unwrap();
        // Flip a byte in the ciphertext body (after preamble).
        encrypted[SEGMENT_ENVELOPE_PREAMBLE_SIZE + 3] ^= 0xFF;
        assert!(decrypt_segment(&kek, &encrypted).is_err());
    }
}
