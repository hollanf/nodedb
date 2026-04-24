//! `WalRecord` — header + payload with encryption + checksum helpers.

use super::header::{
    ENCRYPTED_FLAG, HEADER_SIZE, MAX_WAL_PAYLOAD_SIZE, RecordHeader, WAL_FORMAT_VERSION, WAL_MAGIC,
};
use crate::error::{Result, WalError};

/// A complete WAL record: header + payload.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub header: RecordHeader,
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Create a new WAL record with computed CRC32C.
    ///
    /// If `encryption_key` is provided, the payload is encrypted before
    /// CRC computation. The ciphertext includes a 16-byte auth tag.
    pub fn new(
        record_type: u16,
        lsn: u64,
        tenant_id: u32,
        vshard_id: u16,
        payload: Vec<u8>,
        encryption_key: Option<&crate::crypto::WalEncryptionKey>,
    ) -> Result<Self> {
        if payload.len() > MAX_WAL_PAYLOAD_SIZE {
            return Err(WalError::PayloadTooLarge {
                size: payload.len(),
                max: MAX_WAL_PAYLOAD_SIZE,
            });
        }

        let (final_payload, encrypted) = if let Some(key) = encryption_key {
            let temp_header = RecordHeader {
                magic: WAL_MAGIC,
                format_version: WAL_FORMAT_VERSION,
                record_type,
                lsn,
                tenant_id,
                vshard_id,
                payload_len: 0,
                crc32c: 0,
            };
            let header_bytes = temp_header.to_bytes();
            let ciphertext = key.encrypt(lsn, &header_bytes, &payload)?;
            (ciphertext, true)
        } else {
            (payload, false)
        };

        let record_type = if encrypted {
            record_type | ENCRYPTED_FLAG
        } else {
            record_type
        };

        let mut header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            payload_len: final_payload.len() as u32,
            crc32c: 0,
        };

        header.crc32c = header.compute_checksum(&final_payload);

        Ok(Self {
            header,
            payload: final_payload,
        })
    }

    /// Decrypt the payload if the record is encrypted.
    pub fn decrypt_payload(
        &self,
        epoch: &[u8; 4],
        encryption_key: Option<&crate::crypto::WalEncryptionKey>,
    ) -> Result<Vec<u8>> {
        if !self.is_encrypted() {
            return Ok(self.payload.clone());
        }

        let key = encryption_key.ok_or_else(|| WalError::EncryptionError {
            detail: "record is encrypted but no decryption key provided".into(),
        })?;

        let mut aad_header = self.header;
        aad_header.record_type &= !ENCRYPTED_FLAG;
        aad_header.payload_len = 0;
        aad_header.crc32c = 0;
        let header_bytes = aad_header.to_bytes();

        key.decrypt(epoch, self.header.lsn, &header_bytes, &self.payload)
    }

    /// Decrypt the payload using a key ring (supports dual-key rotation).
    pub fn decrypt_payload_ring(
        &self,
        epoch: &[u8; 4],
        ring: Option<&crate::crypto::KeyRing>,
    ) -> Result<Vec<u8>> {
        if !self.is_encrypted() {
            return Ok(self.payload.clone());
        }

        let ring = ring.ok_or_else(|| WalError::EncryptionError {
            detail: "record is encrypted but no decryption key ring provided".into(),
        })?;

        let mut aad_header = self.header;
        aad_header.record_type &= !ENCRYPTED_FLAG;
        aad_header.payload_len = 0;
        aad_header.crc32c = 0;
        let header_bytes = aad_header.to_bytes();

        ring.decrypt(epoch, self.header.lsn, &header_bytes, &self.payload)
    }

    /// Whether this record's payload is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.header.record_type & ENCRYPTED_FLAG != 0
    }

    /// Logical record type with the encryption flag stripped.
    pub fn logical_record_type(&self) -> u16 {
        self.header.record_type & !ENCRYPTED_FLAG
    }

    /// Verify the CRC32C checksum.
    pub fn verify_checksum(&self) -> Result<()> {
        let expected = self.header.crc32c;
        let actual = self.header.compute_checksum(&self.payload);
        if expected != actual {
            return Err(WalError::ChecksumMismatch {
                lsn: self.header.lsn,
                expected,
                actual,
            });
        }
        Ok(())
    }

    /// Total size on disk: header + payload.
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::RecordType;
    use super::*;

    #[test]
    fn checksum_roundtrip() {
        let payload = b"hello nodedb";
        let record =
            WalRecord::new(RecordType::Put as u16, 1, 0, 0, payload.to_vec(), None).unwrap();
        record.verify_checksum().unwrap();
    }

    #[test]
    fn checksum_detects_corruption() {
        let payload = b"hello nodedb";
        let mut record =
            WalRecord::new(RecordType::Put as u16, 1, 0, 0, payload.to_vec(), None).unwrap();
        record.payload[0] ^= 0xFF;
        assert!(matches!(
            record.verify_checksum(),
            Err(WalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn payload_too_large_rejected() {
        let big_payload = vec![0u8; MAX_WAL_PAYLOAD_SIZE + 1];
        assert!(matches!(
            WalRecord::new(RecordType::Put as u16, 1, 0, 0, big_payload, None),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn anchor_payload_in_record() {
        use super::super::anchor::LsnMsAnchorPayload;
        let anchor = LsnMsAnchorPayload::new(42, 1_700_000_000_000);
        let record = WalRecord::new(
            RecordType::LsnMsAnchor as u16,
            42,
            0,
            0,
            anchor.to_bytes().to_vec(),
            None,
        )
        .unwrap();
        record.verify_checksum().unwrap();
        assert_eq!(record.logical_record_type(), RecordType::LsnMsAnchor as u16);
        let decoded = LsnMsAnchorPayload::from_bytes(&record.payload).unwrap();
        assert_eq!(decoded, anchor);
    }
}
