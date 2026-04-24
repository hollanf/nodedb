//! WAL record header: fixed 30-byte prefix + constants.

use crate::error::{Result, WalError};

/// Magic number identifying a NodeDB WAL record.
pub const WAL_MAGIC: u32 = 0x5359_4E57; // "SYNW"

/// Current WAL format version.
///
/// v2 introduces bitemporal record layout: `LsnMsAnchor` records (type 102)
/// provide stable LSN↔wall-clock interpolation, and engine-level writers emit
/// `system_from_ms` in versioned keys. Pre-release — no v1 readers supported.
pub const WAL_FORMAT_VERSION: u16 = 2;

/// Maximum WAL record payload size (64 MiB). Distinct from cluster RPC's limit.
pub const MAX_WAL_PAYLOAD_SIZE: usize = 64 * 1024 * 1024;

/// Size of the record header in bytes.
pub const HEADER_SIZE: usize = 30;

/// Bit 14 in record_type signals the payload is AES-256-GCM encrypted.
/// Separate from bit 15 (required flag).
pub const ENCRYPTED_FLAG: u16 = 0x4000;

/// WAL record header (fixed 30 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub magic: u32,
    pub format_version: u16,
    pub record_type: u16,
    pub lsn: u64,
    pub tenant_id: u32,
    pub vshard_id: u16,
    pub payload_len: u32,
    pub crc32c: u32,
}

impl RecordHeader {
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.record_type.to_le_bytes());
        buf[8..16].copy_from_slice(&self.lsn.to_le_bytes());
        buf[16..20].copy_from_slice(&self.tenant_id.to_le_bytes());
        buf[20..22].copy_from_slice(&self.vshard_id.to_le_bytes());
        buf[22..26].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[26..30].copy_from_slice(&self.crc32c.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
            format_version: u16::from_le_bytes([buf[4], buf[5]]),
            record_type: u16::from_le_bytes([buf[6], buf[7]]),
            lsn: u64::from_le_bytes([
                buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            ]),
            tenant_id: u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]),
            vshard_id: u16::from_le_bytes([buf[20], buf[21]]),
            payload_len: u32::from_le_bytes([buf[22], buf[23], buf[24], buf[25]]),
            crc32c: u32::from_le_bytes([buf[26], buf[27], buf[28], buf[29]]),
        }
    }

    /// CRC32C over header (excluding the crc32c field) + payload.
    pub fn compute_checksum(&self, payload: &[u8]) -> u32 {
        let header_bytes = self.to_bytes();
        let mut digest = crc32c::crc32c(&header_bytes[..HEADER_SIZE - 4]);
        digest = crc32c::crc32c_append(digest, payload);
        digest
    }

    /// Logical record type with the encryption flag stripped.
    pub fn logical_record_type(&self) -> u16 {
        self.record_type & !ENCRYPTED_FLAG
    }

    pub fn validate(&self, offset: u64) -> Result<()> {
        if self.magic != WAL_MAGIC {
            return Err(WalError::InvalidMagic {
                offset,
                expected: WAL_MAGIC,
                actual: self.magic,
            });
        }
        if self.format_version > WAL_FORMAT_VERSION {
            return Err(WalError::UnsupportedVersion {
                version: self.format_version,
                supported: WAL_FORMAT_VERSION,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type: 1 | 0x8000,
            lsn: 42,
            tenant_id: 7,
            vshard_id: 3,
            payload_len: 100,
            crc32c: 0xDEAD_BEEF,
        };
        let bytes = header.to_bytes();
        assert_eq!(header, RecordHeader::from_bytes(&bytes));
    }

    #[test]
    fn invalid_magic_detected() {
        let header = RecordHeader {
            magic: 0xBAD0_F00D,
            format_version: WAL_FORMAT_VERSION,
            record_type: 0,
            lsn: 0,
            tenant_id: 0,
            vshard_id: 0,
            payload_len: 0,
            crc32c: 0,
        };
        assert!(matches!(
            header.validate(0),
            Err(WalError::InvalidMagic { .. })
        ));
    }

    #[test]
    fn unsupported_version_detected() {
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION + 1,
            record_type: 0,
            lsn: 0,
            tenant_id: 0,
            vshard_id: 0,
            payload_len: 0,
            crc32c: 0,
        };
        assert!(matches!(
            header.validate(0),
            Err(WalError::UnsupportedVersion { .. })
        ));
    }
}
