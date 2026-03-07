//! WAL record format.
//!
//! On-disk layout (all fields little-endian):
//!
//! ```text
//! ┌──────────┬─────────────────┬────────────┬─────┬───────────┬───────────┬─────────────┬─────────┐
//! │  magic   │ format_version  │ record_type│ lsn │ tenant_id │ vshard_id │ payload_len │ crc32c  │
//! │  4 bytes │    2 bytes      │   2 bytes  │ 8B  │   4 bytes │  2 bytes  │   4 bytes   │ 4 bytes │
//! └──────────┴─────────────────┴────────────┴─────┴───────────┴───────────┴─────────────┴─────────┘
//! Total header: 30 bytes
//! Followed by: [payload_len bytes of payload]
//! ```

use crate::error::{Result, WalError};

/// Magic number identifying a SynapseDB WAL record.
pub const WAL_MAGIC: u32 = 0x5359_4E57; // "SYNW" in ASCII

/// Current WAL format version.
pub const WAL_FORMAT_VERSION: u16 = 1;

/// Maximum payload size per record (64 MiB).
pub const MAX_PAYLOAD_SIZE: usize = 64 * 1024 * 1024;

/// Size of the record header in bytes.
pub const HEADER_SIZE: usize = 30;

/// WAL record header (fixed 30 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    /// Magic number (`WAL_MAGIC`).
    pub magic: u32,

    /// Format version for forward/backward compatibility.
    pub format_version: u16,

    /// Record type discriminant.
    pub record_type: u16,

    /// Log Sequence Number — monotonically increasing, globally unique.
    pub lsn: u64,

    /// Tenant ID for multi-tenant isolation.
    pub tenant_id: u32,

    /// Virtual shard ID for routing.
    pub vshard_id: u16,

    /// Length of the payload following this header.
    pub payload_len: u32,

    /// CRC32C of the header (excluding this field) + payload.
    pub crc32c: u32,
}

impl RecordHeader {
    /// Serialize the header to a byte buffer.
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

    /// Deserialize a header from a byte buffer.
    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            format_version: u16::from_le_bytes(buf[4..6].try_into().unwrap()),
            record_type: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            lsn: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            tenant_id: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            vshard_id: u16::from_le_bytes(buf[20..22].try_into().unwrap()),
            payload_len: u32::from_le_bytes(buf[22..26].try_into().unwrap()),
            crc32c: u32::from_le_bytes(buf[26..30].try_into().unwrap()),
        }
    }

    /// Compute the CRC32C over the header (excluding the crc32c field) + payload.
    pub fn compute_checksum(&self, payload: &[u8]) -> u32 {
        let header_bytes = self.to_bytes();
        // Hash everything except the last 4 bytes (the crc32c field itself).
        let mut digest = crc32c::crc32c(&header_bytes[..HEADER_SIZE - 4]);
        digest = crc32c::crc32c_append(digest, payload);
        digest
    }

    /// Validate this header's magic and version.
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

/// Record type discriminants.
///
/// Types 0-255 are reserved for synapseDB core.
/// Types 256+ are available for SynapseDBecific records.
///
/// Bit 15 (0x8000) marks a record as **required** — unknown required records
/// cause a replay failure. Unknown records without bit 15 set are safely skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RecordType {
    /// No-op / padding record (skipped during replay).
    Noop = 0,

    /// Generic key-value write.
    Put = 1 | 0x8000,

    /// Generic key deletion.
    Delete = 2 | 0x8000,

    /// Vector engine: insert/update embedding.
    VectorPut = 10 | 0x8000,

    /// CRDT engine: delta application.
    CrdtDelta = 20 | 0x8000,

    /// Timeseries engine: metric sample batch.
    TimeseriesBatch = 30,

    /// Timeseries engine: log entry batch.
    LogBatch = 31,

    /// Checkpoint marker — indicates a consistent snapshot point.
    Checkpoint = 100 | 0x8000,
}

impl RecordType {
    /// Whether this record type is required (must be understood for correct replay).
    pub fn is_required(raw: u16) -> bool {
        raw & 0x8000 != 0
    }

    /// Convert a raw u16 to a known RecordType, or None if unknown.
    pub fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            0 => Some(Self::Noop),
            x if x == 1 | 0x8000 => Some(Self::Put),
            x if x == 2 | 0x8000 => Some(Self::Delete),
            x if x == 10 | 0x8000 => Some(Self::VectorPut),
            x if x == 20 | 0x8000 => Some(Self::CrdtDelta),
            30 => Some(Self::TimeseriesBatch),
            31 => Some(Self::LogBatch),
            x if x == 100 | 0x8000 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

/// A complete WAL record: header + payload.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub header: RecordHeader,
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Create a new WAL record with computed CRC32C.
    pub fn new(
        record_type: u16,
        lsn: u64,
        tenant_id: u32,
        vshard_id: u16,
        payload: Vec<u8>,
    ) -> Result<Self> {
        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(WalError::PayloadTooLarge {
                size: payload.len(),
                max: MAX_PAYLOAD_SIZE,
            });
        }

        let mut header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            payload_len: payload.len() as u32,
            crc32c: 0, // computed below
        };

        header.crc32c = header.compute_checksum(&payload);

        Ok(Self { header, payload })
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
    use super::*;

    #[test]
    fn header_roundtrip() {
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type: RecordType::Put as u16,
            lsn: 42,
            tenant_id: 7,
            vshard_id: 3,
            payload_len: 100,
            crc32c: 0xDEAD_BEEF,
        };

        let bytes = header.to_bytes();
        let decoded = RecordHeader::from_bytes(&bytes);
        assert_eq!(header, decoded);
    }

    #[test]
    fn checksum_roundtrip() {
        let payload = b"hello synapsedb";
        let record = WalRecord::new(RecordType::Put as u16, 1, 0, 0, payload.to_vec()).unwrap();

        record.verify_checksum().unwrap();
    }

    #[test]
    fn checksum_detects_corruption() {
        let payload = b"hello synapsedb";
        let mut record = WalRecord::new(RecordType::Put as u16, 1, 0, 0, payload.to_vec()).unwrap();

        // Corrupt one byte.
        record.payload[0] ^= 0xFF;

        assert!(matches!(
            record.verify_checksum(),
            Err(WalError::ChecksumMismatch { .. })
        ));
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
    fn payload_too_large_rejected() {
        let big_payload = vec![0u8; MAX_PAYLOAD_SIZE + 1];
        assert!(matches!(
            WalRecord::new(RecordType::Put as u16, 1, 0, 0, big_payload),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn record_type_required_flag() {
        assert!(RecordType::is_required(RecordType::Put as u16));
        assert!(RecordType::is_required(RecordType::Delete as u16));
        assert!(RecordType::is_required(RecordType::Checkpoint as u16));
        assert!(!RecordType::is_required(RecordType::Noop as u16));
        assert!(!RecordType::is_required(RecordType::TimeseriesBatch as u16));
        assert!(!RecordType::is_required(RecordType::LogBatch as u16));
    }
}
