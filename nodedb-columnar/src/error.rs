//! Error types for columnar segment operations.

/// Errors from columnar segment encoding, decoding, and validation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ColumnarError {
    #[error("codec error: {0}")]
    Codec(#[from] nodedb_codec::CodecError),

    #[error("segment too short: expected at least {expected} bytes, got {got}")]
    TruncatedSegment { expected: usize, got: usize },

    #[error("invalid magic bytes: expected NDBS, got {0:?}")]
    InvalidMagic([u8; 4]),

    #[error(
        "incompatible segment version: reader {reader_major}.x, segment {segment_major}.{segment_minor}"
    )]
    IncompatibleVersion {
        reader_major: u8,
        segment_major: u8,
        segment_minor: u8,
    },

    #[error("footer CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    FooterCrcMismatch { stored: u32, computed: u32 },

    #[error("column index {index} out of range (segment has {count} columns)")]
    ColumnOutOfRange { index: usize, count: usize },

    #[error("schema mismatch: expected {expected} columns, memtable has {got}")]
    SchemaMismatch { expected: usize, got: usize },

    #[error("memtable is empty — nothing to flush")]
    EmptyMemtable,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("value type mismatch for column '{column}': expected {expected}")]
    TypeMismatch { column: String, expected: String },

    #[error("null violation: column '{0}' is NOT NULL")]
    NullViolation(String),

    #[error("duplicate primary key")]
    DuplicatePrimaryKey,

    #[error("primary key not found")]
    PrimaryKeyNotFound,

    #[error("segment ID space exhausted: u64::MAX segments have been allocated")]
    SegmentIdExhausted,

    /// Segment is encrypted (starts with `SEGV`) but no KEK was supplied.
    #[error(
        "columnar segment is encrypted but no encryption key was provided; \
         cannot read an encrypted segment without a key"
    )]
    MissingKek,

    /// Segment is plaintext (`NDBS`) but a KEK is configured (policy violation).
    #[error(
        "columnar segment is plaintext but an encryption key is configured; \
         refusing to load an unencrypted segment when encryption is required"
    )]
    KekRequired,

    /// AES-256-GCM encryption of the segment payload failed.
    #[error("columnar segment encryption failed: {0}")]
    EncryptionFailed(String),

    /// AES-256-GCM decryption of the segment payload failed.
    #[error("columnar segment decryption failed: {0}")]
    DecryptionFailed(String),
}
