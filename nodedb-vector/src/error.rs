//! Vector engine error types.

/// Errors from vector index operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VectorError {
    #[error("vector dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("unsupported HNSW checkpoint version {found}; expected {expected}")]
    UnsupportedVersion { found: u8, expected: u8 },
    #[error("invalid PQ codec magic bytes")]
    InvalidMagic,
    #[error("PQ codec deserialization failed: {0}")]
    DeserializationFailed(String),
}
