//! Quarantine wrapper for vector NDVS segment reads.
//!
//! `MmapVectorSegment::open` / `open_with_policy` return `std::io::Error`
//! with `ErrorKind::InvalidData` for CRC, magic, and truncation failures.
//! We treat all `InvalidData` errors as CRC-class because the only source
//! of `InvalidData` in the NDVS reader is structural corruption (magic,
//! version, size mismatch, CRC32C mismatch).

use std::path::Path;
use std::sync::Arc;

use nodedb_vector::mmap_segment::{MmapVectorSegment, VectorSegmentDropPolicy};

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::registry::{QuarantineEngine, QuarantineRegistry, SegmentKey};

/// Attempt to open a vector NDVS segment, routing `InvalidData` errors
/// through the quarantine registry.
pub fn open_vector_segment_with_quarantine(
    registry: &Arc<QuarantineRegistry>,
    path: &Path,
    policy: VectorSegmentDropPolicy,
    collection: &str,
    segment_id: &str,
) -> Result<MmapVectorSegment, VectorOrQuarantine> {
    match MmapVectorSegment::open_with_policy(path, policy) {
        Ok(seg) => {
            let key = SegmentKey {
                engine: QuarantineEngine::Vector,
                collection: collection.to_string(),
                segment_id: segment_id.to_string(),
            };
            registry.record_success(&key);
            Ok(seg)
        }
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            let key = SegmentKey {
                engine: QuarantineEngine::Vector,
                collection: collection.to_string(),
                segment_id: segment_id.to_string(),
            };
            // Provide the actual file path for rename on second strike.
            let path_for_rename = if path.exists() { Some(path) } else { None };
            registry
                .record_failure(key, &e.to_string(), path_for_rename)
                .map_err(VectorOrQuarantine::Quarantined)?;
            Err(VectorOrQuarantine::Io(e))
        }
        Err(e) => Err(VectorOrQuarantine::Io(e)),
    }
}

/// Error type returned by `open_vector_segment_with_quarantine`.
#[derive(Debug, thiserror::Error)]
pub enum VectorOrQuarantine {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}
