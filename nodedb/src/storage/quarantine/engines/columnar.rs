//! Quarantine wrapper for columnar segment reads.
//!
//! All production `nodedb_columnar::SegmentReader::open` call sites that can
//! serve user queries go through `open_segment_with_quarantine`. Sites that
//! only run during compaction or testing bypass this wrapper intentionally —
//! they either tolerate open failures or run in controlled conditions.

use std::sync::Arc;

use nodedb_columnar::ColumnarError;

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::registry::{QuarantineEngine, QuarantineRegistry, SegmentKey};

/// Attempt to open a columnar segment, routing CRC-class errors through the
/// quarantine registry.
///
/// Returns `Ok(reader)` on success, `Err(QuarantineError::SegmentQuarantined)`
/// on the second consecutive failure, or `Err` wrapping a `ColumnarError` for
/// non-CRC failures that should be surfaced normally.
pub fn open_segment_with_quarantine<'a>(
    registry: &Arc<QuarantineRegistry>,
    seg_bytes: &'a [u8],
    collection: &str,
    segment_id: &str,
) -> Result<nodedb_columnar::reader::SegmentReader<'a>, ColumnarOrQuarantine> {
    match nodedb_columnar::SegmentReader::open(seg_bytes) {
        Ok(reader) => {
            let key = SegmentKey {
                engine: QuarantineEngine::Columnar,
                collection: collection.to_string(),
                segment_id: segment_id.to_string(),
            };
            registry.record_success(&key);
            Ok(reader)
        }
        Err(e) if is_crc_class(&e) => {
            let key = SegmentKey {
                engine: QuarantineEngine::Columnar,
                collection: collection.to_string(),
                segment_id: segment_id.to_string(),
            };
            // record_failure returns Ok(()) for the first strike (caller should retry),
            // Err(SegmentQuarantined) for the second.
            registry
                .record_failure(key, &e.to_string(), None)
                .map_err(ColumnarOrQuarantine::Quarantined)?;
            // First strike: propagate the original error so the caller can retry.
            Err(ColumnarOrQuarantine::Columnar(e))
        }
        Err(e) => Err(ColumnarOrQuarantine::Columnar(e)),
    }
}

/// Error type returned by `open_segment_with_quarantine`.
#[derive(Debug, thiserror::Error)]
pub enum ColumnarOrQuarantine {
    #[error(transparent)]
    Columnar(#[from] ColumnarError),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}

/// Returns `true` for the error variants that indicate data corruption
/// (CRC failure, truncation, invalid magic) as opposed to transient I/O or
/// schema mismatches.
fn is_crc_class(e: &ColumnarError) -> bool {
    matches!(
        e,
        ColumnarError::FooterCrcMismatch { .. }
            | ColumnarError::Corruption { .. }
            | ColumnarError::TruncatedSegment { .. }
            | ColumnarError::InvalidMagic(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bytes_triggers_quarantine_path() {
        let reg = Arc::new(QuarantineRegistry::new());
        // Empty bytes → TruncatedSegment (a CRC-class error). First call
        // returns a ColumnarError (first strike); second returns Quarantined.
        let r1 = open_segment_with_quarantine(&reg, &[], "coll", "seg1");
        assert!(matches!(r1, Err(ColumnarOrQuarantine::Columnar(_))));

        let r2 = open_segment_with_quarantine(&reg, &[], "coll", "seg1");
        assert!(matches!(r2, Err(ColumnarOrQuarantine::Quarantined(_))));
    }
}
