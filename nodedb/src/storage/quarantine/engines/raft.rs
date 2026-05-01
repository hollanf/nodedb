//! Quarantine wrapper for Raft snapshot chunk decoding.
//!
//! Snapshot chunks are in-memory (no on-disk file path), so the rename step
//! is skipped. The quarantine record is still maintained so operators can
//! observe via `/v1/cluster/debug/quarantined-segments` that a Raft peer is
//! repeatedly sending corrupt snapshot data.
//!
//! The "collection" for Raft snapshot entries is `"_raft_snapshot"`, and the
//! "segment_id" is the chunk's `engine_id` label combined with the
//! `last_included_index` (e.g. `"columnar:12345"`).

use std::sync::Arc;

use nodedb_raft::snapshot_framing::{SnapshotFramingError, decode_snapshot_chunk};

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::registry::{QuarantineEngine, QuarantineRegistry, SegmentKey};

/// Attempt to decode a snapshot chunk, routing CRC-class errors through the
/// quarantine registry.
///
/// `snapshot_label` is a short human-readable identifier for the snapshot
/// (e.g. `"group=1:index=12345"`) used as `segment_id` in the registry.
pub fn decode_snapshot_chunk_with_quarantine<'a>(
    registry: &Arc<QuarantineRegistry>,
    data: &'a [u8],
    snapshot_label: &str,
) -> Result<(nodedb_raft::snapshot_framing::SnapshotEngineId, &'a [u8]), RaftOrQuarantine> {
    match decode_snapshot_chunk(data) {
        Ok(result) => {
            let key = SegmentKey {
                engine: QuarantineEngine::Raft,
                collection: "_raft_snapshot".to_string(),
                segment_id: snapshot_label.to_string(),
            };
            registry.record_success(&key);
            Ok(result)
        }
        Err(e) if is_crc_class(&e) => {
            let key = SegmentKey {
                engine: QuarantineEngine::Raft,
                collection: "_raft_snapshot".to_string(),
                segment_id: snapshot_label.to_string(),
            };
            registry
                .record_failure(key, &e.to_string(), None)
                .map_err(RaftOrQuarantine::Quarantined)?;
            Err(RaftOrQuarantine::Framing(e))
        }
        Err(e) => Err(RaftOrQuarantine::Framing(e)),
    }
}

/// Error type returned by `decode_snapshot_chunk_with_quarantine`.
#[derive(Debug, thiserror::Error)]
pub enum RaftOrQuarantine {
    #[error(transparent)]
    Framing(#[from] SnapshotFramingError),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}

fn is_crc_class(e: &SnapshotFramingError) -> bool {
    matches!(
        e,
        SnapshotFramingError::CrcMismatch { .. } | SnapshotFramingError::Truncated(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_data_triggers_quarantine_path() {
        let reg = Arc::new(QuarantineRegistry::new());
        // Empty → Truncated (CRC-class).
        let r1 = decode_snapshot_chunk_with_quarantine(&reg, &[], "group=1:index=0");
        assert!(matches!(r1, Err(RaftOrQuarantine::Framing(_))));

        let r2 = decode_snapshot_chunk_with_quarantine(&reg, &[], "group=1:index=0");
        assert!(matches!(r2, Err(RaftOrQuarantine::Quarantined(_))));
    }
}
