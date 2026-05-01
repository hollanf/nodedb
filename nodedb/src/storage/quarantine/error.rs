//! Typed errors for the segment quarantine subsystem.

/// Errors produced by the quarantine registry.
#[derive(Debug, thiserror::Error, Clone)]
pub enum QuarantineError {
    /// The requested segment has been quarantined after two consecutive CRC
    /// failures. All subsequent reads of this segment return this error until
    /// the quarantine file is removed by an operator and the process restarts.
    #[error(
        "segment quarantined: engine={engine} collection={collection} segment_id={segment_id} \
         quarantined_at={quarantined_at_unix_ms}ms"
    )]
    SegmentQuarantined {
        engine: String,
        collection: String,
        segment_id: String,
        quarantined_at_unix_ms: u64,
    },

    /// The quarantine rename operation failed.
    #[error("failed to quarantine segment {segment_id}: {reason}")]
    RenameFailed { segment_id: String, reason: String },
}
