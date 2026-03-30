//! Shared stream consumption logic.
//!
//! Used by both HTTP endpoints and pgwire SELECT to read events from a
//! change stream's buffer using consumer group offsets.

use crate::control::state::SharedState;
use crate::event::cdc::event::CdcEvent;

/// Parameters for consuming events from a stream.
pub struct ConsumeParams<'a> {
    pub tenant_id: u32,
    pub stream_name: &'a str,
    pub group_name: &'a str,
    /// Optional: consume from a specific partition only.
    pub partition: Option<u16>,
    /// Maximum events to return.
    pub limit: usize,
}

/// Result of consuming events from a stream.
pub struct ConsumeResult {
    /// The events read from the buffer.
    pub events: Vec<CdcEvent>,
    /// Per-partition latest LSN seen in this batch (for offset tracking).
    pub partition_offsets: Vec<(u16, u64)>,
}

/// Consume events from a change stream using consumer group offsets.
///
/// Reads events with LSN > the group's committed offset for each partition.
/// Does NOT auto-commit offsets — the caller must explicitly COMMIT OFFSET.
pub fn consume_stream(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    // Verify stream exists.
    if state
        .stream_registry
        .get(params.tenant_id, params.stream_name)
        .is_none()
    {
        return Err(ConsumeError::StreamNotFound(params.stream_name.to_string()));
    }

    // Verify consumer group exists.
    if state
        .group_registry
        .get(params.tenant_id, params.stream_name, params.group_name)
        .is_none()
    {
        return Err(ConsumeError::GroupNotFound(
            params.group_name.to_string(),
            params.stream_name.to_string(),
        ));
    }

    // Get the stream buffer.
    let buffer = state
        .cdc_router
        .get_buffer(params.tenant_id, params.stream_name)
        .ok_or_else(|| ConsumeError::BufferEmpty(params.stream_name.to_string()))?;

    // Read events based on committed offsets.
    let events = if let Some(partition_id) = params.partition {
        // Single partition read.
        let from_lsn = state.offset_store.get_offset(
            params.tenant_id,
            params.stream_name,
            params.group_name,
            partition_id,
        );
        buffer.read_partition_from_lsn(partition_id, from_lsn, params.limit)
    } else {
        // All partitions: read from the minimum committed offset.
        // Each event's partition field lets consumers track per-partition progress.
        let all_offsets = state.offset_store.get_all_offsets(
            params.tenant_id,
            params.stream_name,
            params.group_name,
        );
        // Use the minimum offset across all committed partitions, or 0 if none committed.
        let min_lsn = all_offsets
            .iter()
            .map(|o| o.committed_lsn)
            .min()
            .unwrap_or(0);
        buffer.read_from_lsn(min_lsn, params.limit)
    };

    // Compute per-partition max LSN for the returned batch.
    let mut partition_offsets: std::collections::BTreeMap<u16, u64> =
        std::collections::BTreeMap::new();
    for e in &events {
        let entry = partition_offsets.entry(e.partition).or_insert(0);
        if e.lsn > *entry {
            *entry = e.lsn;
        }
    }

    Ok(ConsumeResult {
        events,
        partition_offsets: partition_offsets.into_iter().collect(),
    })
}

/// Errors from stream consumption.
#[derive(Debug)]
pub enum ConsumeError {
    StreamNotFound(String),
    GroupNotFound(String, String),
    /// Stream exists but buffer is empty (no events yet).
    BufferEmpty(String),
}

impl std::fmt::Display for ConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamNotFound(s) => write!(f, "change stream '{s}' does not exist"),
            Self::GroupNotFound(g, s) => {
                write!(f, "consumer group '{g}' does not exist on stream '{s}'")
            }
            Self::BufferEmpty(s) => write!(f, "stream '{s}' has no buffered events"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_error_display() {
        let e = ConsumeError::StreamNotFound("orders".into());
        assert!(e.to_string().contains("orders"));
    }
}
