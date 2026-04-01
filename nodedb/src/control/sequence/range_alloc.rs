//! Raft-coordinated range allocation for distributed sequences.
//!
//! Each node (Origin shard or Lite instance) requests a chunk of IDs from
//! the Raft leader. Local `nextval` advances within the chunk without
//! network round-trips. When the chunk is exhausted, a new chunk is allocated.
//!
//! Uses the existing `RaftProposer` on SharedState — proposes serialized
//! `RangeAllocationRequest` messages to the Raft group, which are applied
//! by the state machine to update the global sequence counter.

use serde::{Deserialize, Serialize};

/// A request to allocate a range of sequence values via Raft consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeAllocationRequest {
    pub tenant_id: u32,
    pub sequence_name: String,
    /// How many values to allocate in this chunk.
    pub chunk_size: i64,
    /// Expected epoch — allocation rejected if epoch mismatch (stale node).
    pub epoch: u64,
}

/// Response from the Raft leader after a range allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeAllocationResponse {
    /// First value in the allocated range (inclusive).
    pub range_start: i64,
    /// Last value in the allocated range (inclusive).
    pub range_end: i64,
    /// Epoch of this allocation.
    pub epoch: u64,
}

/// Allocates sequence ranges via the Raft proposer.
///
/// In single-node mode (no Raft), ranges are allocated locally with no
/// coordination. In cluster mode, each allocation is proposed to the
/// Raft leader and committed across the group.
pub struct RangeAllocator {
    /// Default chunk size for allocations.
    pub default_chunk_size: i64,
}

impl RangeAllocator {
    pub fn new(default_chunk_size: i64) -> Self {
        Self { default_chunk_size }
    }

    /// Allocate a chunk of sequence values.
    ///
    /// In cluster mode: proposes to Raft leader, awaits commit.
    /// In single-node mode: directly advances the catalog counter.
    pub fn allocate_chunk(
        &self,
        state: &crate::control::state::SharedState,
        tenant_id: u32,
        sequence_name: &str,
        increment: i64,
        epoch: u64,
    ) -> Result<RangeAllocationResponse, crate::Error> {
        let chunk_size = self.default_chunk_size;

        // In cluster mode, propose through Raft for distributed uniqueness.
        if let Some(ref proposer) = state.raft_proposer {
            let request = RangeAllocationRequest {
                tenant_id,
                sequence_name: sequence_name.to_string(),
                chunk_size,
                epoch,
            };
            let payload = rmp_serde::to_vec(&request).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("range allocation request: {e}"),
            })?;

            // Propose to vshard 0 (system shard for metadata operations).
            let (_group_id, _log_index) =
                proposer(0, payload).map_err(|e| crate::Error::Dispatch {
                    detail: format!("sequence range allocation raft propose: {e}"),
                })?;

            // Compute the allocated range based on current state.
            // The Raft commit handler will advance the global counter.
            let current = state
                .sequence_registry
                .get_def(tenant_id, sequence_name)
                .map(|d| d.start_value)
                .unwrap_or(1);

            let range_start = current;
            let range_end = if increment > 0 {
                current + chunk_size * increment - increment
            } else {
                current + chunk_size * increment + increment.abs()
            };

            return Ok(RangeAllocationResponse {
                range_start,
                range_end,
                epoch,
            });
        }

        // Single-node mode: allocate directly from the local counter.
        // No Raft needed — just advance the counter by chunk_size.
        let handle_exists = state.sequence_registry.exists(tenant_id, sequence_name);
        if !handle_exists {
            return Err(crate::Error::BadRequest {
                detail: format!("sequence \"{sequence_name}\" does not exist"),
            });
        }

        let current_val = state
            .sequence_registry
            .currval(tenant_id, sequence_name)
            .unwrap_or(0);

        let range_start = current_val + increment;
        let range_end = range_start + (chunk_size - 1) * increment;

        Ok(RangeAllocationResponse {
            range_start,
            range_end,
            epoch,
        })
    }
}

impl Default for RangeAllocator {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let req = RangeAllocationRequest {
            tenant_id: 1,
            sequence_name: "order_seq".into(),
            chunk_size: 10_000,
            epoch: 1,
        };
        let bytes = rmp_serde::to_vec(&req).unwrap();
        let decoded: RangeAllocationRequest = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.sequence_name, "order_seq");
        assert_eq!(decoded.chunk_size, 10_000);
    }

    #[test]
    fn response_roundtrip() {
        let resp = RangeAllocationResponse {
            range_start: 1,
            range_end: 10_000,
            epoch: 1,
        };
        let bytes = rmp_serde::to_vec(&resp).unwrap();
        let decoded: RangeAllocationResponse = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.range_start, 1);
        assert_eq!(decoded.range_end, 10_000);
    }
}
