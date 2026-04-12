//! Inbound RPC dispatch — look up the target group and delegate.
//!
//! Also holds the response handlers (`handle_append_entries_response`,
//! `handle_request_vote_response`) and the helpers for the tick loop
//! (`snapshot_metadata`, `advance_applied`, `match_index_for`).

use nodedb_raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

use crate::error::{ClusterError, Result};

use super::core::MultiRaft;

impl MultiRaft {
    /// Route an AppendEntries RPC to the correct group.
    pub fn handle_append_entries(
        &mut self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_append_entries(req))
    }

    /// Route a RequestVote RPC to the correct group.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> Result<RequestVoteResponse> {
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_request_vote(req))
    }

    /// Route an InstallSnapshot RPC to the correct group.
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_install_snapshot(req))
    }

    /// Get the current term and snapshot metadata for a group (for building
    /// InstallSnapshot RPCs).
    pub fn snapshot_metadata(&self, group_id: u64) -> Result<(u64, u64, u64)> {
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok((
            node.current_term(),
            node.log_snapshot_index(),
            node.log_snapshot_term(),
        ))
    }

    /// Handle AppendEntries response for a specific group.
    pub fn handle_append_entries_response(
        &mut self,
        group_id: u64,
        peer: u64,
        resp: &AppendEntriesResponse,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.handle_append_entries_response(peer, resp);
        Ok(())
    }

    /// Handle RequestVote response for a specific group.
    pub fn handle_request_vote_response(
        &mut self,
        group_id: u64,
        peer: u64,
        resp: &RequestVoteResponse,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.handle_request_vote_response(peer, resp);
        Ok(())
    }

    /// Advance applied index for a group after processing committed entries.
    pub fn advance_applied(&mut self, group_id: u64, applied_to: u64) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.advance_applied(applied_to);
        Ok(())
    }

    /// Query a peer's match_index from a specific Raft group's leader state.
    pub fn match_index_for(&self, group_id: u64, peer: u64) -> Option<u64> {
        self.groups.get(&group_id)?.match_index_for(peer)
    }
}
