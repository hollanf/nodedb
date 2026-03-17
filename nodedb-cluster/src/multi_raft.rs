use std::collections::HashMap;
use std::time::Duration;

use tracing::debug;

use nodedb_raft::node::RaftConfig;
use nodedb_raft::storage::MemStorage;
use nodedb_raft::{
    AppendEntriesRequest, AppendEntriesResponse, RaftNode, Ready, RequestVoteRequest,
    RequestVoteResponse,
};

use crate::error::{ClusterError, Result};
use crate::routing::RoutingTable;

/// Snapshot of a single Raft group's state for observability.
#[derive(Debug, Clone)]
pub struct GroupStatus {
    pub group_id: u64,
    /// Role as a human-readable string ("Leader", "Follower", "Candidate", "Learner").
    pub role: String,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub member_count: usize,
    pub vshard_count: usize,
}

/// Multi-Raft coordinator managing multiple Raft groups on a single node.
///
/// This coordinator:
/// - Manages all Raft groups hosted on this node
/// - Batches heartbeats across groups sharing the same leader
/// - Routes incoming RPCs to the correct group
/// - Collects `Ready` output from all groups for the caller to execute
pub struct MultiRaft {
    /// This node's ID.
    node_id: u64,
    /// Raft groups hosted on this node (group_id → RaftNode).
    groups: HashMap<u64, RaftNode<MemStorage>>,
    /// Routing table (vShard → group mapping).
    routing: RoutingTable,
    /// Default election timeout range.
    election_timeout_min: Duration,
    election_timeout_max: Duration,
    /// Heartbeat interval.
    heartbeat_interval: Duration,
}

/// Aggregated output from all Raft groups after a tick.
#[derive(Debug, Default)]
pub struct MultiRaftReady {
    /// Per-group ready output: (group_id, Ready).
    pub groups: Vec<(u64, Ready)>,
}

impl MultiRaftReady {
    pub fn is_empty(&self) -> bool {
        self.groups.iter().all(|(_gid, r)| r.is_empty())
    }

    /// Total committed entries across all groups.
    pub fn total_committed(&self) -> usize {
        self.groups
            .iter()
            .map(|(_, r)| r.committed_entries.len())
            .sum()
    }
}

impl MultiRaft {
    pub fn new(node_id: u64, routing: RoutingTable) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            routing,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }

    /// Configure election timeout range.
    pub fn with_election_timeout(mut self, min: Duration, max: Duration) -> Self {
        self.election_timeout_min = min;
        self.election_timeout_max = max;
        self
    }

    /// Configure heartbeat interval.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Initialize a Raft group on this node.
    pub fn add_group(&mut self, group_id: u64, peers: Vec<u64>) {
        let config = RaftConfig {
            node_id: self.node_id,
            group_id,
            peers,
            election_timeout_min: self.election_timeout_min,
            election_timeout_max: self.election_timeout_max,
            heartbeat_interval: self.heartbeat_interval,
        };

        let node = RaftNode::new(config, MemStorage::new());
        self.groups.insert(group_id, node);

        debug!(node = self.node_id, group = group_id, "added raft group");
    }

    /// Tick all Raft groups. Returns aggregated ready output.
    pub fn tick(&mut self) -> MultiRaftReady {
        let mut ready = MultiRaftReady::default();

        for (&group_id, node) in &mut self.groups {
            node.tick();
            let r = node.take_ready();
            if !r.is_empty() {
                ready.groups.push((group_id, r));
            }
        }

        ready
    }

    /// Propose a command to the Raft group that owns the given vShard.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose(&mut self, vshard_id: u16, data: Vec<u8>) -> Result<(u64, u64)> {
        let group_id = self.routing.group_for_vshard(vshard_id)?;
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let log_index = node.propose(data)?;
        Ok((group_id, log_index))
    }

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

    pub fn routing(&self) -> &RoutingTable {
        &self.routing
    }

    pub fn routing_mut(&mut self) -> &mut RoutingTable {
        &mut self.routing
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Mutable access to the underlying Raft groups (for testing / bootstrap).
    pub fn groups_mut(&mut self) -> &mut HashMap<u64, RaftNode<MemStorage>> {
        &mut self.groups
    }

    /// Snapshot of all Raft group states for observability.
    pub fn group_statuses(&self) -> Vec<GroupStatus> {
        let mut statuses = Vec::with_capacity(self.groups.len());
        for (&group_id, node) in &self.groups {
            let vshard_count = self.routing.vshards_for_group(group_id).len();
            let members = self
                .routing
                .group_info(group_id)
                .map(|info| info.members.clone())
                .unwrap_or_default();

            statuses.push(GroupStatus {
                group_id,
                role: format!("{:?}", node.role()),
                leader_id: node.leader_id(),
                term: node.current_term(),
                commit_index: node.commit_index(),
                last_applied: node.last_applied(),
                member_count: members.len(),
                vshard_count,
            });
        }
        statuses.sort_by_key(|s| s.group_id);
        statuses
    }

    /// Get the leader for a given vShard (from local group state).
    pub fn leader_for_vshard(&self, vshard_id: u16) -> Result<Option<u64>> {
        let group_id = self.routing.group_for_vshard(vshard_id)?;
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let lid = node.leader_id();
        Ok(if lid == 0 { None } else { Some(lid) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn single_node_multi_raft() {
        let rt = RoutingTable::uniform(4, &[1], 1);
        let mut mr = MultiRaft::new(1, rt);

        // Add 4 groups, each with no peers (single-node).
        for gid in 0..4 {
            mr.add_group(gid, vec![]);
        }
        assert_eq!(mr.group_count(), 4);

        // Force election timeout on all groups.
        // We need to access groups directly for this test.
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let ready = mr.tick();
        // All 4 groups should have become leaders.
        assert_eq!(ready.groups.len(), 4);
    }

    #[test]
    fn propose_routes_to_correct_group() {
        let rt = RoutingTable::uniform(4, &[1], 1);
        let mut mr = MultiRaft::new(1, rt);

        for gid in 0..4 {
            mr.add_group(gid, vec![]);
        }
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        mr.tick();
        // Drain initial ready.
        for (gid, ready) in mr.tick().groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).unwrap();
            }
        }

        // vShard 0 maps to group 0, vShard 1 to group 1, etc.
        let (_gid, idx) = mr.propose(0, b"cmd-shard-0".to_vec()).unwrap();
        assert!(idx > 0);

        let (_gid, idx) = mr.propose(256, b"cmd-shard-256".to_vec()).unwrap();
        assert!(idx > 0);
    }

    #[test]
    fn three_node_multi_raft_election() {
        let nodes = vec![1, 2, 3];
        let rt = RoutingTable::uniform(2, &nodes, 3);

        // Create MultiRaft for each node.
        let mut mr1 = MultiRaft::new(1, rt.clone());
        let mut mr2 = MultiRaft::new(2, rt.clone());
        let mut mr3 = MultiRaft::new(3, rt.clone());

        // Add groups to each node.
        for gid in 0..2u64 {
            mr1.add_group(gid, vec![2, 3]);
            mr2.add_group(gid, vec![1, 3]);
            mr3.add_group(gid, vec![1, 2]);
        }

        // Force node 1 to start elections.
        for node in mr1.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let ready1 = mr1.tick();

        // Process vote requests on nodes 2 and 3.
        for (group_id, ready) in &ready1.groups {
            for (peer_id, vote_req) in &ready.vote_requests {
                if *peer_id == 2 {
                    let resp = mr2.handle_request_vote(vote_req).unwrap();
                    mr1.handle_request_vote_response(*group_id, 2, &resp)
                        .unwrap();
                } else if *peer_id == 3 {
                    let resp = mr3.handle_request_vote(vote_req).unwrap();
                    mr1.handle_request_vote_response(*group_id, 3, &resp)
                        .unwrap();
                }
            }
        }

        // Node 1 should be leader for both groups.
        for gid in 0..2u64 {
            let leader = mr1.leader_for_vshard(gid as u16 * 512).unwrap();
            assert_eq!(leader, Some(1));
        }
    }
}
