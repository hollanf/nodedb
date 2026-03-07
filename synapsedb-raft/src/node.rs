use std::time::{Duration, Instant};

use rand::Rng;
use tracing::{debug, info, warn};

use crate::error::{RaftError, Result};
use crate::log::RaftLog;
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry, RequestVoteRequest, RequestVoteResponse,
};
use crate::state::{HardState, LeaderState, NodeRole, VolatileState};
use crate::storage::LogStorage;

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID (must be unique within the Raft group).
    pub node_id: u64,
    /// Raft group ID (for Multi-Raft routing).
    pub group_id: u64,
    /// IDs of all peers in this group (excluding self).
    pub peers: Vec<u64>,
    /// Minimum election timeout.
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// Heartbeat interval (must be << election_timeout_min).
    pub heartbeat_interval: Duration,
}

impl RaftConfig {
    /// Total number of voters (self + peers).
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Quorum size: floor(n/2) + 1.
    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
}

/// Output actions produced by a tick or RPC handler.
///
/// The caller (Multi-Raft coordinator) is responsible for executing these
/// via the transport and applying committed entries to the state machine.
#[derive(Debug, Default)]
pub struct Ready {
    /// Hard state to persist (if changed).
    pub hard_state: Option<HardState>,
    /// Entries to send to specific peers (peer_id, request).
    pub messages: Vec<(u64, AppendEntriesRequest)>,
    /// Vote requests to send (peer_id, request).
    pub vote_requests: Vec<(u64, RequestVoteRequest)>,
    /// Newly committed entries to apply to the state machine.
    pub committed_entries: Vec<LogEntry>,
    /// Peers that need an InstallSnapshot RPC because their next_index
    /// falls behind the leader's snapshot_index (log compacted).
    pub snapshots_needed: Vec<u64>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.messages.is_empty()
            && self.vote_requests.is_empty()
            && self.committed_entries.is_empty()
            && self.snapshots_needed.is_empty()
    }
}

/// A single Raft group's state machine.
///
/// This is a deterministic, event-driven core. It does NOT own any threads
/// or timers — the caller drives it via `tick()` and RPC handler methods,
/// and reads output via `take_ready()`.
pub struct RaftNode<S: LogStorage> {
    config: RaftConfig,
    role: NodeRole,
    hard_state: HardState,
    volatile: VolatileState,
    leader_state: Option<LeaderState>,
    log: RaftLog<S>,
    /// When the next election timeout fires.
    election_deadline: Instant,
    /// When the next heartbeat should be sent (leader only).
    heartbeat_deadline: Instant,
    /// Votes received in current election.
    votes_received: Vec<u64>,
    /// Pending ready output.
    ready: Ready,
    /// Known leader ID (0 = unknown).
    leader_id: u64,
}

impl<S: LogStorage> RaftNode<S> {
    /// Create a new Raft node. Call `restore()` before ticking.
    pub fn new(config: RaftConfig, storage: S) -> Self {
        let now = Instant::now();
        Self {
            log: RaftLog::new(storage),
            role: NodeRole::Follower,
            hard_state: HardState::new(),
            volatile: VolatileState::new(),
            leader_state: None,
            election_deadline: now + config.election_timeout_max,
            heartbeat_deadline: now,
            votes_received: Vec::new(),
            ready: Ready::default(),
            leader_id: 0,
            config,
        }
    }

    /// Restore state from persistent storage. Must be called before ticking.
    pub fn restore(&mut self) -> Result<()> {
        self.hard_state = self.log.storage().load_hard_state()?;
        self.log.restore()?;
        self.reset_election_timeout();
        Ok(())
    }

    pub fn node_id(&self) -> u64 {
        self.config.node_id
    }

    pub fn group_id(&self) -> u64 {
        self.config.group_id
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    pub fn leader_id(&self) -> u64 {
        self.leader_id
    }

    pub fn current_term(&self) -> u64 {
        self.hard_state.current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.volatile.commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.volatile.last_applied
    }

    /// Override election deadline (for testing).
    pub fn election_deadline_override(&mut self, deadline: Instant) {
        self.election_deadline = deadline;
    }

    /// Take the pending `Ready` output. Caller must execute messages,
    /// persist hard state, and apply committed entries.
    pub fn take_ready(&mut self) -> Ready {
        std::mem::take(&mut self.ready)
    }

    /// Advance `last_applied` after the caller has applied entries.
    pub fn advance_applied(&mut self, applied_to: u64) {
        self.volatile.last_applied = applied_to;
    }

    /// Drive time-based events: election timeout, heartbeat.
    pub fn tick(&mut self) {
        let now = Instant::now();

        match self.role {
            NodeRole::Follower | NodeRole::Candidate => {
                if now >= self.election_deadline {
                    self.start_election();
                }
            }
            NodeRole::Leader => {
                if now >= self.heartbeat_deadline {
                    self.replicate_to_all();
                    self.heartbeat_deadline = now + self.config.heartbeat_interval;
                }
            }
            NodeRole::Learner => {
                // Learners don't participate in elections.
            }
        }
    }

    /// Propose a new entry (leader only). Returns the log index.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64> {
        if self.role != NodeRole::Leader {
            return Err(RaftError::NotLeader {
                leader_hint: if self.leader_id != 0 {
                    Some(self.leader_id)
                } else {
                    None
                },
            });
        }

        let index = self.log.last_index() + 1;
        let entry = LogEntry {
            term: self.hard_state.current_term,
            index,
            data,
        };

        self.log.append(entry)?;

        // Replicate to all peers.
        self.replicate_to_all();

        // Single-node cluster: commit immediately.
        if self.config.cluster_size() == 1 {
            self.volatile.commit_index = index;
            self.collect_committed_entries();
        }

        Ok(index)
    }

    /// Handle incoming AppendEntries RPC.
    pub fn handle_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        // Rule: if term < currentTerm, reject.
        if req.term < self.hard_state.current_term {
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.log.last_index(),
            };
        }

        // If RPC term >= our term, recognize as leader and step down.
        if req.term > self.hard_state.current_term {
            self.become_follower(req.term);
        } else if self.role == NodeRole::Candidate {
            // Same term but valid leader exists — step down.
            self.become_follower(req.term);
        }

        self.leader_id = req.leader_id;
        self.reset_election_timeout();

        // Check prev_log consistency.
        if req.prev_log_index > 0 {
            match self.log.term_at(req.prev_log_index) {
                Some(term) if term == req.prev_log_term => {}
                _ => {
                    return AppendEntriesResponse {
                        term: self.hard_state.current_term,
                        success: false,
                        last_log_index: self.log.last_index(),
                    };
                }
            }
        }

        // Append entries.
        if let Err(e) = self.log.append_entries(req.prev_log_index, &req.entries) {
            warn!(group = self.config.group_id, error = %e, "append_entries failed");
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.log.last_index(),
            };
        }

        // Update commit_index.
        if req.leader_commit > self.volatile.commit_index {
            self.volatile.commit_index = req.leader_commit.min(self.log.last_index());
            self.collect_committed_entries();
        }

        AppendEntriesResponse {
            term: self.hard_state.current_term,
            success: true,
            last_log_index: self.log.last_index(),
        }
    }

    /// Handle incoming RequestVote RPC.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> RequestVoteResponse {
        // If term > ours, update and become follower.
        if req.term > self.hard_state.current_term {
            self.become_follower(req.term);
        }

        // Reject if term < ours.
        if req.term < self.hard_state.current_term {
            return RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            };
        }

        // Grant vote if: (a) we haven't voted or voted for this candidate,
        // AND (b) candidate's log is at least as up-to-date as ours.
        let voted_for = self.hard_state.voted_for;
        let can_vote = voted_for == 0 || voted_for == req.candidate_id;

        let log_ok = req.last_log_term > self.log.last_term()
            || (req.last_log_term == self.log.last_term()
                && req.last_log_index >= self.log.last_index());

        if can_vote && log_ok {
            self.hard_state.voted_for = req.candidate_id;
            self.persist_hard_state();
            self.reset_election_timeout();

            debug!(
                node = self.config.node_id,
                group = self.config.group_id,
                candidate = req.candidate_id,
                term = req.term,
                "granted vote"
            );

            RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            }
        }
    }

    /// Handle AppendEntries response from a peer (leader only).
    pub fn handle_append_entries_response(&mut self, peer: u64, resp: &AppendEntriesResponse) {
        if resp.term > self.hard_state.current_term {
            self.become_follower(resp.term);
            return;
        }

        if self.role != NodeRole::Leader {
            return;
        }

        let leader = match self.leader_state.as_mut() {
            Some(ls) => ls,
            None => return,
        };

        if resp.success {
            // Update match_index and next_index.
            let new_match = resp.last_log_index;
            if new_match > leader.match_index_for(peer) {
                leader.set_match_index(peer, new_match);
                leader.set_next_index(peer, new_match + 1);
            }
            self.try_advance_commit_index();
        } else {
            // Decrement next_index and retry. Use fast-backup: jump to
            // follower's last_log_index + 1.
            let new_next = resp.last_log_index + 1;
            let current_next = leader.next_index_for(peer);
            if new_next < current_next {
                leader.set_next_index(peer, new_next.max(1));
            } else {
                leader.set_next_index(peer, current_next.saturating_sub(1).max(1));
            }
            self.send_append_entries(peer);
        }
    }

    /// Handle RequestVote response (candidate only).
    pub fn handle_request_vote_response(&mut self, _peer: u64, resp: &RequestVoteResponse) {
        if resp.term > self.hard_state.current_term {
            self.become_follower(resp.term);
            return;
        }

        if self.role != NodeRole::Candidate {
            return;
        }

        if resp.vote_granted {
            self.votes_received.push(resp.term); // using term as placeholder
            let vote_count = self.votes_received.len() + 1; // +1 for self-vote

            if vote_count >= self.config.quorum() {
                self.become_leader();
            }
        }
    }

    // --- Internal state transitions ---

    fn start_election(&mut self) {
        self.hard_state.current_term += 1;
        self.role = NodeRole::Candidate;
        self.hard_state.voted_for = self.config.node_id;
        self.votes_received.clear();
        self.leader_id = 0;

        self.persist_hard_state();
        self.reset_election_timeout();

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            term = self.hard_state.current_term,
            "starting election"
        );

        // Single-node: win immediately.
        if self.config.peers.is_empty() {
            self.become_leader();
            return;
        }

        // Send RequestVote to all peers.
        for &peer in &self.config.peers {
            self.ready.vote_requests.push((
                peer,
                RequestVoteRequest {
                    term: self.hard_state.current_term,
                    candidate_id: self.config.node_id,
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                    group_id: self.config.group_id,
                },
            ));
        }
    }

    fn become_follower(&mut self, term: u64) {
        let was_leader = self.role == NodeRole::Leader;
        self.role = NodeRole::Follower;
        self.hard_state.current_term = term;
        self.hard_state.voted_for = 0;
        self.leader_state = None;
        self.votes_received.clear();
        self.persist_hard_state();
        self.reset_election_timeout();

        if was_leader {
            info!(
                node = self.config.node_id,
                group = self.config.group_id,
                term,
                "stepped down from leader"
            );
        }
    }

    fn become_leader(&mut self) {
        self.role = NodeRole::Leader;
        self.leader_id = self.config.node_id;
        self.leader_state = Some(LeaderState::new(&self.config.peers, self.log.last_index()));

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            term = self.hard_state.current_term,
            "became leader"
        );

        // Raft paper §5.4.2: leader appends a no-op entry to commit
        // entries from previous terms.
        let noop = LogEntry {
            term: self.hard_state.current_term,
            index: self.log.last_index() + 1,
            data: Vec::new(),
        };
        let _ = self.log.append(noop);

        // Single-node: commit the no-op immediately.
        if self.config.cluster_size() == 1 {
            self.volatile.commit_index = self.log.last_index();
            self.collect_committed_entries();
        }

        // Send initial heartbeats (with the no-op).
        self.replicate_to_all();
    }

    fn replicate_to_all(&mut self) {
        let peers: Vec<u64> = self.config.peers.clone();
        for peer in peers {
            self.send_append_entries(peer);
        }
    }

    fn send_append_entries(&mut self, peer: u64) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let next_index = leader.next_index_for(peer);
        let prev_log_index = next_index.saturating_sub(1);

        // If prev_log_index falls in the compacted range, we cannot
        // construct a valid AppendEntries — the follower needs a snapshot.
        let prev_log_term = match self.log.term_at(prev_log_index) {
            Some(term) => term,
            None => {
                // Term unavailable: index is compacted. Signal that this
                // peer needs an InstallSnapshot (handled by the caller
                // via Ready::snapshots_needed).
                debug!(
                    node = self.config.node_id,
                    group = self.config.group_id,
                    peer,
                    next_index,
                    snapshot_index = self.log.snapshot_index(),
                    "peer needs snapshot (log compacted)"
                );
                self.ready.snapshots_needed.push(peer);
                return;
            }
        };

        // Gather entries to send. If the range is compacted, the follower
        // needs a snapshot instead.
        let entries = if next_index <= self.log.last_index() {
            match self.log.entries_range(next_index, self.log.last_index()) {
                Ok(slice) => slice.to_vec(),
                Err(RaftError::LogCompacted { .. }) => {
                    debug!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        peer,
                        next_index,
                        "peer needs snapshot (entries compacted)"
                    );
                    self.ready.snapshots_needed.push(peer);
                    return;
                }
                Err(_) => vec![],
            }
        } else {
            vec![]
        };

        self.ready.messages.push((
            peer,
            AppendEntriesRequest {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.volatile.commit_index,
                group_id: self.config.group_id,
            },
        ));
    }

    /// Try to advance commit_index based on match_index quorum.
    fn try_advance_commit_index(&mut self) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        // Find the highest N such that a majority of match_index[i] >= N
        // and log[N].term == currentTerm (Raft paper Figure 8).
        let last = self.log.last_index();
        for n in (self.volatile.commit_index + 1..=last).rev() {
            let term_at_n = match self.log.term_at(n) {
                Some(t) => t,
                None => continue,
            };

            // Only commit entries from current term.
            if term_at_n != self.hard_state.current_term {
                continue;
            }

            // Count replicas (self = 1).
            let mut count = 1u64;
            for &peer in &self.config.peers {
                if leader.match_index_for(peer) >= n {
                    count += 1;
                }
            }

            if count as usize >= self.config.quorum() {
                self.volatile.commit_index = n;
                self.collect_committed_entries();
                break;
            }
        }
    }

    /// Collect newly committed entries into ready output.
    fn collect_committed_entries(&mut self) {
        let from = self.volatile.last_applied + 1;
        let to = self.volatile.commit_index;
        if from > to {
            return;
        }
        if let Ok(entries) = self.log.entries_range(from, to) {
            self.ready.committed_entries.extend(entries.iter().cloned());
        }
    }

    fn persist_hard_state(&mut self) {
        self.ready.hard_state = Some(self.hard_state.clone());
    }

    fn reset_election_timeout(&mut self) {
        let mut rng = rand::rng();
        let min = self.config.election_timeout_min.as_millis() as u64;
        let max = self.config.election_timeout_max.as_millis() as u64;
        let timeout = Duration::from_millis(rng.random_range(min..=max));
        self.election_deadline = Instant::now() + timeout;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;

    fn test_config(node_id: u64, peers: Vec<u64>) -> RaftConfig {
        RaftConfig {
            node_id,
            group_id: 1,
            peers,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }

    #[test]
    fn single_node_election() {
        let config = test_config(1, vec![]);
        let mut node = RaftNode::new(config, MemStorage::new());

        // Tick past election timeout.
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();

        assert_eq!(node.role(), NodeRole::Leader);
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.leader_id(), 1);
    }

    #[test]
    fn single_node_propose_and_commit() {
        let config = test_config(1, vec![]);
        let mut node = RaftNode::new(config, MemStorage::new());
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);

        // No-op entry committed at index 1.
        let ready = node.take_ready();
        assert!(!ready.committed_entries.is_empty());
        // Simulate caller applying entries.
        node.advance_applied(ready.committed_entries.last().unwrap().index);

        let idx = node.propose(b"hello".to_vec()).unwrap();
        assert_eq!(idx, 2); // index 1 is no-op

        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].data, b"hello");
    }

    #[test]
    fn follower_rejects_old_term() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());
        // Manually set term.
        node.hard_state.current_term = 5;

        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
        };

        let resp = node.handle_append_entries(&req);
        assert!(!resp.success);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn follower_accepts_valid_append() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry {
                    term: 1,
                    index: 1,
                    data: b"a".to_vec(),
                },
                LogEntry {
                    term: 1,
                    index: 2,
                    data: b"b".to_vec(),
                },
            ],
            leader_commit: 1,
            group_id: 1,
        };

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(resp.last_log_index, 2);
        assert_eq!(node.commit_index(), 1);
        assert_eq!(node.leader_id(), 2);
    }

    #[test]
    fn vote_grant_and_reject() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        // Should grant: term is higher, log is ok.
        let req = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
        };
        let resp = node.handle_request_vote(&req);
        assert!(resp.vote_granted);

        // Should reject: already voted for 2 in term 1.
        let req2 = RequestVoteRequest {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
        };
        let resp2 = node.handle_request_vote(&req2);
        assert!(!resp2.vote_granted);
    }

    #[test]
    fn three_node_election() {
        let config1 = test_config(1, vec![2, 3]);
        let config2 = test_config(2, vec![1, 3]);
        let config3 = test_config(3, vec![1, 2]);

        let mut node1 = RaftNode::new(config1, MemStorage::new());
        let mut node2 = RaftNode::new(config2, MemStorage::new());
        let mut node3 = RaftNode::new(config3, MemStorage::new());

        // Node 1 starts election.
        node1.election_deadline = Instant::now() - Duration::from_millis(1);
        node1.tick();
        assert_eq!(node1.role(), NodeRole::Candidate);
        assert_eq!(node1.current_term(), 1);

        let ready = node1.take_ready();
        assert_eq!(ready.vote_requests.len(), 2);

        // Node 2 and 3 handle vote requests.
        let resp2 = node2.handle_request_vote(&ready.vote_requests[0].1);
        let resp3 = node3.handle_request_vote(&ready.vote_requests[1].1);
        assert!(resp2.vote_granted);
        assert!(resp3.vote_granted);

        // Node 1 receives votes.
        node1.handle_request_vote_response(2, &resp2);
        // Should become leader after receiving one vote (quorum = 2).
        assert_eq!(node1.role(), NodeRole::Leader);
    }

    #[test]
    fn three_node_replication() {
        let config1 = test_config(1, vec![2, 3]);
        let config2 = test_config(2, vec![1, 3]);
        let _config3 = test_config(3, vec![1, 2]);

        let mut node1 = RaftNode::new(config1, MemStorage::new());
        let mut node2 = RaftNode::new(config2, MemStorage::new());

        // Make node1 leader.
        node1.election_deadline = Instant::now() - Duration::from_millis(1);
        node1.tick();
        let ready = node1.take_ready();
        let resp2 = node2.handle_request_vote(&ready.vote_requests[0].1);
        node1.handle_request_vote_response(2, &resp2);
        assert_eq!(node1.role(), NodeRole::Leader);

        // Drain initial heartbeats.
        let heartbeat_ready = node1.take_ready();
        // Process heartbeats on node2 (no-op entry).
        for (peer_id, msg) in &heartbeat_ready.messages {
            if *peer_id == 2 {
                let resp = node2.handle_append_entries(msg);
                node1.handle_append_entries_response(2, &resp);
            }
        }

        // Propose an entry.
        let idx = node1.propose(b"cmd1".to_vec()).unwrap();
        assert_eq!(idx, 2);

        let ready = node1.take_ready();
        // Send entries to node2.
        for (peer_id, msg) in &ready.messages {
            if *peer_id == 2 {
                let resp = node2.handle_append_entries(msg);
                assert!(resp.success);
                node1.handle_append_entries_response(2, &resp);
            }
        }

        // After quorum ack, commit_index advances.
        let ready = node1.take_ready();
        let committed: Vec<_> = ready
            .committed_entries
            .iter()
            .filter(|e| !e.data.is_empty())
            .collect();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].data, b"cmd1");
    }

    #[test]
    fn leader_steps_down_on_higher_term() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        // Make leader.
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        let _ready = node.take_ready();
        let resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &resp);
        assert_eq!(node.role(), NodeRole::Leader);

        // Receive AppendEntries with higher term.
        let req = AppendEntriesRequest {
            term: 5,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
        };
        node.handle_append_entries(&req);
        assert_eq!(node.role(), NodeRole::Follower);
        assert_eq!(node.current_term(), 5);
        assert_eq!(node.leader_id(), 2);
    }

    #[test]
    fn propose_as_follower_fails() {
        let config = test_config(1, vec![2, 3]);
        let node = &mut RaftNode::new(config, MemStorage::new());
        let err = node.propose(b"data".to_vec()).unwrap_err();
        assert!(matches!(err, RaftError::NotLeader { .. }));
    }

    #[test]
    fn snapshot_needed_after_compaction() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        // Make leader.
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        let _ready = node.take_ready();
        let resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &resp);
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready(); // drain initial heartbeats

        // Append entries 2..10 (index 1 is the no-op).
        for i in 0..9 {
            node.propose(vec![i]).unwrap();
        }
        let _ = node.take_ready(); // drain messages

        // Compact log up to index 8.
        node.log.apply_snapshot(8, 1);

        // Now try to replicate to peer 2 whose next_index is 1 (behind snapshot).
        // The leader should detect this and signal snapshots_needed.
        node.replicate_to_all();
        let ready = node.take_ready();

        // Peer 2 and 3 should both need snapshots (their next_index is behind snapshot_index=8).
        assert!(
            !ready.snapshots_needed.is_empty(),
            "expected snapshots_needed to be non-empty"
        );
    }

    #[test]
    fn quorum_calculation() {
        let config3 = test_config(1, vec![2, 3]);
        assert_eq!(config3.quorum(), 2);

        let config5 = RaftConfig {
            node_id: 1,
            group_id: 1,
            peers: vec![2, 3, 4, 5],
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        };
        assert_eq!(config5.quorum(), 3);
    }
}
