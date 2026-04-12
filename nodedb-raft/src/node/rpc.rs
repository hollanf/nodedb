//! RPC handlers for incoming Raft messages.

use tracing::{debug, info, warn};

use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::state::NodeRole;
use crate::storage::LogStorage;

use super::core::RaftNode;

impl<S: LogStorage> RaftNode<S> {
    /// Handle incoming AppendEntries RPC.
    pub fn handle_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.hard_state.current_term {
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.log.last_index(),
            };
        }

        if req.term > self.hard_state.current_term || self.role == NodeRole::Candidate {
            // `become_follower` preserves Learner role — see internal.rs.
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

        if let Err(e) = self.log.append_entries(req.prev_log_index, &req.entries) {
            warn!(group = self.config.group_id, error = %e, "append_entries failed");
            return AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                last_log_index: self.log.last_index(),
            };
        }

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
    ///
    /// Learners never grant votes: by definition they are not members of
    /// the voting set for this term, and granting a vote could let an
    /// incorrect quorum form.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> RequestVoteResponse {
        if self.role == NodeRole::Learner {
            return RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            };
        }

        if req.term > self.hard_state.current_term {
            self.become_follower(req.term);
        }

        if req.term < self.hard_state.current_term {
            return RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            };
        }

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
    ///
    /// For both voter and learner peers we update `match_index`/`next_index`
    /// so we can tell when a learner has caught up. Only voter responses
    /// trigger a commit-advancement check — learners never contribute to
    /// the commit quorum.
    pub fn handle_append_entries_response(&mut self, peer: u64, resp: &AppendEntriesResponse) {
        if resp.term > self.hard_state.current_term {
            self.become_follower(resp.term);
            return;
        }

        if self.role != NodeRole::Leader {
            return;
        }

        let peer_is_voter = self.config.peers.contains(&peer);

        let leader = match self.leader_state.as_mut() {
            Some(ls) => ls,
            None => return,
        };

        if resp.success {
            let new_match = resp.last_log_index;
            if new_match > leader.match_index_for(peer) {
                leader.set_match_index(peer, new_match);
                leader.set_next_index(peer, new_match + 1);
            }
            if peer_is_voter {
                self.try_advance_commit_index();
            }
        } else {
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
            self.votes_received.push(resp.term);
            let vote_count = self.votes_received.len() + 1; // +1 for self-vote

            if vote_count >= self.config.quorum() {
                self.become_leader();
            }
        }
    }

    /// Handle incoming InstallSnapshot RPC (Raft paper Figure 13).
    ///
    /// Called on followers (and learners) that are too far behind for
    /// log-based catch-up. The leader sends its snapshot; the receiver
    /// replaces its log and state.
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        if req.term < self.hard_state.current_term {
            return InstallSnapshotResponse {
                term: self.hard_state.current_term,
            };
        }

        if req.term > self.hard_state.current_term {
            self.become_follower(req.term);
        }

        self.leader_id = req.leader_id;
        self.reset_election_timeout();

        if req.done && req.last_included_index > self.log.snapshot_index() {
            info!(
                node = self.config.node_id,
                group = self.config.group_id,
                snapshot_index = req.last_included_index,
                snapshot_term = req.last_included_term,
                "applying installed snapshot"
            );

            self.log
                .apply_snapshot(req.last_included_index, req.last_included_term);

            if self.volatile.commit_index < req.last_included_index {
                self.volatile.commit_index = req.last_included_index;
            }
            if self.volatile.last_applied < req.last_included_index {
                self.volatile.last_applied = req.last_included_index;
            }
        }

        InstallSnapshotResponse {
            term: self.hard_state.current_term,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{AppendEntriesRequest, LogEntry, RequestVoteRequest, RequestVoteResponse};
    use crate::node::config::RaftConfig;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;

    use super::*;

    fn test_config(node_id: u64, peers: Vec<u64>) -> RaftConfig {
        RaftConfig {
            node_id,
            group_id: 1,
            peers,
            learners: vec![],
            starts_as_learner: false,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }

    #[test]
    fn follower_rejects_old_term() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());
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

        let req = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
        };
        let resp = node.handle_request_vote(&req);
        assert!(resp.vote_granted);

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
    fn learner_rejects_vote_request() {
        let mut config = test_config(2, vec![1]);
        config.starts_as_learner = true;
        let mut node = RaftNode::new(config, MemStorage::new());
        assert_eq!(node.role(), NodeRole::Learner);

        let req = RequestVoteRequest {
            term: 5,
            candidate_id: 1,
            last_log_index: 10,
            last_log_term: 4,
            group_id: 1,
        };
        let resp = node.handle_request_vote(&req);
        assert!(
            !resp.vote_granted,
            "learner must never grant a vote, got {resp:?}"
        );
    }

    #[test]
    fn learner_accepts_append_entries_and_stays_learner() {
        let mut config = test_config(2, vec![1]);
        config.starts_as_learner = true;
        let mut node = RaftNode::new(config, MemStorage::new());

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                data: b"x".to_vec(),
            }],
            leader_commit: 1,
            group_id: 1,
        };

        let resp = node.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(node.commit_index(), 1);
        // Crucially, the learner did not turn into a Follower.
        assert_eq!(node.role(), NodeRole::Learner);
        assert_eq!(node.leader_id(), 1);
    }

    /// Learner AE responses update match_index but must NOT trigger a
    /// commit advancement that relies on the learner counting toward
    /// quorum.
    #[test]
    fn learner_ae_response_does_not_drive_commit() {
        // 3 voters + 1 learner cluster: quorum = 2. Without any voter ACK,
        // a learner "ack" must not advance commit_index.
        let mut config = test_config(1, vec![2, 3]);
        config.learners = vec![4];
        let mut node = RaftNode::new(config, MemStorage::new());

        // Force leader.
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        // Grant self-vote via two voter responses.
        let yes = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &yes);
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();

        // Propose an entry at index 2 (no-op is index 1).
        let idx = node.propose(b"cmd".to_vec()).unwrap();
        assert_eq!(idx, 2);
        let _ = node.take_ready();

        // Baseline: commit_index should still be <2 (no voter ACKs yet for index 2).
        let baseline_commit = node.commit_index();
        assert!(baseline_commit < 2);

        // Learner (peer 4) ACKs index 2. This must NOT advance commit.
        let ae_ok = AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: 2,
        };
        node.handle_append_entries_response(4, &ae_ok);
        assert_eq!(
            node.commit_index(),
            baseline_commit,
            "learner ACK must not contribute to commit quorum"
        );

        // Now a voter (peer 2) ACKs index 2. Quorum = 2 (self + peer 2) — commit advances.
        node.handle_append_entries_response(2, &ae_ok);
        assert_eq!(node.commit_index(), 2);
    }

    #[test]
    fn three_node_election() {
        let config1 = test_config(1, vec![2, 3]);
        let config2 = test_config(2, vec![1, 3]);
        let config3 = test_config(3, vec![1, 2]);

        let mut node1 = RaftNode::new(config1, MemStorage::new());
        let mut node2 = RaftNode::new(config2, MemStorage::new());
        let mut node3 = RaftNode::new(config3, MemStorage::new());

        node1.election_deadline = Instant::now() - Duration::from_millis(1);
        node1.tick();
        assert_eq!(node1.role(), NodeRole::Candidate);

        let ready = node1.take_ready();
        assert_eq!(ready.vote_requests.len(), 2);

        let resp2 = node2.handle_request_vote(&ready.vote_requests[0].1);
        let resp3 = node3.handle_request_vote(&ready.vote_requests[1].1);
        assert!(resp2.vote_granted);
        assert!(resp3.vote_granted);

        node1.handle_request_vote_response(2, &resp2);
        assert_eq!(node1.role(), NodeRole::Leader);
    }

    #[test]
    fn three_node_replication() {
        let config1 = test_config(1, vec![2, 3]);
        let config2 = test_config(2, vec![1, 3]);

        let mut node1 = RaftNode::new(config1, MemStorage::new());
        let mut node2 = RaftNode::new(config2, MemStorage::new());

        node1.election_deadline = Instant::now() - Duration::from_millis(1);
        node1.tick();
        let ready = node1.take_ready();
        let resp2 = node2.handle_request_vote(&ready.vote_requests[0].1);
        node1.handle_request_vote_response(2, &resp2);
        assert_eq!(node1.role(), NodeRole::Leader);

        let heartbeat_ready = node1.take_ready();
        for (peer_id, msg) in &heartbeat_ready.messages {
            if *peer_id == 2 {
                let resp = node2.handle_append_entries(msg);
                node1.handle_append_entries_response(2, &resp);
            }
        }

        let idx = node1.propose(b"cmd1".to_vec()).unwrap();
        assert_eq!(idx, 2);

        let ready = node1.take_ready();
        for (peer_id, msg) in &ready.messages {
            if *peer_id == 2 {
                let resp = node2.handle_append_entries(msg);
                assert!(resp.success);
                node1.handle_append_entries_response(2, &resp);
            }
        }

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

        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        let _ready = node.take_ready();
        let resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &resp);
        assert_eq!(node.role(), NodeRole::Leader);

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
}
