//! Inbound Raft RPC dispatch — `impl RaftRpcHandler for RaftLoop`.
//!
//! This is the single entry point for every RPC that hits this node over the
//! QUIC transport. Each variant is either handled inline (Raft consensus RPCs
//! that just lock `MultiRaft`) or delegated to a helper module (health,
//! forwarding, VShard envelopes, cluster join).
//!
//! ## `JoinRequest` handling
//!
//! When a joining node sends a `JoinRequest`, this handler:
//!
//! 1. Looks up the current leader of the topology group (group 0) via
//!    `MultiRaft::group_statuses()`.
//! 2. If this node is **not** the group-0 leader, returns
//!    `JoinResponse { success: false, error: "not leader; retry at <addr>" }`
//!    where `<addr>` is the leader's address from the topology, so the
//!    client can redirect.
//! 3. If this node **is** the group-0 leader (or group 0 has no leader yet,
//!    which only happens on the founding bootstrap seed), takes
//!    `topology.write()` and calls `bootstrap::handle_join_request()` to
//!    (idempotently) admit the new node and produce the wire response.
//! 4. The routing table is cloned from `MultiRaft` under the same lock that
//!    reads the group statuses so the two stay consistent.

use tracing::{debug, warn};

use crate::bootstrap::handle_join_request;
use crate::error::{ClusterError, Result};
use crate::forward::RequestForwarder;
use crate::health;
use crate::multi_raft::GroupStatus;
use crate::routing::RoutingTable;
use crate::rpc_codec::{JoinRequest, JoinResponse, RaftRpc};
use crate::transport::RaftRpcHandler;

use super::loop_core::{CommitApplier, RaftLoop};

/// The Raft group that owns cluster topology / membership.
///
/// Group 0 is always the "metadata" group and is the authoritative source of
/// truth for who is in the cluster. Joins must be processed by its leader.
const TOPOLOGY_GROUP_ID: u64 = 0;

/// Outcome of the leader-check phase of `handle_join`.
///
/// Extracted as a pure enum so the decision logic can be unit-tested without
/// spinning up a real `MultiRaft` just to observe its leader id.
#[derive(Debug, PartialEq, Eq)]
enum JoinDecision {
    /// This node is the group-0 leader (or the founding seed with no leader
    /// elected yet). Admit the join locally.
    Admit,
    /// Another node is the group-0 leader. The client should retry at
    /// `leader_addr`.
    Redirect { leader_addr: String },
}

/// Pure decision: given the observed group-0 leader, this node's id, and the
/// leader's address (as known to the local topology), should we admit the
/// join or redirect?
///
/// - `group0_leader == 0` means "no elected leader yet". On a freshly
///   bootstrapped single-seed cluster this is normal — the founding node is
///   the only possible leader, so we accept.
/// - `group0_leader == self_node_id` means we are the leader — accept.
/// - Otherwise redirect. If the leader's address is unknown to topology (an
///   operator error that shouldn't happen in practice), we still redirect
///   with an empty string so the client at least sees the "not leader"
///   prefix and can decide to try the next seed.
fn decide_join(group0_leader: u64, self_node_id: u64, leader_addr: Option<String>) -> JoinDecision {
    if group0_leader == 0 || group0_leader == self_node_id {
        JoinDecision::Admit
    } else {
        JoinDecision::Redirect {
            leader_addr: leader_addr.unwrap_or_default(),
        }
    }
}

impl<A: CommitApplier, F: RequestForwarder> RaftRpcHandler for RaftLoop<A, F> {
    async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
        match rpc {
            // Raft consensus RPCs — lock MultiRaft (sync, never across await).
            RaftRpc::AppendEntriesRequest(req) => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let resp = mr.handle_append_entries(&req)?;
                Ok(RaftRpc::AppendEntriesResponse(resp))
            }
            RaftRpc::RequestVoteRequest(req) => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let resp = mr.handle_request_vote(&req)?;
                Ok(RaftRpc::RequestVoteResponse(resp))
            }
            RaftRpc::InstallSnapshotRequest(req) => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let resp = mr.handle_install_snapshot(&req)?;
                Ok(RaftRpc::InstallSnapshotResponse(resp))
            }
            // Cluster join.
            RaftRpc::JoinRequest(req) => Ok(RaftRpc::JoinResponse(self.handle_join(req))),
            // Health check.
            RaftRpc::Ping(req) => {
                let topo_version = {
                    let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
                    topo.version()
                };
                Ok(health::handle_ping(self.node_id, topo_version, &req))
            }
            // Topology broadcast.
            RaftRpc::TopologyUpdate(update) => {
                let (_updated, ack) =
                    health::handle_topology_update(self.node_id, &self.topology, &update);
                Ok(ack)
            }
            // Query forwarding — execute locally via the RequestForwarder.
            RaftRpc::ForwardRequest(req) => {
                let resp = self.forwarder.execute_forwarded(req).await;
                Ok(RaftRpc::ForwardResponse(resp))
            }
            // VShardEnvelope — dispatch to registered handler (Event Plane, etc.).
            RaftRpc::VShardEnvelope(bytes) => {
                if let Some(ref handler) = self.vshard_handler {
                    let response_bytes = handler(bytes).await?;
                    Ok(RaftRpc::VShardEnvelope(response_bytes))
                } else {
                    Err(ClusterError::Transport {
                        detail: "VShardEnvelope handler not configured".into(),
                    })
                }
            }
            other => Err(ClusterError::Transport {
                detail: format!("unexpected request type in RPC handler: {other:?}"),
            }),
        }
    }
}

impl<A: CommitApplier, F: RequestForwarder> RaftLoop<A, F> {
    /// Server-side `JoinRequest` handler — see module docs.
    ///
    /// Leader check + routing-clone is done under the `MultiRaft` lock so the
    /// leader id and the routing table are observed atomically. Topology
    /// mutation then happens under a separate `topology.write()` guard; the
    /// guard is dropped before the function returns so no async boundary is
    /// crossed while holding it.
    fn handle_join(&self, req: JoinRequest) -> JoinResponse {
        // Snapshot the group-0 status and a clone of the routing table under
        // a single MultiRaft lock. Cloning the routing table up front keeps
        // the lock scope small and makes the subsequent handle_join_request
        // call (which needs &RoutingTable) lock-free.
        let (group0_leader, routing): (u64, RoutingTable) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let routing = mr.routing().clone();
            let leader_id = mr
                .group_statuses()
                .into_iter()
                .find(|s: &GroupStatus| s.group_id == TOPOLOGY_GROUP_ID)
                .map(|s| s.leader_id)
                .unwrap_or(0);
            (leader_id, routing)
        };

        // Decide admit vs redirect. We look up the leader's address before
        // calling `decide_join` so the decision is a pure function of its
        // inputs — testable without any MultiRaft state.
        let leader_addr_hint = if group0_leader != 0 && group0_leader != self.node_id {
            self.topology
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get_node(group0_leader)
                .map(|n| n.addr.clone())
        } else {
            None
        };

        if let JoinDecision::Redirect { leader_addr } =
            decide_join(group0_leader, self.node_id, leader_addr_hint)
        {
            warn!(
                joining_node = req.node_id,
                leader_id = group0_leader,
                leader_addr = %leader_addr,
                "JoinRequest received on non-leader; redirecting"
            );
            return JoinResponse {
                success: false,
                error: format!("not leader; retry at {leader_addr}"),
                nodes: vec![],
                vshard_to_group: vec![],
                groups: vec![],
            };
        }

        // We are the leader (or the founding seed). Admit the node through
        // the shared `handle_join_request` function under a short-lived
        // topology write guard. The guard is dropped at end of block — we
        // do not await while holding it.
        let resp = {
            let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
            handle_join_request(&req, &mut topo, &routing)
        };

        debug!(
            joining_node = req.node_id,
            success = resp.success,
            "JoinRequest processed"
        );

        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_raft::MultiRaft;
    use crate::routing::RoutingTable;
    use crate::topology::{ClusterTopology, NodeInfo, NodeState};
    use crate::transport::NexarTransport;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    use nodedb_raft::message::LogEntry;

    /// No-op applier for tests that don't care about state machine output.
    struct NoopApplier;
    impl CommitApplier for NoopApplier {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            entries.last().map(|e| e.index).unwrap_or(0)
        }
    }

    fn make_transport(node_id: u64) -> Arc<NexarTransport> {
        Arc::new(NexarTransport::new(node_id, "127.0.0.1:0".parse().unwrap()).unwrap())
    }

    #[tokio::test]
    async fn rpc_handler_routes_append_entries() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        // Make it a leader first.
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, NoopApplier);

        // Tick to trigger election via a single pass.
        raft_loop.do_tick();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = RaftRpc::AppendEntriesRequest(nodedb_raft::AppendEntriesRequest {
            term: 99, // Higher term — will cause step-down.
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::AppendEntriesResponse(r) => {
                assert!(r.success);
                assert_eq!(r.term, 99);
            }
            other => panic!("expected AppendEntriesResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rpc_handler_routes_request_vote() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![2, 3]).unwrap();

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, NoopApplier);

        let req = RaftRpc::RequestVoteRequest(nodedb_raft::RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::RequestVoteResponse(r) => {
                assert!(r.vote_granted);
                assert_eq!(r.term, 1);
            }
            other => panic!("expected RequestVoteResponse, got {other:?}"),
        }
    }

    /// JoinRequest on a freshly-bootstrapped single-seed RaftLoop
    /// is admitted locally (we are the only possible leader; group 0 has no
    /// leader yet until the first tick, so the "leader_id == 0" path applies).
    #[tokio::test]
    async fn rpc_handler_accepts_join_on_bootstrap_seed() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(2, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        mr.add_group(1, vec![]).unwrap();

        // Seed topology with ourselves (matches what bootstrap_fn does).
        let mut topology = ClusterTopology::new();
        topology.add_node(NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        let topo = Arc::new(RwLock::new(topology));

        let raft_loop = RaftLoop::new(mr, transport, topo.clone(), NoopApplier);

        let req = RaftRpc::JoinRequest(JoinRequest {
            node_id: 2,
            listen_addr: "127.0.0.1:9401".into(),
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::JoinResponse(r) => {
                assert!(
                    r.success,
                    "join should succeed on bootstrap seed: {}",
                    r.error
                );
                assert_eq!(r.nodes.len(), 2);
                assert_eq!(r.groups.len(), 2);
                assert_eq!(r.vshard_to_group.len(), 1024);
            }
            other => panic!("expected JoinResponse, got {other:?}"),
        }

        // Topology must have been mutated.
        let topo_guard = topo.read().unwrap();
        assert_eq!(topo_guard.node_count(), 2);
        assert!(topo_guard.contains(2));
    }

    /// Decision logic: pure unit tests on the `decide_join` helper. The
    /// full end-to-end non-leader path (where a real MultiRaft reports a
    /// non-self leader after Raft election) is covered by the integration
    /// test at `tests/cluster_join.rs`.
    #[test]
    fn decide_join_self_leader_admits() {
        assert_eq!(
            decide_join(7, 7, Some("10.0.0.7:9400".into())),
            JoinDecision::Admit
        );
    }

    #[test]
    fn decide_join_no_leader_yet_admits() {
        // Fresh bootstrap seed — group 0 has no elected leader yet, accept.
        assert_eq!(decide_join(0, 7, None), JoinDecision::Admit);
    }

    #[test]
    fn decide_join_other_leader_redirects() {
        assert_eq!(
            decide_join(1, 7, Some("10.0.0.1:9400".into())),
            JoinDecision::Redirect {
                leader_addr: "10.0.0.1:9400".into()
            }
        );
    }

    #[test]
    fn decide_join_other_leader_unknown_addr_still_redirects() {
        // Leader id known but address missing from topology — still redirect
        // (operator can see the error and investigate) but with an empty
        // addr so the client code can decide whether to try next seed.
        assert_eq!(
            decide_join(1, 7, None),
            JoinDecision::Redirect {
                leader_addr: String::new()
            }
        );
    }
}
