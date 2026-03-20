//! Cluster bootstrap and join protocol.
//!
//! Three startup paths:
//!
//! 1. **Bootstrap**: First seed node — creates topology, routing table, Raft groups,
//!    persists to catalog. The cluster is born.
//!
//! 2. **Join**: New node contacts a seed, receives full cluster state via
//!    `JoinResponse`, persists, and registers peers.
//!
//! 3. **Restart**: Node loads topology + routing from catalog, reconnects to
//!    known peers.

use std::net::SocketAddr;
use tracing::{info, warn};

use crate::catalog::ClusterCatalog;
use crate::error::{ClusterError, Result};
use crate::multi_raft::MultiRaft;
use crate::routing::{GroupInfo, RoutingTable};
use crate::rpc_codec::{JoinGroupInfo, JoinNodeInfo, JoinRequest, JoinResponse, RaftRpc};
use crate::topology::{ClusterTopology, NodeInfo, NodeState};
use crate::transport::NexarTransport;

/// Configuration for cluster formation.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's unique ID.
    pub node_id: u64,
    /// Address to listen on for Raft RPCs.
    pub listen_addr: SocketAddr,
    /// Seed node addresses for bootstrap/join.
    pub seed_nodes: Vec<SocketAddr>,
    /// Number of Raft groups to create on bootstrap.
    pub num_groups: u64,
    /// Replication factor (number of replicas per group).
    pub replication_factor: usize,
    /// Data directory for persistent Raft log storage.
    pub data_dir: std::path::PathBuf,
}

/// Result of cluster startup — everything needed to run the Raft loop.
pub struct ClusterState {
    pub topology: ClusterTopology,
    pub routing: RoutingTable,
    pub multi_raft: MultiRaft,
}

/// Start the cluster — bootstrap, join, or restart depending on state.
///
/// Returns the initialized cluster state ready for the Raft loop.
pub async fn start_cluster(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    // Check if we have existing state.
    if catalog.is_bootstrapped()? {
        return restart(config, catalog, transport);
    }

    // No existing state — try bootstrap or join.
    let is_seed = config.seed_nodes.contains(&config.listen_addr);

    if is_seed && should_bootstrap(config, transport).await {
        bootstrap(config, catalog)
    } else {
        join(config, catalog, transport).await
    }
}

/// Check if this seed should bootstrap a new cluster.
///
/// A seed bootstraps if no other seed is already running.
async fn should_bootstrap(config: &ClusterConfig, transport: &NexarTransport) -> bool {
    for addr in &config.seed_nodes {
        if *addr == config.listen_addr {
            continue;
        }
        // Try to contact another seed.
        let probe = RaftRpc::JoinRequest(JoinRequest {
            node_id: config.node_id,
            listen_addr: config.listen_addr.to_string(),
        });
        match transport.send_rpc_to_addr(*addr, probe).await {
            Ok(_) => return false, // Another seed is alive — join instead.
            Err(_) => continue,    // Seed not reachable — keep checking.
        }
    }
    // No other seed responded — we bootstrap.
    true
}

// ── Bootstrap ───────────────────────────────────────────────────────

/// Bootstrap a new cluster: this node is the founding member.
fn bootstrap(config: &ClusterConfig, catalog: &ClusterCatalog) -> Result<ClusterState> {
    info!(
        node_id = config.node_id,
        addr = %config.listen_addr,
        groups = config.num_groups,
        "bootstrapping new cluster"
    );

    // Create topology with this node.
    let mut topology = ClusterTopology::new();
    topology.add_node(NodeInfo::new(
        config.node_id,
        config.listen_addr,
        NodeState::Active,
    ));

    // Create routing table: all groups on this single node.
    let routing = RoutingTable::uniform(
        config.num_groups,
        &[config.node_id],
        config.replication_factor.min(1), // Single node → RF=1.
    );

    // Create MultiRaft with all groups (single-node, no peers).
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone());
    for group_id in routing.group_ids() {
        multi_raft.add_group(group_id, vec![])?;
    }

    // Generate cluster ID and persist everything.
    let cluster_id = generate_cluster_id();
    catalog.save_cluster_id(cluster_id)?;
    catalog.save_topology(&topology)?;
    catalog.save_routing(&routing)?;

    info!(
        node_id = config.node_id,
        cluster_id,
        groups = config.num_groups,
        "cluster bootstrapped"
    );

    Ok(ClusterState {
        topology,
        routing,
        multi_raft,
    })
}

// ── Join ────────────────────────────────────────────────────────────

/// Join an existing cluster by contacting seed nodes.
async fn join(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    info!(
        node_id = config.node_id,
        seeds = ?config.seed_nodes,
        "joining existing cluster"
    );

    let req = RaftRpc::JoinRequest(JoinRequest {
        node_id: config.node_id,
        listen_addr: config.listen_addr.to_string(),
    });

    // Try each seed until one accepts.
    let mut last_err = None;
    for addr in &config.seed_nodes {
        match transport.send_rpc_to_addr(*addr, req.clone()).await {
            Ok(RaftRpc::JoinResponse(resp)) => {
                if !resp.success {
                    last_err = Some(ClusterError::Transport {
                        detail: format!("join rejected by {addr}: {}", resp.error),
                    });
                    continue;
                }
                return apply_join_response(config, catalog, transport, &resp);
            }
            Ok(other) => {
                last_err = Some(ClusterError::Transport {
                    detail: format!("unexpected response from {addr}: {other:?}"),
                });
            }
            Err(e) => {
                warn!(%addr, error = %e, "seed unreachable");
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| ClusterError::Transport {
        detail: "no seed nodes configured".into(),
    }))
}

/// Apply a JoinResponse: reconstruct topology, routing, and MultiRaft from wire data.
fn apply_join_response(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
    resp: &JoinResponse,
) -> Result<ClusterState> {
    // Reconstruct topology.
    let mut topology = ClusterTopology::new();
    for node in &resp.nodes {
        let state = NodeState::from_u8(node.state).unwrap_or(NodeState::Active);
        let mut info = NodeInfo {
            node_id: node.node_id,
            addr: node.addr.clone(),
            state,
            raft_groups: node.raft_groups.clone(),
        };
        // If this is us, mark as Active.
        if node.node_id == config.node_id {
            info.state = NodeState::Active;
        }
        topology.add_node(info);
    }

    // Reconstruct routing table.
    let mut group_members = std::collections::HashMap::new();
    for g in &resp.groups {
        group_members.insert(
            g.group_id,
            GroupInfo {
                leader: g.leader,
                members: g.members.clone(),
            },
        );
    }
    let routing = RoutingTable::from_parts(resp.vshard_to_group.clone(), group_members);

    // Create MultiRaft — join the groups that include this node.
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone());
    for g in &resp.groups {
        if g.members.contains(&config.node_id) {
            let peers: Vec<u64> = g
                .members
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group(g.group_id, peers)?;
        }
    }

    // Register all peers in the transport.
    for node in &resp.nodes {
        if node.node_id != config.node_id
            && let Ok(addr) = node.addr.parse::<SocketAddr>()
        {
            transport.register_peer(node.node_id, addr);
        }
    }

    // Persist.
    catalog.save_topology(&topology)?;
    catalog.save_routing(&routing)?;

    info!(
        node_id = config.node_id,
        nodes = topology.node_count(),
        groups = routing.num_groups(),
        "joined cluster"
    );

    Ok(ClusterState {
        topology,
        routing,
        multi_raft,
    })
}

// ── Restart ─────────────────────────────────────────────────────────

/// Restart from persisted state — load topology and routing from catalog.
fn restart(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    let topology = catalog
        .load_topology()?
        .ok_or_else(|| ClusterError::Transport {
            detail: "catalog is bootstrapped but topology is missing".into(),
        })?;

    let routing = catalog
        .load_routing()?
        .ok_or_else(|| ClusterError::Transport {
            detail: "catalog is bootstrapped but routing table is missing".into(),
        })?;

    // Reconstruct MultiRaft from routing table.
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone());
    for (group_id, info) in routing.group_members() {
        if info.members.contains(&config.node_id) {
            let peers: Vec<u64> = info
                .members
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group(*group_id, peers)?;
        }
    }

    // Register all known peers in the transport.
    for node in topology.all_nodes() {
        if node.node_id != config.node_id
            && let Some(addr) = node.socket_addr()
        {
            transport.register_peer(node.node_id, addr);
        }
    }

    info!(
        node_id = config.node_id,
        nodes = topology.node_count(),
        groups = multi_raft.group_count(),
        "restarted from catalog"
    );

    Ok(ClusterState {
        topology,
        routing,
        multi_raft,
    })
}

// ── Join request handler ────────────────────────────────────────────

/// Build a JoinResponse from current cluster state.
///
/// Called by the RPC handler on the seed/leader node when a JoinRequest arrives.
pub fn handle_join_request(
    req: &JoinRequest,
    topology: &mut ClusterTopology,
    routing: &RoutingTable,
) -> JoinResponse {
    // Add the new node to topology.
    let addr: SocketAddr = match req.listen_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            return JoinResponse {
                success: false,
                error: format!("invalid listen_addr '{}': {e}", req.listen_addr),
                nodes: vec![],
                vshard_to_group: vec![],
                groups: vec![],
            };
        }
    };

    if topology.contains(req.node_id) {
        // Node already known — update address if changed.
        if let Some(existing) = topology.get_node_mut(req.node_id) {
            existing.addr = req.listen_addr.clone();
            existing.state = NodeState::Active;
        }
    } else {
        topology.add_node(NodeInfo::new(req.node_id, addr, NodeState::Active));
    }

    // Build wire response.
    let nodes: Vec<JoinNodeInfo> = topology
        .all_nodes()
        .map(|n| JoinNodeInfo {
            node_id: n.node_id,
            addr: n.addr.clone(),
            state: n.state.as_u8(),
            raft_groups: n.raft_groups.clone(),
        })
        .collect();

    let groups: Vec<JoinGroupInfo> = routing
        .group_members()
        .iter()
        .map(|(&gid, info)| JoinGroupInfo {
            group_id: gid,
            leader: info.leader,
            members: info.members.clone(),
        })
        .collect();

    JoinResponse {
        success: true,
        error: String::new(),
        nodes,
        vshard_to_group: routing.vshard_to_group().to_vec(),
        groups,
    }
}

/// Generate a unique cluster ID (random u64).
fn generate_cluster_id() -> u64 {
    use rand::Rng;
    rand::rng().random::<u64>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ClusterCatalog;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[test]
    fn bootstrap_creates_cluster() {
        let (_dir, catalog) = temp_catalog();
        let config = ClusterConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:9400".parse().unwrap(),
            seed_nodes: vec!["127.0.0.1:9400".parse().unwrap()],
            num_groups: 4,
            replication_factor: 1,
            data_dir: _dir.path().to_path_buf(),
        };

        let state = bootstrap(&config, &catalog).unwrap();

        assert_eq!(state.topology.node_count(), 1);
        assert_eq!(state.topology.active_nodes().len(), 1);
        assert_eq!(state.routing.num_groups(), 4);
        assert_eq!(state.multi_raft.group_count(), 4);

        // Verify persistence.
        assert!(catalog.is_bootstrapped().unwrap());
        let loaded_topo = catalog.load_topology().unwrap().unwrap();
        assert_eq!(loaded_topo.node_count(), 1);
        let loaded_rt = catalog.load_routing().unwrap().unwrap();
        assert_eq!(loaded_rt.num_groups(), 4);
    }

    #[tokio::test]
    async fn restart_from_catalog() {
        let (_dir, catalog) = temp_catalog();
        let config = ClusterConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:9400".parse().unwrap(),
            seed_nodes: vec![],
            num_groups: 4,
            replication_factor: 1,
            data_dir: _dir.path().to_path_buf(),
        };

        // Bootstrap first.
        let _ = bootstrap(&config, &catalog).unwrap();

        // Create transport for restart.
        let transport = NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap();

        // Restart — should load from catalog.
        let state = restart(&config, &catalog, &transport).unwrap();

        assert_eq!(state.topology.node_count(), 1);
        assert_eq!(state.routing.num_groups(), 4);
        assert_eq!(state.multi_raft.group_count(), 4);
    }

    #[test]
    fn handle_join_request_adds_node() {
        let mut topology = ClusterTopology::new();
        topology.add_node(NodeInfo::new(
            1,
            "10.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));

        let routing = RoutingTable::uniform(2, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
        };

        let resp = handle_join_request(&req, &mut topology, &routing);

        assert!(resp.success);
        assert_eq!(resp.nodes.len(), 2);
        assert_eq!(resp.vshard_to_group.len(), 1024);
        assert_eq!(resp.groups.len(), 2);

        // Topology should now contain node 2.
        assert!(topology.contains(2));
        assert_eq!(topology.node_count(), 2);
    }

    #[test]
    fn handle_join_request_idempotent() {
        let mut topology = ClusterTopology::new();
        topology.add_node(NodeInfo::new(
            1,
            "10.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));

        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
        };

        // Join twice — should be idempotent.
        let _ = handle_join_request(&req, &mut topology, &routing);
        let resp = handle_join_request(&req, &mut topology, &routing);

        assert!(resp.success);
        assert_eq!(resp.nodes.len(), 2); // Still 2, not 3.
    }

    #[test]
    fn handle_join_invalid_addr() {
        let mut topology = ClusterTopology::new();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "not-a-valid-address".into(),
        };

        let resp = handle_join_request(&req, &mut topology, &routing);
        assert!(!resp.success);
        assert!(!resp.error.is_empty());
    }

    #[tokio::test]
    async fn full_bootstrap_join_flow() {
        // Node 1 bootstraps, Node 2 joins via QUIC.
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        let t1 = Arc::new(NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap());
        let t2 = Arc::new(NexarTransport::new(2, "127.0.0.1:0".parse().unwrap()).unwrap());

        let (_dir1, catalog1) = temp_catalog();
        let (_dir2, catalog2) = temp_catalog();

        let addr1 = t1.local_addr();
        let addr2 = t2.local_addr();

        // Bootstrap node 1.
        let config1 = ClusterConfig {
            node_id: 1,
            listen_addr: addr1,
            seed_nodes: vec![addr1],
            num_groups: 2,
            replication_factor: 1,
            data_dir: _dir1.path().to_path_buf(),
        };
        let state1 = bootstrap(&config1, &catalog1).unwrap();

        // Set up a handler for node 1 that handles JoinRequests.
        let topology1 = Arc::new(Mutex::new(state1.topology));
        let routing1 = Arc::new(state1.routing);

        struct JoinHandler {
            topology: Arc<Mutex<ClusterTopology>>,
            routing: Arc<RoutingTable>,
        }

        impl crate::transport::RaftRpcHandler for JoinHandler {
            async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
                match rpc {
                    RaftRpc::JoinRequest(req) => {
                        let mut topo = self.topology.lock().unwrap();
                        let resp = handle_join_request(&req, &mut topo, &self.routing);
                        Ok(RaftRpc::JoinResponse(resp))
                    }
                    other => Err(ClusterError::Transport {
                        detail: format!("unexpected: {other:?}"),
                    }),
                }
            }
        }

        let handler = Arc::new(JoinHandler {
            topology: topology1.clone(),
            routing: routing1.clone(),
        });

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let t1c = t1.clone();
        tokio::spawn(async move {
            t1c.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(30)).await;

        // Node 2 joins.
        let config2 = ClusterConfig {
            node_id: 2,
            listen_addr: addr2,
            seed_nodes: vec![addr1],
            num_groups: 2,
            replication_factor: 1,
            data_dir: _dir2.path().to_path_buf(),
        };

        let state2 = join(&config2, &catalog2, &t2).await.unwrap();

        assert_eq!(state2.topology.node_count(), 2);
        assert_eq!(state2.routing.num_groups(), 2);

        // Verify node 2's state was persisted.
        assert!(catalog2.load_topology().unwrap().is_some());
        assert!(catalog2.load_routing().unwrap().is_some());

        // Verify node 1's topology was updated.
        let topo1 = topology1.lock().unwrap();
        assert_eq!(topo1.node_count(), 2);
        assert!(topo1.contains(2));

        shutdown_tx.send(true).unwrap();
    }
}
