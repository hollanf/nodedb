//! Shared integration-test helpers: a full in-process `TestNode`
//! that owns a transport, catalog, topology, lifecycle tracker, and
//! the two background tasks (`serve`, `run`) a production-shaped
//! node needs.
//!
//! Every integration test file under `nodedb-cluster/tests/` imports
//! this module via `mod common;` — by convention the subdirectory
//! form (`tests/common/mod.rs`) is NOT picked up by cargo as a test
//! crate itself, so it can contain the shared scaffolding without
//! showing up in the test harness.
//!
//! Three construction entry points:
//!
//! - [`TestNode::spawn`] — fresh transport + fresh temp data dir.
//!   The original "just give me a node" path used by the simple
//!   bootstrap+join tests.
//! - [`TestNode::spawn_with_transport`] — caller provides a
//!   pre-bound `NexarTransport` so it can learn every node's
//!   ephemeral address before any `start_cluster` call. Needed by
//!   the race test, where all five seeds must exist in the config
//!   of every node at startup.
//! - [`TestNode::spawn_with_data_dir`] — caller owns the data
//!   directory as a `PathBuf` (keeping the `TempDir` alive in the
//!   test scope) and the `TestNode` reuses it. Needed by the
//!   idempotent restart test where the same catalog must survive
//!   across two distinct `TestNode` lifetimes.

#![allow(dead_code)] // Not every test file uses every helper.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use nodedb_cluster::{
    ClusterCatalog, ClusterConfig, ClusterLifecycleState, ClusterLifecycleTracker, ClusterTopology,
    NexarTransport, RaftLoop, start_cluster,
};
use nodedb_raft::message::LogEntry;
use tempfile::TempDir;
use tokio::sync::watch;

/// No-op `CommitApplier` — we care about cluster formation, not
/// state machine application.
pub struct NoopApplier;

impl nodedb_cluster::CommitApplier for NoopApplier {
    fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
        entries.last().map(|e| e.index).unwrap_or(0)
    }
}

/// Everything one cluster node needs to run in-process for a test.
pub struct TestNode {
    /// Owns the temp dir when we created it ourselves. `None` when
    /// the test owns the directory via its own `TempDir` handle
    /// (see `spawn_with_data_dir`).
    _data_dir: Option<TempDir>,
    /// Always-valid path to the data directory, whether we own the
    /// underlying `TempDir` or the test does.
    pub data_dir_path: PathBuf,
    /// Held so the redb file stays open for the node's lifetime.
    _catalog: Arc<ClusterCatalog>,
    pub transport: Arc<NexarTransport>,
    pub topology: Arc<RwLock<ClusterTopology>>,
    pub lifecycle: ClusterLifecycleTracker,
    pub node_id: u64,
    shutdown_tx: watch::Sender<bool>,
    serve_handle: tokio::task::JoinHandle<()>,
    run_handle: tokio::task::JoinHandle<()>,
}

impl TestNode {
    /// Fresh transport + fresh temp data dir. Convenience for tests
    /// that don't care about address or lifetime control.
    pub async fn spawn(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let transport = Arc::new(NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse().unwrap(),
        )?);
        Self::spawn_with_transport(node_id, transport, seed_nodes).await
    }

    /// Use a pre-bound transport so the caller knows the listen
    /// address before start_cluster runs. Fresh temp data dir.
    pub async fn spawn_with_transport(
        node_id: u64,
        transport: Arc<NexarTransport>,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_dir = tempfile::tempdir()?;
        let data_dir_path = data_dir.path().to_path_buf();
        Self::spawn_inner(
            node_id,
            transport,
            seed_nodes,
            data_dir_path,
            Some(data_dir),
        )
        .await
    }

    /// Reuse an existing data directory. The caller is responsible
    /// for keeping the directory alive (typically by holding a
    /// `TempDir` in the test's own scope).
    ///
    /// Used to test the `restart()` path: spawn → shutdown → spawn
    /// again on the same data dir, verifying the catalog is
    /// authoritative and no duplicate topology entries or Raft group
    /// disruption occurs.
    pub async fn spawn_with_data_dir(
        node_id: u64,
        data_dir: &Path,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let transport = Arc::new(NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse().unwrap(),
        )?);
        Self::spawn_inner(node_id, transport, seed_nodes, data_dir.to_path_buf(), None).await
    }

    async fn spawn_inner(
        node_id: u64,
        transport: Arc<NexarTransport>,
        seed_nodes: Vec<SocketAddr>,
        data_dir_path: PathBuf,
        owned_data_dir: Option<TempDir>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let catalog = Arc::new(ClusterCatalog::open(&data_dir_path.join("cluster.redb"))?);
        let listen_addr = transport.local_addr();

        // Empty seeds → imply single-node bootstrap by listing only
        // our own address. Otherwise use whatever the caller supplied.
        let seeds = if seed_nodes.is_empty() {
            vec![listen_addr]
        } else {
            seed_nodes
        };

        let config = ClusterConfig {
            node_id,
            listen_addr,
            seed_nodes: seeds,
            num_groups: 2,
            replication_factor: 3,
            data_dir: data_dir_path.clone(),
            force_bootstrap: false,
        };

        let lifecycle = ClusterLifecycleTracker::new();
        let state = start_cluster(&config, &catalog, &transport, &lifecycle).await?;
        // Match the main binary: the caller is responsible for the
        // final `Ready` transition once the node is wired up. The
        // node count is whatever the node observed at the moment of
        // transition.
        lifecycle.to_ready(state.topology.node_count());

        let topology = Arc::new(RwLock::new(state.topology));
        let raft_loop = Arc::new(
            RaftLoop::new(
                state.multi_raft,
                transport.clone(),
                topology.clone(),
                NoopApplier,
            )
            // Attach the catalog so the server-side `join_flow` can
            // read `cluster_id` from it and echo it back on every
            // `JoinResponse`. Without this, joined nodes persist
            // `cluster_id = 0` which still makes `is_bootstrapped`
            // return true but loses the real cluster identity. The
            // catalog is also what the flow's §2.4 persist step
            // writes to after a successful `AddLearner` commit.
            .with_catalog(catalog.clone()),
        );

        // transport.serve(raft_loop) runs the inbound RPC handler
        // end-to-end — the production code path this whole harness
        // exists to exercise.
        let (shutdown_tx, shutdown_rx_serve) = watch::channel(false);
        let shutdown_rx_run = shutdown_tx.subscribe();

        let transport_for_serve = transport.clone();
        let handler_for_serve = raft_loop.clone();
        let serve_handle = tokio::spawn(async move {
            let _ = transport_for_serve
                .serve(handler_for_serve, shutdown_rx_serve)
                .await;
        });

        let run_handle = tokio::spawn(async move {
            raft_loop.run(shutdown_rx_run).await;
        });

        Ok(Self {
            _data_dir: owned_data_dir,
            data_dir_path,
            _catalog: catalog,
            transport,
            topology,
            lifecycle,
            node_id,
            shutdown_tx,
            serve_handle,
            run_handle,
        })
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.transport.local_addr()
    }

    pub fn topology_size(&self) -> usize {
        self.topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .node_count()
    }

    pub fn lifecycle_state(&self) -> ClusterLifecycleState {
        self.lifecycle.current()
    }

    pub fn topology_contains(&self, node_id: u64) -> bool {
        self.topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(node_id)
    }

    /// Collect every node_id this node knows about, sorted.
    /// Stable ordering lets tests compare cluster membership
    /// between nodes with a single `assert_eq!`.
    pub fn topology_ids(&self) -> Vec<u64> {
        let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
        let mut ids: Vec<u64> = topo.all_nodes().map(|n| n.node_id).collect();
        ids.sort_unstable();
        ids
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        // Abort the tasks in addition to signalling shutdown so they
        // drop their captures (the `Arc<RaftLoop>` carrying the
        // `MultiRaft` + per-group redb file handles) immediately
        // instead of waiting for `transport.serve` / `raft_loop.run`
        // to notice the shutdown signal on their next poll.
        self.serve_handle.abort();
        self.run_handle.abort();
        let _ = self.serve_handle.await;
        let _ = self.run_handle.await;
    }
}

/// Poll a predicate up to `deadline`, sleeping `step` between
/// attempts. Panics with a descriptive message if the predicate
/// never holds.
pub async fn wait_for<F: FnMut() -> bool>(
    desc: &str,
    deadline: Duration,
    step: Duration,
    mut pred: F,
) {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(step).await;
    }
    panic!("timed out after {:?} waiting for: {}", deadline, desc);
}
