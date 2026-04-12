//! Integration test: a restart in place from an existing catalog
//! is idempotent — the node re-registers with the same topology
//! entry, Raft groups come back with the same membership, and the
//! rest of the cluster is undisturbed.
//!
//! Exercises the `restart()` path in `bootstrap/restart.rs`, which
//! is what production does when a node crashes and systemd brings
//! it back.

mod common;

use std::time::Duration;

use common::{TestNode, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_restart_is_idempotent() {
    // Build a 3-node cluster first. Node 1 uses the owned-data-dir
    // path so we can inspect / reuse its catalog later; nodes 1 and
    // 3 use the default spawn. Node 2 is the one we'll restart.
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let seeds = vec![node1.listen_addr()];

    // The test owns node 2's data directory so it survives across
    // the shutdown-and-respawn cycle. `TestNode::spawn_with_data_dir`
    // reuses this path.
    let node2_data_dir = tempfile::tempdir().expect("create node 2 data dir");
    let node2 = TestNode::spawn_with_data_dir(2, node2_data_dir.path(), seeds.clone())
        .await
        .expect("node 2 join");

    let node3 = TestNode::spawn(3, seeds).await.expect("node 3 join");

    // Wait for the 3-node cluster to converge.
    let nodes_initial = [&node1, &node2, &node3];
    wait_for(
        "all 3 nodes converge on topology_size == 3",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || nodes_initial.iter().all(|n| n.topology_size() == 3),
    )
    .await;

    // Snapshot node 1 / 3's topology before the restart so we can
    // assert it's unchanged afterwards.
    let topo_before_1 = node1.topology_ids();
    let topo_before_3 = node3.topology_ids();
    assert_eq!(topo_before_1, vec![1, 2, 3]);
    assert_eq!(topo_before_3, vec![1, 2, 3]);

    // Gracefully shut node 2 down.
    node2.shutdown().await;

    // In-process restart limitation: `tick::do_tick` spawns detached
    // `tokio::spawn` tasks that hold `Arc<Mutex<MultiRaft>>` clones
    // for the lifetime of one `AppendEntries` round trip. Abort on
    // the run/serve handles does not propagate to those detached
    // tasks, so both the per-group redb raft logs **and** the
    // catalog `cluster.redb` stay locked by the in-process redb
    // handles for some window after `shutdown`. A real process
    // restart (systemd / ops) doesn't have this problem: the new
    // process has fresh fds.
    //
    // To simulate a process restart in-process, copy the data dir
    // contents byte-for-byte into a fresh location before the
    // respawn. The copy preserves the on-disk catalog state exactly
    // — `cluster.redb` with its topology + routing + cluster_id
    // stamps is copied over — so the respawn's `start_cluster` sees
    // `catalog.is_bootstrapped() == true` and takes the `restart()`
    // branch. The new files have fresh inodes, so redb opens them
    // without colliding with the old locks.
    let node2_restart_dir = tempfile::tempdir().expect("create restart dir");
    copy_dir_all(node2_data_dir.path(), node2_restart_dir.path())
        .expect("clone node 2 data dir for restart");

    // Short pause so any still-running detached tasks for the
    // original node 2 have a moment to wind down. Not strictly
    // required for correctness (the new paths are independent) but
    // gives cleaner test output if one of those tasks logs a
    // transport error.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Respawn node 2 from the cloned data directory. Because the
    // cloned catalog is already bootstrapped, `start_cluster` MUST
    // take the `restart()` path regardless of seed list — pass empty
    // seeds so this is unambiguous.
    let node2_restarted = TestNode::spawn_with_data_dir(2, node2_restart_dir.path(), vec![])
        .await
        .expect("node 2 restart from catalog");

    // The restarted node must load its catalog-persisted topology
    // (3 members), not a fresh bootstrap.
    assert_eq!(
        node2_restarted.topology_size(),
        3,
        "restarted node 2 should load 3 members from catalog, got {}",
        node2_restarted.topology_size()
    );
    assert_eq!(node2_restarted.topology_ids(), vec![1, 2, 3]);

    // Node 1 and 3's topology must be unchanged — no duplicate
    // entries, no "new" node 2 at a different address. Collisions
    // would show up as a node_count != 3 or as topology_ids
    // containing a stray id.
    assert_eq!(
        node1.topology_ids(),
        topo_before_1,
        "node 1 topology must be unchanged across node 2's restart"
    );
    assert_eq!(
        node3.topology_ids(),
        topo_before_3,
        "node 3 topology must be unchanged across node 2's restart"
    );

    // Lifecycle: the restart path transitions to Restarting first,
    // then the test helper's caller-owned `to_ready` fires.
    assert!(
        matches!(
            node2_restarted.lifecycle_state(),
            nodedb_cluster::ClusterLifecycleState::Ready { nodes: 3 }
        ),
        "restarted node 2 should be Ready{{nodes:3}}, got {:?}",
        node2_restarted.lifecycle_state()
    );

    node2_restarted.shutdown().await;
    node3.shutdown().await;
    node1.shutdown().await;
    drop(node2_restart_dir);
    drop(node2_data_dir);
}

/// Recursive byte-for-byte directory copy. Used to clone the
/// original node 2 data directory into a fresh location before the
/// restart so redb doesn't fight with its own still-alive in-process
/// locks on the original files.
fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dst_path = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&entry.path(), &dst_path)?;
        } else {
            std::fs::copy(entry.path(), dst_path)?;
        }
    }
    Ok(())
}
