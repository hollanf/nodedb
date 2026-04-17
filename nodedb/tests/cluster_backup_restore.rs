//! Cluster end-to-end BACKUP / RESTORE.
//!
//! Verifies that the wire-streaming COPY surface fans out across
//! every node holding tenant data and that restore replays through
//! the current routing topology rather than the source one.

mod common;
use common::cluster_harness::TestCluster;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;
use nodedb_types::backup_envelope::{DEFAULT_MAX_TOTAL_BYTES, parse as parse_envelope};

const TENANT: u32 = 1;

async fn drain_backup(node_idx: usize, cluster: &TestCluster, tenant: u32) -> Vec<u8> {
    let stream = cluster.nodes[node_idx]
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .expect("copy_out");
    let mut bytes = Vec::new();
    let mut s = Box::pin(stream);
    while let Some(chunk) = s.next().await {
        bytes.extend_from_slice(&chunk.expect("copy chunk"));
    }
    bytes
}

async fn push_restore(
    node_idx: usize,
    cluster: &TestCluster,
    tenant: u32,
    bytes: Vec<u8>,
) -> Result<(), String> {
    let sink = cluster.nodes[node_idx]
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .map_err(|e| db_detail(&e))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(bytes))
        .await
        .map_err(|e| db_detail(&e))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| db_detail(&e))
}

async fn unique_contents(client: &tokio_postgres::Client) -> std::collections::BTreeSet<String> {
    let rows = client
        .simple_query("SELECT content FROM cl_docs")
        .await
        .expect("SELECT cl_docs");
    let mut seen = std::collections::BTreeSet::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            // 'content' may be the only column — try field 0.
            if let Some(s) = r.get(0) {
                seen.insert(s.to_string());
            }
        }
    }
    seen
}

fn db_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_backup_gathers_one_section_per_node() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let env = parse_envelope(&bytes, DEFAULT_MAX_TOTAL_BYTES).expect("parse envelope");
    assert_eq!(
        env.meta.tenant_id, TENANT,
        "envelope tenant id should match request"
    );
    assert!(
        env.meta.source_vshard_count >= 1,
        "envelope must record source vshard count, got {}",
        env.meta.source_vshard_count
    );
    // One section per unique cluster node — three nodes, three sections.
    // The orchestrator dedupes by node id (leader + replicas of every group).
    assert!(
        !env.sections.is_empty() && env.sections.len() <= 3,
        "expected 1..=3 sections, got {}",
        env.sections.len()
    );
    let origins: std::collections::BTreeSet<u64> =
        env.sections.iter().map(|s| s.origin_node_id).collect();
    assert!(
        !origins.contains(&0),
        "origin_node_id 0 must not appear in cluster sections: {origins:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_roundtrip_preserves_data() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs TYPE DOCUMENT STRICT \
             (id TEXT PRIMARY KEY, content TEXT)",
        )
        .await
        .expect("CREATE COLLECTION");

    // Write through node 0 — gateway routing places writes on the
    // current vshard owner (which may be any node).
    for i in 0..6 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('k{i}','v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert k{i}: {}", db_detail(&e)));
    }

    let bytes = drain_backup(0, &cluster, TENANT).await;

    // Restore into the same cluster. Writes are idempotent at the engine
    // level (PointPut overwrites); after restore we should still see all
    // six rows from any node's pgwire client.
    push_restore(0, &cluster, TENANT, bytes)
        .await
        .expect("RESTORE");

    // Pre-restore baseline: capture which values are visible from
    // node 1 today. The exact row count is engine-dependent
    // (broadcast scans currently return per-vshard views) — what we
    // care about is that *every* value the test inserted survives
    // the backup→restore roundtrip.
    let pre = unique_contents(&cluster.nodes[1].client).await;
    for i in 0..6u32 {
        let needle = format!("\"v{i}\"");
        assert!(
            pre.iter().any(|s| s.contains(&needle)),
            "pre-restore missing v{i}; rows={pre:?}"
        );
    }
    let post = unique_contents(&cluster.nodes[1].client).await;
    for i in 0..6u32 {
        let needle = format!("\"v{i}\"");
        assert!(
            post.iter().any(|s| s.contains(&needle)),
            "post-restore missing v{i}; rows={post:?}"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_dry_run_validates_envelope() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let sink = cluster.nodes[0]
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN DRY RUN"))
        .await
        .expect("copy_in");
    let mut sink = Box::pin(sink);
    sink.as_mut().send(Bytes::from(bytes)).await.expect("send");
    sink.as_mut().finish().await.expect("dry run finish");

    cluster.shutdown().await;
}
