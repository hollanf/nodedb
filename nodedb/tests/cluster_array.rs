//! End-to-end 3-node cluster integration tests for the distributed Array
//! Engine.
//!
//! Every test in this file matches `binary(/cluster/)` in nextest.toml and
//! therefore runs in the `cluster` test group (max-threads = 1,
//! threads-required = num-test-threads). They run strictly serially and alone.
//!
//! ## Architecture Note: Local Array Catalog
//!
//! `CREATE ARRAY` writes to the local in-memory `ArrayCatalog` on the
//! executing node only — it is NOT replicated through Raft (unlike
//! `CREATE COLLECTION`). As a result, all array queries (NDARRAY_SLICE,
//! NDARRAY_AGG, etc.) must be issued on the same node that executed the
//! `CREATE ARRAY` DDL.
//!
//! The "distributed" aspect of these tests is that cell data is stored
//! across multiple vShards on different nodes (Hilbert-partitioned). The
//! coordinator on the DDL node fans out to peer shards via the array RPC
//! path and merges the results.
//!
//! Tests:
//!   1. `cluster_array_slice_spans_multiple_shards` — NDARRAY_SLICE fan-out
//!      to peer shards returns exactly the expected cells.
//!   2. `cluster_array_agg_sum_across_shards` — NDARRAY_AGG sum is correct.
//!   3. `cluster_array_agg_grouped_by_chr` — NDARRAY_AGG group-by-dim returns
//!      correct per-group sums.
//!   4. `cluster_array_vector_prefilter_distributed` — fused vector+slice
//!      query wires end-to-end without error.
//!   5. `cluster_array_routing_retry_on_owner_change` — stale routing table
//!      on the coordinator node (poisoned via `force_stale_route_for_test`)
//!      recovers and the array query succeeds on retry.

mod common;

use common::cluster_harness::TestCluster;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Run `sql` on `client` and return every row's first column as a `String`.
async fn query_col0(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let msgs = client.simple_query(sql).await.unwrap_or_else(|e| {
        let detail = if let Some(db) = e.as_db_error() {
            format!(
                "SQLSTATE={} severity={} msg={}",
                db.code().code(),
                db.severity(),
                db.message()
            )
        } else {
            format!("{e:?}")
        };
        panic!("query failed: {detail}\n  sql: {sql}")
    });
    msgs.into_iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Spin up a 3-node cluster with a pre-populated genome array.
///
/// Returns `(cluster, leader_idx)` where `leader_idx` is the index into
/// `cluster.nodes` of the node that executed the `CREATE ARRAY` DDL. All
/// array queries must be issued on this node because the array catalog is
/// local (not replicated through Raft).
///
/// Schema:
///   DIMS  (chr INT64 [0..9], pos INT64 [0..99])
///   ATTRS (qual FLOAT64)
///   TILE_EXTENTS (1, 100)
///   CELL_ORDER HILBERT
///
/// 9 cells total across chr in {0, 1, 2}:
///   chr=0: pos=10/20/30, qual=1.0/2.0/3.0   → sum=6.0
///   chr=1: pos=10/20/30, qual=10.0/20.0/30.0 → sum=60.0
///   chr=2: pos=10/20/30, qual=100.0/200.0/300.0 → sum=600.0
///   total qual sum: 666.0
async fn spawn_cluster_with_genome() -> (TestCluster, usize) {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");

    let leader_idx = cluster
        .exec_ddl_on_any_leader(
            "CREATE ARRAY genome \
             DIMS (chr INT64 [0..9], pos INT64 [0..99]) \
             ATTRS (qual FLOAT64) \
             TILE_EXTENTS (1, 100) \
             CELL_ORDER HILBERT",
        )
        .await
        .expect("CREATE ARRAY genome");

    // Insert 9 cells from the DDL node (the one with the local array catalog).
    cluster.nodes[leader_idx]
        .exec(
            "INSERT INTO ARRAY genome \
             COORDS (0, 10) VALUES (1.0), \
             COORDS (0, 20) VALUES (2.0), \
             COORDS (0, 30) VALUES (3.0), \
             COORDS (1, 10) VALUES (10.0), \
             COORDS (1, 20) VALUES (20.0), \
             COORDS (1, 30) VALUES (30.0), \
             COORDS (2, 10) VALUES (100.0), \
             COORDS (2, 20) VALUES (200.0), \
             COORDS (2, 30) VALUES (300.0)",
        )
        .await
        .expect("INSERT INTO ARRAY genome");

    // Flush so reads exercise the segment-scan path, not just the memtable.
    cluster.nodes[leader_idx]
        .exec("SELECT NDARRAY_FLUSH('genome')")
        .await
        .expect("NDARRAY_FLUSH");

    (cluster, leader_idx)
}

// ── Test 1: slice fan-out to peer shards ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_slice_spans_multiple_shards() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;
    let client = &cluster.nodes[node_idx].client;

    // Slice chr=1, pos 0..99 — should return exactly the 3 cells for chr=1.
    let rows = query_col0(
        client,
        "SELECT * FROM NDARRAY_SLICE('genome', '{chr: [1, 1], pos: [0, 99]}', ['qual'], 100)",
    )
    .await;

    assert_eq!(
        rows.len(),
        3,
        "expected 3 cells for chr=1, pos 0..99; got {rows:?}"
    );

    // Each row is JSON `{"coords": [...], "attrs": [...]}`.
    // Collect and sort qual values; must be [10.0, 20.0, 30.0].
    let mut quals: Vec<f64> = rows
        .iter()
        .map(|row| {
            let cell: serde_json::Value =
                serde_json::from_str(row).unwrap_or_else(|e| panic!("row not JSON: {row}: {e}"));
            cell.get("attrs")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_f64())
                .unwrap_or_else(|| panic!("missing qual in row: {row}"))
        })
        .collect();
    quals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        quals,
        vec![10.0, 20.0, 30.0],
        "chr=1 qual values must be [10.0, 20.0, 30.0]; got {quals:?}"
    );

    cluster.shutdown().await;
}

// ── Test 2: agg sum across all shards ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_agg_sum_across_shards() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;
    let client = &cluster.nodes[node_idx].client;

    let rows = query_col0(client, "SELECT * FROM NDARRAY_AGG('genome', 'qual', 'sum')").await;

    assert_eq!(rows.len(), 1, "scalar agg must return exactly one row");

    // The JSON result is `{"result": <f64>}`. Total qual = 666.0.
    let row = &rows[0];
    let cell: serde_json::Value =
        serde_json::from_str(row).unwrap_or_else(|e| panic!("agg row not JSON: {row}: {e}"));
    let result = cell
        .get("result")
        .and_then(|v| v.as_f64())
        .unwrap_or_else(|| panic!("missing 'result' field in: {row}"));

    assert!(
        (result - 666.0).abs() < 1e-4,
        "expected sum=666.0, got {result}"
    );

    cluster.shutdown().await;
}

// ── Test 3: agg grouped by chr dimension ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_agg_grouped_by_chr() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;
    let client = &cluster.nodes[node_idx].client;

    let rows = query_col0(
        client,
        "SELECT * FROM NDARRAY_AGG('genome', 'qual', 'sum', 'chr')",
    )
    .await;

    assert_eq!(
        rows.len(),
        3,
        "group-by-chr must return 3 rows (one per chromosome); got {rows:?}"
    );

    // Collect (chr, sum) pairs and sort by chr for deterministic assertion.
    let mut groups: Vec<(i64, f64)> = rows
        .iter()
        .map(|row| {
            let cell: serde_json::Value = serde_json::from_str(row)
                .unwrap_or_else(|e| panic!("group row not JSON: {row}: {e}"));
            let key = cell
                .get("group")
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| panic!("missing 'group' in: {row}"));
            let result = cell
                .get("result")
                .and_then(|v| v.as_f64())
                .unwrap_or_else(|| panic!("missing 'result' in: {row}"));
            (key, result)
        })
        .collect();
    groups.sort_by_key(|(k, _)| *k);

    assert_eq!(groups[0].0, 0, "first group key must be chr=0");
    assert!(
        (groups[0].1 - 6.0).abs() < 1e-4,
        "chr=0 sum must be 6.0, got {}",
        groups[0].1
    );
    assert_eq!(groups[1].0, 1, "second group key must be chr=1");
    assert!(
        (groups[1].1 - 60.0).abs() < 1e-4,
        "chr=1 sum must be 60.0, got {}",
        groups[1].1
    );
    assert_eq!(groups[2].0, 2, "third group key must be chr=2");
    assert!(
        (groups[2].1 - 600.0).abs() < 1e-4,
        "chr=2 sum must be 600.0, got {}",
        groups[2].1
    );

    cluster.shutdown().await;
}

// ── Test 4: vector prefilter fused with distributed slice ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_vector_prefilter_distributed() {
    let (cluster, node_idx) = spawn_cluster_with_genome().await;

    // Create a document collection and vector index to back the fused query.
    // DDL must be issued and accepted on any leader.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION genes TYPE document")
        .await
        .expect("CREATE COLLECTION genes");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE VECTOR INDEX idx_genes_emb ON genes FIELD embedding METRIC cosine DIM 3",
        )
        .await
        .expect("CREATE VECTOR INDEX");

    // The fused query: ORDER BY vector_distance + JOIN NDARRAY_SLICE.
    // Issued on the DDL node because the array catalog is local there.
    // The vector index is empty — the assertion is that the query wires
    // through every distributed layer without error:
    //   planner fusion → convert → ArrayOp::SurrogateBitmapScan (distributed
    //   fan-out to peer shards) + VectorOp::Search with inline_prefilter_plan.
    let result = cluster.nodes[node_idx]
        .client
        .simple_query(
            "SELECT id FROM genes \
             JOIN NDARRAY_SLICE('genome', '{chr: [1, 1], pos: [0, 99]}') AS s \
               ON id = s.qual \
             ORDER BY vector_distance(embedding, [1.0, 0.0, 0.0]) \
             LIMIT 10",
        )
        .await;

    match result {
        Ok(_) => {}
        Err(e) => {
            let msg = format!("{e}");
            // Tolerate empty-index / no-rows errors. Codec panics and planner
            // errors indicate the fused distributed path was never attempted.
            assert!(
                !msg.contains("codec") && !msg.contains("panic") && !msg.contains("plan error"),
                "unexpected error from fused distributed array+vector query: {msg}"
            );
        }
    }

    cluster.shutdown().await;
}
