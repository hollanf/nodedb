//! End-to-end Calvin pgwire test: verifies that a multi-shard transaction
//! submitted via `simple_query` under `cross_shard_txn = 'strict'` is admitted
//! by the sequencer and advances `admitted_total`.
//!
//! The Calvin multi-shard path is exercised by BEGIN + two point INSERTs into
//! collections on different vShards + COMMIT.  On COMMIT, `handle_commit`
//! calls `classify_dispatch` on the buffered task set, detects MultiShard, and
//! submits the batch to the Calvin sequencer inbox.  After the epoch ticker
//! admits the batch, `admitted_total` on `SequencerMetrics` increments.
//!
//! If `admitted_total` stays 0 after COMMIT, STOP and report — do not paper over.
//!
//! Foldability of individual tasks is also spot-checked: for each buffered task
//! the Calvin response path in `transaction_cmds.rs` does NOT synthesise
//! per-task tags (COMMIT returns a single COMMIT tag), so that path is
//! verified separately by asserting that `is_calvin_foldable` on PointInsert
//! plans returns `true` at the unit-test level in `plan.rs`.
//!
//! File name contains "cluster" so nextest applies the cluster test group:
//! `binary(/cluster/)` → max-threads=1, threads-required=num-test-threads.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::VShardId;

/// Find two collection names whose vShard ids differ.
fn two_distinct_vshard_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("calvin_e2e_{i}");
        let vshard = VShardId::from_collection(&name).as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

/// Calvin multi-shard batch via pgwire `simple_query` admits to the sequencer.
///
/// Steps:
/// 1. Spin up a single-node cluster (Raft + Calvin sequencer wired by `start_raft`).
/// 2. Wait until `sequencer_metrics` is set (sequencer ready).
/// 3. Create two collections on different vShards.
/// 4. Enable strict cross-shard mode.
/// 5. Via one `simple_query` call, send BEGIN + two point INSERTs + COMMIT.
///    The server splits at semicolons, buffers the INSERTs during the transaction,
///    and on COMMIT detects MultiShard → submits to Calvin sequencer inbox.
/// 6. Wait for the epoch ticker to process the inbox.
/// 7. Assert `admitted_total > baseline` — STOP if it stayed 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_multishard_transaction_via_simple_query_advances_admitted_total() {
    let node = common::cluster_harness::TestClusterNode::spawn(1, vec![])
        .await
        .expect("single-node cluster spawn");

    // Allow Raft to elect and the sequencer to start ticking.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Gate on sequencer_metrics being set (wired by start_raft via SequencerService).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if node.shared.sequencer_metrics.get().is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("sequencer_metrics not set within 10s — start_raft may not have wired it");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let (col_a, col_b) = two_distinct_vshard_collections();

    // Create both collections on this (single) node.
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {col_a} (id STRING PRIMARY KEY, v STRING)"
        ))
        .await
        .expect("CREATE COLLECTION col_a");

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {col_b} (id STRING PRIMARY KEY, v STRING)"
        ))
        .await
        .expect("CREATE COLLECTION col_b");

    // Enable strict Calvin mode so COMMIT's multi-shard path runs.
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // Baseline before the Calvin batch.
    let metrics = node
        .shared
        .sequencer_metrics
        .get()
        .expect("sequencer_metrics must be set");
    let admitted_before = metrics.admitted_total.load(Ordering::Relaxed);

    // Send the full transaction in one `simple_query` call.
    // tokio-postgres sends this as a single wire message; the server's
    // `execute_sql` splits at top-level semicolons and dispatches each
    // statement in order.  The two INSERTs are buffered during the
    // BEGIN block; on COMMIT the buffer spans two vShards → MultiShard
    // → Calvin sequencer inbox submission.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {col_a} (id, v) VALUES ('k1', 'hello'); \
         INSERT INTO {col_b} (id, v) VALUES ('k2', 'world'); \
         COMMIT"
    );
    node.client
        .simple_query(&txn_sql)
        .await
        .expect("multi-shard Calvin transaction must succeed");

    // Wait for the sequencer epoch ticker to process the inbox.
    // Epoch window is 10–50 ms; allow up to 5 s for CI headroom.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let admitted_after = metrics.admitted_total.load(Ordering::Relaxed);
        if admitted_after > admitted_before {
            break;
        }
        if std::time::Instant::now() >= deadline {
            // ── STOP-AND-REPORT ──────────────────────────────────────────────
            // `admitted_total` did not advance after the COMMIT completed.
            // The Calvin sequencer inbox was not processed — either the batch
            // never reached the inbox, or the epoch ticker is not running.
            // Do NOT paper over: this is a real infrastructure gap.
            panic!(
                "admitted_total stayed at {admitted_before} after multi-shard COMMIT; \
                 the Calvin sequencer did not admit the batch. \
                 Verify: (1) sequencer_inbox is set on SharedState via start_raft, \
                 (2) SequencerService epoch ticker is running (see start_raft.rs), \
                 (3) handle_commit MultiShard path submits to sequencer_inbox."
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let admitted_after = metrics.admitted_total.load(Ordering::Relaxed);
    assert!(
        admitted_after > admitted_before,
        "admitted_total must have advanced: before={admitted_before} after={admitted_after}"
    );

    // Verify the writes landed: SELECT from both collections.
    let rows_a = node
        .client
        .simple_query(&format!("SELECT * FROM {col_a}"))
        .await
        .expect("SELECT col_a");
    let row_count_a = rows_a
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        row_count_a, 1,
        "col_a must have 1 row after Calvin commit; got {row_count_a}"
    );

    let rows_b = node
        .client
        .simple_query(&format!("SELECT * FROM {col_b}"))
        .await
        .expect("SELECT col_b");
    let row_count_b = rows_b
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        row_count_b, 1,
        "col_b must have 1 row after Calvin commit; got {row_count_b}"
    );

    node.shutdown().await;
}
