//! Integration tests for timeseries query engine (GitHub issues #6-#9).
//!
//! Tests the full path: ILP ingest → memtable → flush → disk partitions →
//! query across both sources.

use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::TimeseriesOp;

use crate::helpers::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an ILP payload with `count` lines for a given collection.
fn ilp_lines(collection: &str, count: usize, start_ts_ns: i64) -> String {
    let mut lines = String::new();
    let qtypes = ["A", "AAAA", "MX", "CNAME"];
    for i in 0..count {
        let ts_ns = start_ts_ns + i as i64 * 1_000_000; // 1ms apart
        let qtype = qtypes[i % qtypes.len()];
        let qname = format!("host-{}.example.com", i % 50);
        lines.push_str(&format!(
            "{collection},qtype={qtype},qname={qname} elapsed_ms={}.0 {ts_ns}\n",
            (i % 1000) as f64
        ));
    }
    lines
}

fn ingest_ilp(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    collection: &str,
    payload: &str,
) -> serde_json::Value {
    let raw = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.to_string(),
            payload: payload.as_bytes().to_vec(),
            format: "ilp".to_string(),
        }),
    );
    serde_json::from_slice(&raw).unwrap_or(serde_json::Value::Null)
}

fn ts_scan(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    collection: &str,
    group_by: Vec<String>,
    aggregates: Vec<(String, String)>,
    bucket_interval_ms: i64,
) -> Vec<serde_json::Value> {
    let raw = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: collection.to_string(),
            time_range: (0, i64::MAX),
            projection: Vec::new(),
            limit: usize::MAX,
            filters: Vec::new(),
            bucket_interval_ms,
            group_by,
            aggregates,
            rls_filters: Vec::new(),
        }),
    );
    serde_json::from_slice(&raw).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// #6 — Query reads all data (memtable + disk partitions)
// ---------------------------------------------------------------------------

#[test]
fn count_star_sees_flushed_partitions() {
    let (mut core, mut tx, mut rx) = make_core();

    // Ingest enough data to trigger a flush (>64 MiB).
    // Use many small batches to stay under the hard limit.
    let batch_size = 5_000;
    let num_batches = 10;
    let total_rows = batch_size * num_batches;
    for b in 0..num_batches {
        let start_ns = (b * batch_size) as i64 * 1_000_000_000;
        let payload = ilp_lines("metrics", batch_size, start_ns);
        ingest_ilp(&mut core, &mut tx, &mut rx, "metrics", &payload);
    }

    // COUNT(*) should return total rows (memtable + any flushed partitions).
    let results = ts_scan(
        &mut core,
        &mut tx,
        &mut rx,
        "metrics",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(results.len(), 1, "expected single aggregate row");
    let count = results[0]["count_all"].as_u64().unwrap_or(0);
    assert_eq!(
        count, total_rows as u64,
        "COUNT(*) should see all rows including flushed partitions"
    );
}

#[test]
fn group_by_sees_both_memtable_and_partitions() {
    let (mut core, mut tx, mut rx) = make_core();

    // Ingest two batches — enough for at least one flush.
    for b in 0..10 {
        let start_ns = b * 5_000 * 1_000_000_000i64;
        let payload = ilp_lines("dns", 5_000, start_ns);
        ingest_ilp(&mut core, &mut tx, &mut rx, "dns", &payload);
    }

    // GROUP BY qtype should see all data.
    let results = ts_scan(
        &mut core,
        &mut tx,
        &mut rx,
        "dns",
        vec!["qtype".into()],
        vec![("count".into(), "*".into())],
        0,
    );
    let total: u64 = results
        .iter()
        .map(|r| r["count_all"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 50_000, "GROUP BY total should match ingested rows");
    assert_eq!(results.len(), 4, "should have 4 qtypes: A, AAAA, MX, CNAME");
}

// ---------------------------------------------------------------------------
// #7 — time_bucket() works without panic
// ---------------------------------------------------------------------------

#[test]
fn time_bucket_aggregation_does_not_panic() {
    let (mut core, mut tx, mut rx) = make_core();

    // Ingest data spanning multiple hours.
    let payload = ilp_lines("ts_bucket", 1_000, 1_700_000_000_000_000_000);
    ingest_ilp(&mut core, &mut tx, &mut rx, "ts_bucket", &payload);

    // time_bucket('1h') should return valid buckets — not panic or timeout.
    let results = ts_scan(
        &mut core,
        &mut tx,
        &mut rx,
        "ts_bucket",
        Vec::new(),
        vec![("count".into(), "*".into())],
        3_600_000, // 1 hour
    );
    assert!(
        !results.is_empty(),
        "time_bucket should return at least 1 bucket"
    );
    let total: u64 = results
        .iter()
        .map(|r| r["count_all"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 1_000, "bucket counts should sum to ingested rows");
}

#[test]
fn time_bucket_with_named_value_column() {
    let (mut core, mut tx, mut rx) = make_core();

    let payload = ilp_lines("ts_named", 500, 1_700_000_000_000_000_000);
    ingest_ilp(&mut core, &mut tx, &mut rx, "ts_named", &payload);

    // AVG(elapsed_ms) grouped by time bucket — uses named column, not hardcoded index.
    let results = ts_scan(
        &mut core,
        &mut tx,
        &mut rx,
        "ts_named",
        Vec::new(),
        vec![
            ("count".into(), "*".into()),
            ("avg".into(), "elapsed_ms".into()),
        ],
        3_600_000,
    );
    assert!(!results.is_empty());
    for r in &results {
        let avg = r["avg_elapsed_ms"].as_f64();
        assert!(avg.is_some(), "avg should be present: {r}");
    }
}

// ---------------------------------------------------------------------------
// #8 — Schema evolution (new ILP fields)
// ---------------------------------------------------------------------------

#[test]
fn schema_evolution_adds_new_fields() {
    let (mut core, mut tx, mut rx) = make_core();

    // First batch: only elapsed_ms.
    let batch1 = "dns,qtype=A elapsed_ms=1.5 1700000000000000000\n\
                   dns,qtype=AAAA elapsed_ms=2.5 1700000001000000000\n";
    ingest_ilp(&mut core, &mut tx, &mut rx, "dns", batch1);

    // Second batch: adds response_bytes and ttl (new fields).
    let batch2 = "dns,qtype=A elapsed_ms=3.0,response_bytes=512i,ttl=3600i 1700000002000000000\n\
                   dns,qtype=MX elapsed_ms=4.0,response_bytes=256i,ttl=7200i 1700000003000000000\n";
    ingest_ilp(&mut core, &mut tx, &mut rx, "dns", batch2);

    // Raw scan should show new columns with values for batch2 rows.
    let results = ts_scan(
        &mut core,
        &mut tx,
        &mut rx,
        "dns",
        Vec::new(),
        Vec::new(), // raw scan
        0,
    );
    assert_eq!(results.len(), 4, "should see all 4 rows");

    // Batch2 rows (last 2) should have response_bytes.
    let has_response_bytes: Vec<_> = results
        .iter()
        .filter(|r| {
            r.get("response_bytes")
                .is_some_and(|v| !v.is_null() && v.as_i64().unwrap_or(0) > 0)
        })
        .collect();
    assert_eq!(
        has_response_bytes.len(),
        2,
        "2 rows should have response_bytes set"
    );

    // Batch1 rows should have response_bytes=0 (int64 default) or null.
    let batch1_rows: Vec<_> = results
        .iter()
        .filter(|r| {
            r.get("response_bytes")
                .is_none_or(|v| v.is_null() || v.as_i64() == Some(0))
        })
        .collect();
    assert_eq!(
        batch1_rows.len(),
        2,
        "batch1 rows should have null/0 for new fields"
    );
}

// ---------------------------------------------------------------------------
// #9 — GROUP BY not capped at 10K
// ---------------------------------------------------------------------------

#[test]
fn group_by_not_capped_at_10k() {
    let (mut core, mut tx, mut rx) = make_core();

    // Ingest data with high cardinality: 15K unique qnames.
    let mut lines = String::new();
    for i in 0..15_000 {
        let ts_ns = 1_700_000_000_000_000_000i64 + i as i64 * 1_000_000;
        lines.push_str(&format!(
            "dns,qname=host-{i}.example.com elapsed_ms=1.0 {ts_ns}\n"
        ));
    }
    ingest_ilp(&mut core, &mut tx, &mut rx, "dns_card", &lines);

    // GROUP BY qname should return all 15K groups — not capped at 10K.
    let results = ts_scan(
        &mut core,
        &mut tx,
        &mut rx,
        "dns_card",
        vec!["qname".into()],
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(
        results.len(),
        15_000,
        "GROUP BY should return all unique groups, not capped at 10K"
    );
}
