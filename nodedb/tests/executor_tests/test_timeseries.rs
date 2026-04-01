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
///
/// Generates wide rows (~10 columns, ~80 bytes each) to trigger memtable
/// flushes faster. Each 64MB memtable holds ~840K wide rows.
fn ilp_lines(collection: &str, count: usize, start_ts_ns: i64) -> String {
    let mut lines = String::new();
    let qtypes = ["A", "AAAA", "MX", "CNAME"];
    let rcodes = ["NOERROR", "NXDOMAIN", "SERVFAIL", "REFUSED"];
    for i in 0..count {
        let ts_ns = start_ts_ns + i as i64 * 1_000_000; // 1ms apart
        let qtype = qtypes[i % qtypes.len()];
        let rcode = rcodes[i % rcodes.len()];
        let qname = format!("host-{}.example.com", i % 50);
        let client_ip = format!("10.0.{}.{}", (i / 256) % 256, i % 256);
        lines.push_str(&format!(
            "{collection},qtype={qtype},rcode={rcode},qname={qname},client_ip={client_ip} elapsed_ms={}.0 {ts_ns}\n",
            (i % 1000) as f64
        ));
    }
    lines
}

fn ingest_ilp(
    ctx: &mut crate::helpers::TestCtx,
    collection: &str,
    payload: &str,
) -> serde_json::Value {
    let raw = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.to_string(),
            payload: payload.as_bytes().to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
        }),
    );
    serde_json::from_slice(&raw).unwrap_or(serde_json::Value::Null)
}

fn ts_scan(
    ctx: &mut crate::helpers::TestCtx,
    collection: &str,
    group_by: Vec<String>,
    aggregates: Vec<(String, String)>,
    bucket_interval_ms: i64,
) -> Vec<serde_json::Value> {
    let raw = send_ok_streaming(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
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
    // Streaming responses produce concatenated JSON arrays: [a,b][c,d]...
    // Parse each array and merge.
    parse_chunked_json(&raw)
}

/// Parse concatenated JSON arrays from chunked responses into a single Vec.
fn parse_chunked_json(data: &[u8]) -> Vec<serde_json::Value> {
    // Try single array first (most common, non-chunked case).
    if let Ok(arr) = serde_json::from_slice::<Vec<serde_json::Value>>(data) {
        return arr;
    }
    // Chunked: multiple JSON arrays concatenated. Use streaming deserializer.
    let mut results = Vec::new();
    let deser = serde_json::Deserializer::from_slice(data).into_iter::<Vec<serde_json::Value>>();
    for arr in deser.flatten() {
        results.extend(arr);
    }
    results
}

fn ts_scan_filtered(
    ctx: &mut crate::helpers::TestCtx,
    collection: &str,
    group_by: Vec<String>,
    aggregates: Vec<(String, String)>,
    bucket_interval_ms: i64,
    filters: Vec<nodedb::bridge::scan_filter::ScanFilter>,
) -> Vec<serde_json::Value> {
    let filter_bytes = if filters.is_empty() {
        Vec::new()
    } else {
        rmp_serde::to_vec_named(&filters).unwrap_or_default()
    };
    let raw = send_ok_streaming(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: collection.to_string(),
            time_range: (0, i64::MAX),
            projection: Vec::new(),
            limit: usize::MAX,
            filters: filter_bytes,
            bucket_interval_ms,
            group_by,
            aggregates,
            rls_filters: Vec::new(),
        }),
    );
    parse_chunked_json(&raw)
}

// ---------------------------------------------------------------------------
// #6 — Query reads all data (memtable + disk partitions)
// ---------------------------------------------------------------------------

#[test]
fn count_star_sees_flushed_partitions() {
    let mut ctx = make_ctx();

    // Ingest enough wide rows to force multiple memtable flushes.
    // Each row is ~7 columns × ~8 bytes = ~56 bytes of column data.
    // 64MB / 56 = ~1.2M rows per flush. We send 3M rows to guarantee
    // at least 2 flush cycles. Each batch is 10K rows.
    let batch_size = 10_000;
    let num_batches = 300;
    let mut total_accepted: u64 = 0;
    let mut total_rejected: u64 = 0;

    for b in 0..num_batches {
        let start_ns = (b * batch_size) as i64 * 1_000_000;
        let payload = ilp_lines("metrics", batch_size, start_ns);
        let resp = ingest_ilp(&mut ctx, "metrics", &payload);
        total_accepted += resp["accepted"].as_u64().unwrap_or(0);
        total_rejected += resp["rejected"].as_u64().unwrap_or(0);
    }

    let total_sent = (batch_size * num_batches) as u64;
    assert_eq!(
        total_rejected, 0,
        "no rows should be rejected — pre-flush must prevent hard-limit rejections"
    );
    assert_eq!(
        total_accepted, total_sent,
        "all sent rows should be accepted by the Data Plane"
    );

    // COUNT(*) must equal total accepted (memtable + ALL flushed partitions).
    // This catches: partition registry not populated, flush losing drain data,
    // or query path not reading from all sources.
    let results = ts_scan(
        &mut ctx,
        "metrics",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(results.len(), 1, "expected single aggregate row");
    let count = results[0]["count_all"].as_u64().unwrap_or(0);
    assert_eq!(
        count, total_accepted,
        "COUNT(*) must see every accepted row — found {count}, expected {total_accepted}. \
         If count < accepted, rows were lost during flush or partitions are invisible."
    );
}

#[test]
fn group_by_sees_both_memtable_and_partitions() {
    let mut ctx = make_ctx();

    // Ingest two batches — enough for at least one flush.
    for b in 0..10 {
        let start_ns = b * 5_000 * 1_000_000_000i64;
        let payload = ilp_lines("dns", 5_000, start_ns);
        ingest_ilp(&mut ctx, "dns", &payload);
    }

    // GROUP BY qtype should see all data.
    let results = ts_scan(
        &mut ctx,
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
    let mut ctx = make_ctx();

    // Ingest data spanning multiple hours.
    let payload = ilp_lines("ts_bucket", 1_000, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "ts_bucket", &payload);

    // time_bucket('1h') should return valid buckets — not panic or timeout.
    let results = ts_scan(
        &mut ctx,
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
    let mut ctx = make_ctx();

    let payload = ilp_lines("ts_named", 500, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "ts_named", &payload);

    // AVG(elapsed_ms) grouped by time bucket — uses named column, not hardcoded index.
    let results = ts_scan(
        &mut ctx,
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
// #6 (cont.) — WHERE predicates filter aggregate results
// ---------------------------------------------------------------------------

#[test]
fn where_predicate_filters_count() {
    let mut ctx = make_ctx();

    let payload = ilp_lines("dns_filt", 1_000, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "dns_filt", &payload);

    // Unfiltered COUNT(*) = 1000.
    let all = ts_scan(
        &mut ctx,
        "dns_filt",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    let total = all[0]["count_all"].as_u64().unwrap();
    assert_eq!(total, 1_000);

    // Filtered: qtype = 'A' → 25% of rows (4 qtypes, round-robin).
    let filtered = ts_scan_filtered(
        &mut ctx,
        "dns_filt",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
        vec![nodedb::bridge::scan_filter::ScanFilter {
            field: "qtype".into(),
            op: "eq".into(),
            value: serde_json::json!("A"),
            clauses: vec![],
        }],
    );
    let filtered_count = filtered[0]["count_all"].as_u64().unwrap();
    assert_eq!(filtered_count, 250, "WHERE qtype='A' should return 25%");
}

// ---------------------------------------------------------------------------
// #8 — Schema evolution (new ILP fields)
// ---------------------------------------------------------------------------

#[test]
fn schema_evolution_adds_new_fields() {
    let mut ctx = make_ctx();

    // First batch: only elapsed_ms.
    let batch1 = "dns,qtype=A elapsed_ms=1.5 1700000000000000000\n\
                   dns,qtype=AAAA elapsed_ms=2.5 1700000001000000000\n";
    ingest_ilp(&mut ctx, "dns", batch1);

    // Second batch: adds response_bytes and ttl (new fields).
    let batch2 = "dns,qtype=A elapsed_ms=3.0,response_bytes=512i,ttl=3600i 1700000002000000000\n\
                   dns,qtype=MX elapsed_ms=4.0,response_bytes=256i,ttl=7200i 1700000003000000000\n";
    ingest_ilp(&mut ctx, "dns", batch2);

    // Raw scan should show new columns with values for batch2 rows.
    let results = ts_scan(
        &mut ctx,
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
    let mut ctx = make_ctx();

    // Ingest data with high cardinality: 15K unique qnames.
    let mut lines = String::new();
    for i in 0..15_000 {
        let ts_ns = 1_700_000_000_000_000_000i64 + i as i64 * 1_000_000;
        lines.push_str(&format!(
            "dns,qname=host-{i}.example.com elapsed_ms=1.0 {ts_ns}\n"
        ));
    }
    ingest_ilp(&mut ctx, "dns_card", &lines);

    // GROUP BY qname should return all 15K groups — not capped at 10K.
    let results = ts_scan(
        &mut ctx,
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
