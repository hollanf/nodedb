//! SQL SELECT on KV collections must return rows whose JSON payload
//! carries every projected column, the same way every other engine
//! does. The RESP protocol is what gives KV its bare-value / cursor-
//! pair shape; SQL must not inherit that — protocol dictates response
//! shape, not engine.
//!
//! These tests exercise the simple-query path: a row's envelope text
//! (the `result` / `document` column) must parse to a JSON object whose
//! keys are the projected columns. Extended-query coverage for the same
//! invariant lives in `pgwire_extended_query.rs`.

mod common;

use common::pgwire_harness::TestServer;

/// Parse the envelope text from a simple-query row and return the
/// decoded JSON object. Panics if parsing fails or the payload isn't
/// a JSON object.
fn parse_row_envelope(text: &str) -> serde_json::Map<String, serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(text).expect("row envelope must be JSON");
    match value {
        serde_json::Value::Object(map) => map,
        other => panic!("row envelope must be a JSON object, got {other:?}"),
    }
}

/// `SELECT key, value FROM kv WHERE key = 'x'` must return one row
/// whose payload carries both `key` and `value` fields — not a bare
/// scalar.
#[tokio::test]
async fn kv_sql_point_select_returns_key_and_value() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('hello', 'world')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT key, value FROM kv WHERE key = 'hello'")
        .await
        .expect("point SELECT should succeed");

    assert_eq!(rows.len(), 1, "expected exactly one row");

    // Regression guard: the bug returned `"119"` — the msgpack fixint
    // of the first byte of the stored value — as the envelope text.
    // A real row envelope is a JSON object, not a bare integer.
    let obj = parse_row_envelope(&rows[0]);
    assert!(
        obj.contains_key("key") && obj.contains_key("value"),
        "row envelope must carry [key, value], got keys {:?}",
        obj.keys().collect::<Vec<_>>()
    );
    assert_eq!(obj.get("key").and_then(|v| v.as_str()), Some("hello"));
    assert_eq!(obj.get("value").and_then(|v| v.as_str()), Some("world"));
}

/// Full-table SELECT must return one row per stored entry, each
/// envelope carrying the projected columns.
#[tokio::test]
async fn kv_sql_full_scan_returns_all_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('a', 'one')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('b', 'two')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT key, value FROM kv")
        .await
        .expect("full scan SELECT should succeed");

    assert_eq!(rows.len(), 2, "expected 2 rows, got {}", rows.len());

    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            let obj = parse_row_envelope(r);
            (
                obj.get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                obj.get("value")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("a".to_string(), "one".to_string()),
            ("b".to_string(), "two".to_string()),
        ]
    );
}

/// Star projection over KV must expose every stored column in the
/// envelope payload.
#[tokio::test]
async fn kv_sql_star_projection_returns_all_columns() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('hello', 'world')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT * FROM kv WHERE key = 'hello'")
        .await
        .expect("star SELECT should succeed");

    assert_eq!(rows.len(), 1);
    let obj = parse_row_envelope(&rows[0]);
    assert_eq!(obj.get("key").and_then(|v| v.as_str()), Some("hello"));
    assert_eq!(obj.get("value").and_then(|v| v.as_str()), Some("world"));
}

/// KV collections with multi-column typed values must expose every
/// declared column as its own envelope field. This test uses `key` as
/// the primary-key column name because the current KV INSERT planner
/// only recognises a PK column literally named `key` — an orthogonal
/// bug tracked separately.
#[tokio::test]
async fn kv_sql_typed_columns_point_select() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION users (key STRING PRIMARY KEY, name STRING, age INT) WITH (engine='kv')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO users (key, name, age) VALUES ('u1', 'alice', 30)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT key, name, age FROM users WHERE key = 'u1'")
        .await
        .expect("multi-column point SELECT should succeed");

    assert_eq!(rows.len(), 1);
    let obj = parse_row_envelope(&rows[0]);
    assert_eq!(obj.get("key").and_then(|v| v.as_str()), Some("u1"));
    assert_eq!(obj.get("name").and_then(|v| v.as_str()), Some("alice"));
    let age = obj.get("age").expect("age field must be present");
    let age_as_i64 = age
        .as_i64()
        .or_else(|| age.as_str().and_then(|s| s.parse::<i64>().ok()))
        .expect("age must decode as integer");
    assert_eq!(age_as_i64, 30);
}

/// Single-column projection must still return the row envelope with
/// the requested field present.
#[tokio::test]
async fn kv_sql_single_column_projection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('hello', 'world')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT value FROM kv WHERE key = 'hello'")
        .await
        .expect("single-column point SELECT should succeed");

    assert_eq!(rows.len(), 1);
    let obj = parse_row_envelope(&rows[0]);
    assert_eq!(obj.get("value").and_then(|v| v.as_str()), Some("world"));
}
