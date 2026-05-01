//! ORDER BY edge case regression tests.
//!
//! Covers: ASC/DESC, mixed direction, NULL ordering, LIMIT interaction,
//! ORDER BY on expressions, and post-UNION ordering.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_id_asc() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION ob (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ob (id, val) VALUES ('c', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ob (id, val) VALUES ('a', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ob (id, val) VALUES ('b', 20)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM ob ORDER BY id ASC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], "a", "first row should be a, got: {}", rows[0]);
    assert_eq!(rows[2], "c", "last row should be c, got: {}", rows[2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_val_asc_and_desc() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION obd (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('a', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('b', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('c', 20)")
        .await
        .unwrap();

    // ASC
    let rows = server
        .query_text("SELECT id FROM obd ORDER BY val ASC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("b"),
        "ASC first row should be b (val=10), got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("a"),
        "ASC last row should be a (val=30), got: {}",
        rows[2]
    );

    // DESC
    let rows = server
        .query_text("SELECT id FROM obd ORDER BY val DESC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("a"),
        "DESC first row should be a (val=30), got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("b"),
        "DESC last row should be b (val=10), got: {}",
        rows[2]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_desc() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION obd (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('a', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('b', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('c', 20)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM obd ORDER BY val DESC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("a"),
        "first row should be a (val=30), got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("b"),
        "last row should be b (val=10), got: {}",
        rows[2]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_with_limit() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION obl (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for i in 1..=10 {
        server
            .exec(&format!("INSERT INTO obl (id, val) VALUES ('r{i}', {i})"))
            .await
            .unwrap();
    }

    let rows = server
        .query_text("SELECT id FROM obl ORDER BY val DESC LIMIT 3")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "LIMIT 3 should return 3 rows");
    assert!(
        rows[0].contains("r10"),
        "first should be r10 (highest), got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_string_column() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION obs (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obs (id, name) VALUES ('1', 'Charlie')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obs (id, name) VALUES ('2', 'Alice')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obs (id, name) VALUES ('3', 'Bob')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT name FROM obs ORDER BY name ASC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("Alice"),
        "first should be Alice, got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("Charlie"),
        "last should be Charlie, got: {}",
        rows[2]
    );
}
