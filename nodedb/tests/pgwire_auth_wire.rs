//! End-to-end TCP roundtrip: real pgwire connection executes DDL and
//! observes both state mutation and SHOW SESSION results.

mod common;

use std::sync::Arc;

use common::pgwire_auth_helpers::make_state;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn pgwire_ddl_roundtrip() {
    let state = make_state();

    let pg_listener =
        nodedb::control::server::pgwire::listener::PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let port = pg_listener.local_addr().port();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&state.shutdown));
    let shared_pg = Arc::clone(&state);
    let test_startup_gate = Arc::clone(&state.startup);
    let bus_pg = shutdown_bus.clone();
    tokio::spawn(async move {
        pg_listener
            .run(
                shared_pg,
                nodedb::config::auth::AuthMode::Trust,
                None,
                Arc::new(tokio::sync::Semaphore::new(128)),
                test_startup_gate,
                bus_pg,
            )
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let conn_str = format!("host=127.0.0.1 port={port} user=nodedb dbname=nodedb");
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .simple_query("CREATE USER wire_test WITH PASSWORD 'pass'")
        .await
        .unwrap();

    let msgs = client.simple_query("SHOW SESSION").await.unwrap();
    let username = msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
        _ => None,
    });
    assert_eq!(username, Some("nodedb".to_string()));

    assert!(state.credentials.get_user("wire_test").is_some());
}
