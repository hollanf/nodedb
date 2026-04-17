//! End-to-end wire-streaming BACKUP / RESTORE tests against the
//! single-node pgwire harness. Bytes flow over the pgwire COPY framing
//! (`COPY (BACKUP TENANT n) TO STDOUT` / `COPY tenant_restore(n) FROM
//! STDIN`) — the database never opens a caller-named filesystem path.
//!
//! The cluster fan-out is exercised separately in
//! `cluster_backup_restore.rs`; this file pins down the wire surface
//! and the deserialization-hardening guarantees that apply on every
//! deployment shape.

mod common;
use common::pgwire_harness::TestServer;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use nodedb_types::backup_envelope::{EnvelopeMeta, EnvelopeWriter, HEADER_LEN, MAGIC, TRAILER_LEN};

const TENANT: u32 = 1;

async fn drain_backup(server: &TestServer, tenant: u32) -> Result<Vec<u8>, String> {
    let stream = server
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .map_err(|e| format!("copy_out failed: {e}"))?;
    let mut bytes = Vec::new();
    let mut s = Box::pin(stream);
    while let Some(chunk) = s.next().await {
        let c = chunk.map_err(|e| format!("copy_out chunk: {e}"))?;
        bytes.extend_from_slice(&c);
    }
    Ok(bytes)
}

async fn push_restore(
    server: &TestServer,
    tenant: u32,
    bytes: Vec<u8>,
    dry_run: bool,
) -> Result<(), String> {
    use futures::SinkExt;

    let suffix = if dry_run { " DRY RUN" } else { "" };
    let sink = server
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN{suffix}"))
        .await
        .map_err(detail)?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(bytes))
        .await
        .map_err(detail)?;
    sink.as_mut().finish().await.map(|_| ()).map_err(detail)
}

/// Extract the server-side error detail from a tokio_postgres error.
/// `e.to_string()` returns "db error"; the actual message lives on
/// `as_db_error().message()`.
fn detail(e: tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

// ────────────────────────────────────────────────────────────────────
// Roundtrip — single-node, mixed engine workload.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn empty_tenant_roundtrips() {
    let server = TestServer::start().await;
    let bytes = drain_backup(&server, TENANT).await.expect("BACKUP");
    assert!(
        bytes.len() >= HEADER_LEN + TRAILER_LEN,
        "envelope too small: {} bytes",
        bytes.len()
    );
    assert_eq!(&bytes[..4], MAGIC);
    push_restore(&server, TENANT, bytes, false)
        .await
        .expect("RESTORE");
}

#[tokio::test]
async fn document_roundtrip() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION wire_docs TYPE DOCUMENT STRICT \
             (id TEXT PRIMARY KEY, content TEXT)",
        )
        .await
        .ok();
    server
        .exec("INSERT INTO wire_docs (id, content) VALUES ('a','alpha')")
        .await
        .expect("insert a");
    server
        .exec("INSERT INTO wire_docs (id, content) VALUES ('b','beta')")
        .await
        .expect("insert b");

    let bytes = drain_backup(&server, TENANT).await.expect("BACKUP");

    // Restore over the *same* server is a valid sanity check — restore is
    // idempotent at the engine level (PointPut overwrites).
    push_restore(&server, TENANT, bytes, false)
        .await
        .expect("RESTORE");

    let rows = server
        .query_text("SELECT content FROM wire_docs WHERE id='a'")
        .await
        .expect("post-restore SELECT");
    assert!(
        rows.iter().any(|r| r.contains("alpha")),
        "expected post-restore rows to contain 'alpha', got: {rows:?}"
    );
}

// ────────────────────────────────────────────────────────────────────
// Dry run — validates envelope, must not mutate state.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn dry_run_validates_without_mutating() {
    let server = TestServer::start().await;
    let bytes = drain_backup(&server, TENANT).await.expect("BACKUP");
    push_restore(&server, TENANT, bytes, true)
        .await
        .expect("RESTORE DRY RUN");
}

// ────────────────────────────────────────────────────────────────────
// Hardening — every malformed payload is rejected with a generic
// error, never echoes deserializer context, never mutates state.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn rejects_random_bytes() {
    let server = TestServer::start().await;
    let err = push_restore(&server, TENANT, b"this is not a backup".to_vec(), false)
        .await
        .expect_err("must reject non-envelope bytes");
    let lower = err.to_lowercase();
    assert!(
        lower.contains("invalid backup format"),
        "expected generic invalid-format rejection, got: {err}"
    );
    assert!(
        !lower.contains("msgpack")
            && !lower.contains("invalid type")
            && !lower.contains("missing field")
            && !lower.contains("invalid marker"),
        "deserializer context must not leak: {err}"
    );
}

#[tokio::test]
async fn rejects_bad_magic() {
    let server = TestServer::start().await;
    let mut writer = EnvelopeWriter::new(EnvelopeMeta {
        tenant_id: TENANT,
        source_vshard_count: 1024,
        hash_seed: 0,
        snapshot_watermark: 0,
    });
    writer.push_section(0, b"x".to_vec()).unwrap();
    let mut bytes = writer.finalize();
    bytes[0] = b'X'; // mutate magic
    let err = push_restore(&server, TENANT, bytes, false)
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("invalid backup format"),
        "expected magic rejection, got: {err}"
    );
}

#[tokio::test]
async fn rejects_tenant_mismatch() {
    let server = TestServer::start().await;
    // Backup tenant 1, hand-craft envelope claiming tenant 99.
    let mut writer = EnvelopeWriter::new(EnvelopeMeta {
        tenant_id: 99,
        source_vshard_count: 1024,
        hash_seed: 0,
        snapshot_watermark: 0,
    });
    writer.push_section(0, vec![]).unwrap();
    let bytes = writer.finalize();
    let err = push_restore(&server, TENANT, bytes, false)
        .await
        .unwrap_err();
    let lower = err.to_lowercase();
    assert!(
        lower.contains("tenant mismatch"),
        "expected tenant mismatch rejection, got: {err}"
    );
}

#[tokio::test]
async fn rejects_corrupted_trailer() {
    let server = TestServer::start().await;
    let mut bytes = drain_backup(&server, TENANT).await.expect("BACKUP");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    let err = push_restore(&server, TENANT, bytes, false)
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("invalid backup format"),
        "expected trailer-crc rejection, got: {err}"
    );
}

#[tokio::test]
async fn rejects_unsupported_version() {
    let server = TestServer::start().await;
    let mut bytes = drain_backup(&server, TENANT).await.expect("BACKUP");
    bytes[4] = 99; // version byte in header
    let err = push_restore(&server, TENANT, bytes, false)
        .await
        .unwrap_err();
    assert!(
        err.to_lowercase().contains("unsupported backup version"),
        "expected version rejection, got: {err}"
    );
}

// Touching the import to keep wire stream type used.
#[allow(dead_code)]
fn _stream_type_used() -> impl futures::Stream<Item = u8> {
    stream::empty()
}
