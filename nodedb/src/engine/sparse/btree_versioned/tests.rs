//! Unit tests for the versioned document + index tables.

use super::value::VersionedPut;
use crate::engine::sparse::btree::SparseEngine;

fn open_temp() -> (SparseEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = SparseEngine::open(&dir.path().join("v.redb")).unwrap();
    (engine, dir)
}

fn put(e: &SparseEngine, coll: &str, id: &str, sys_from: i64, body: &[u8]) {
    e.versioned_put(VersionedPut {
        tenant: 1,
        coll,
        doc_id: id,
        sys_from_ms: sys_from,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
        body,
    })
    .unwrap();
}

fn put_valid(
    e: &SparseEngine,
    coll: &str,
    id: &str,
    sys_from: i64,
    valid_from: i64,
    valid_until: i64,
    body: &[u8],
) {
    e.versioned_put(VersionedPut {
        tenant: 1,
        coll,
        doc_id: id,
        sys_from_ms: sys_from,
        valid_from_ms: valid_from,
        valid_until_ms: valid_until,
        body,
    })
    .unwrap();
}

#[test]
fn put_and_read_current() {
    let (e, _d) = open_temp();
    put(&e, "users", "u1", 100, b"v1");
    let got = e.versioned_get_current(1, "users", "u1").unwrap();
    assert_eq!(got.as_deref(), Some(b"v1" as &[u8]));
}

#[test]
fn ceiling_picks_newest_le_cutoff() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 100, b"a");
    put(&e, "c", "k", 200, b"b");
    put(&e, "c", "k", 300, b"c");
    assert_eq!(
        e.versioned_get_as_of(1, "c", "k", Some(150), None)
            .unwrap()
            .as_deref(),
        Some(b"a" as &[u8])
    );
    assert_eq!(
        e.versioned_get_as_of(1, "c", "k", Some(250), None)
            .unwrap()
            .as_deref(),
        Some(b"b" as &[u8])
    );
    assert_eq!(
        e.versioned_get_as_of(1, "c", "k", Some(400), None)
            .unwrap()
            .as_deref(),
        Some(b"c" as &[u8])
    );
}

#[test]
fn ceiling_before_first_version_is_none() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 200, b"x");
    assert!(
        e.versioned_get_as_of(1, "c", "k", Some(100), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn tombstone_hides_row_at_and_after_cutoff() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 100, b"x");
    e.versioned_tombstone(1, "c", "k", 200).unwrap();
    assert_eq!(
        e.versioned_get_as_of(1, "c", "k", Some(150), None)
            .unwrap()
            .as_deref(),
        Some(b"x" as &[u8])
    );
    assert!(
        e.versioned_get_as_of(1, "c", "k", Some(250), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn valid_time_predicate_skips_out_of_window_versions() {
    let (e, _d) = open_temp();
    put_valid(&e, "c", "k", 10, 0, 100, b"v1");
    put_valid(&e, "c", "k", 20, 200, 300, b"v2");
    // valid-time hole at 150: neither version applies.
    assert!(
        e.versioned_get_as_of(1, "c", "k", Some(10_000), Some(150))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        e.versioned_get_as_of(1, "c", "k", Some(10_000), Some(50))
            .unwrap()
            .as_deref(),
        Some(b"v1" as &[u8])
    );
    assert_eq!(
        e.versioned_get_as_of(1, "c", "k", Some(10_000), Some(250))
            .unwrap()
            .as_deref(),
        Some(b"v2" as &[u8])
    );
}

#[test]
fn gdpr_erase_preserves_history_structure_but_hides_body() {
    let (e, _d) = open_temp();
    put(&e, "c", "k", 100, b"pii");
    put(&e, "c", "k", 200, b"more-pii");
    let n = e.versioned_gdpr_erase(1, "c", "k").unwrap();
    assert_eq!(n, 2);
    assert!(
        e.versioned_get_as_of(1, "c", "k", Some(150), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn scan_returns_latest_per_doc_id() {
    let (e, _d) = open_temp();
    put(&e, "c", "a", 100, b"a1");
    put(&e, "c", "a", 200, b"a2");
    put(&e, "c", "b", 150, b"b1");
    let all = e.versioned_scan_as_of(1, "c", None, None, 100).unwrap();
    let map: std::collections::HashMap<_, _> = all.into_iter().collect();
    assert_eq!(map.get("a").map(|v| v.as_slice()), Some(b"a2" as &[u8]));
    assert_eq!(map.get("b").map(|v| v.as_slice()), Some(b"b1" as &[u8]));
}

#[test]
fn scan_as_of_hides_tombstoned_rows() {
    let (e, _d) = open_temp();
    put(&e, "c", "a", 100, b"a1");
    e.versioned_tombstone(1, "c", "a", 200).unwrap();
    let at_150 = e
        .versioned_scan_as_of(1, "c", Some(150), None, 100)
        .unwrap();
    assert_eq!(at_150.len(), 1);
    let at_250 = e
        .versioned_scan_as_of(1, "c", Some(250), None, 100)
        .unwrap();
    assert!(at_250.is_empty());
}

#[test]
fn index_lookup_honors_cutoff_and_tombstone() {
    let (e, _d) = open_temp();
    e.versioned_index_put(1, "c", "email", "a@x", "u1", 100)
        .unwrap();
    e.versioned_index_put(1, "c", "email", "a@x", "u2", 150)
        .unwrap();
    e.versioned_index_tombstone(1, "c", "email", "a@x", "u1", 200)
        .unwrap();

    let at_120 = e
        .versioned_index_lookup_as_of(1, "c", "email", "a@x", Some(120))
        .unwrap();
    assert_eq!(at_120, vec!["u1"]);

    let at_175 = e
        .versioned_index_lookup_as_of(1, "c", "email", "a@x", Some(175))
        .unwrap();
    assert_eq!(at_175.len(), 2);

    let at_250 = e
        .versioned_index_lookup_as_of(1, "c", "email", "a@x", Some(250))
        .unwrap();
    assert_eq!(at_250, vec!["u2"]);
}

#[test]
fn nul_in_doc_id_is_rejected() {
    let (e, _d) = open_temp();
    let r = e.versioned_put(VersionedPut {
        tenant: 1,
        coll: "c",
        doc_id: "a\x00b",
        sys_from_ms: 100,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
        body: b"x",
    });
    assert!(r.is_err());
}
