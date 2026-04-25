//! Integration tests for `assign_surrogate` semantics over a real
//! `SystemCatalog`, including UPSERT idempotency, allocation
//! divergence, drop-collection cleanup, and the WAL-flush trigger.

use std::sync::{Arc, RwLock};

use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::surrogate::{
    FLUSH_OPS_THRESHOLD, NoopWalAppender, SurrogateAssigner, SurrogateRegistry,
    SurrogateRegistryHandle, SurrogateWalAppender,
};
use nodedb_types::Surrogate;

fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
    let dir = tempfile::tempdir().unwrap();
    let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
    (dir, cat)
}

fn fresh_registry() -> SurrogateRegistryHandle {
    Arc::new(RwLock::new(SurrogateRegistry::new()))
}

#[test]
fn assign_is_idempotent_for_same_pk() {
    let (_dir, cat) = open_catalog();
    let reg = fresh_registry();
    let wal = NoopWalAppender;
    let a = SurrogateAssigner::new(&reg, &cat, &wal);
    let s1 = a.assign("users", b"alice").unwrap();
    let s2 = a.assign("users", b"alice").unwrap();
    let s3 = a.assign("users", b"alice").unwrap();
    assert_eq!(s1, s2);
    assert_eq!(s2, s3);
    // Catalog binding survives — verifiable directly.
    assert_eq!(
        cat.get_surrogate_for_pk("users", b"alice").unwrap(),
        Some(s1)
    );
    assert_eq!(
        cat.get_pk_for_surrogate("users", s1).unwrap(),
        Some(b"alice".to_vec())
    );
}

#[test]
fn assign_distinct_pks_returns_distinct_surrogates() {
    let (_dir, cat) = open_catalog();
    let reg = fresh_registry();
    let wal = NoopWalAppender;
    let a = SurrogateAssigner::new(&reg, &cat, &wal);
    let s1 = a.assign("users", b"alice").unwrap();
    let s2 = a.assign("users", b"bob").unwrap();
    let s3 = a.assign("users", b"carol").unwrap();
    assert_ne!(s1, s2);
    assert_ne!(s2, s3);
    assert_ne!(s1, s3);
    // Surrogates are monotonic for sequential assigns from a fresh registry.
    assert!(s1.as_u32() < s2.as_u32());
    assert!(s2.as_u32() < s3.as_u32());
}

#[test]
fn drop_collection_wipes_surrogate_map() {
    let (_dir, cat) = open_catalog();
    let reg = fresh_registry();
    let wal = NoopWalAppender;
    let a = SurrogateAssigner::new(&reg, &cat, &wal);
    let _ = a.assign("users", b"alice").unwrap();
    let _ = a.assign("users", b"bob").unwrap();
    let s_other = a.assign("orders", b"o1").unwrap();
    assert_eq!(
        cat.scan_surrogates_for_collection("users").unwrap().len(),
        2
    );

    cat.delete_all_surrogates_for_collection("users").unwrap();
    assert!(
        cat.scan_surrogates_for_collection("users")
            .unwrap()
            .is_empty()
    );
    // Other collection's surrogates intact.
    assert_eq!(
        cat.get_surrogate_for_pk("orders", b"o1").unwrap(),
        Some(s_other)
    );
}

/// Counting WAL appender — verifies `assign_surrogate` actually
/// triggers a `SurrogateAlloc` WAL emission when the registry's
/// 1024-ops threshold is crossed.
struct CountingAppender {
    allocs: std::sync::atomic::AtomicU32,
    binds: std::sync::atomic::AtomicU32,
}

impl CountingAppender {
    fn new() -> Self {
        Self {
            allocs: std::sync::atomic::AtomicU32::new(0),
            binds: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

impl SurrogateWalAppender for CountingAppender {
    fn record_alloc_to_wal(&self, _hi: u32) -> nodedb::Result<()> {
        self.allocs
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    fn record_bind_to_wal(
        &self,
        _surrogate: u32,
        _collection: &str,
        _pk_bytes: &[u8],
    ) -> nodedb::Result<()> {
        self.binds.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }
}

#[test]
fn flush_emits_wal_record_at_threshold() {
    let (_dir, cat) = open_catalog();
    let reg = fresh_registry();
    let wal = CountingAppender::new();
    let a = SurrogateAssigner::new(&reg, &cat, &wal);

    let n = FLUSH_OPS_THRESHOLD as usize;
    for i in 0..n {
        let pk = format!("u{i:08}");
        let _ = a.assign("users", pk.as_bytes()).unwrap();
    }

    // The 1024th assign trips `should_flush`, which in turn calls the
    // WAL appender exactly once and persists the new hwm to the
    // catalog. Subsequent assigns inside the same window will not
    // re-flush until the next threshold.
    let alloc_calls = wal.allocs.load(std::sync::atomic::Ordering::Acquire);
    assert!(
        alloc_calls >= 1,
        "expected at least one SurrogateAlloc WAL emission after {n} allocations, got {alloc_calls}"
    );
    let bind_calls = wal.binds.load(std::sync::atomic::Ordering::Acquire);
    assert_eq!(
        bind_calls as usize, n,
        "expected one SurrogateBind per fresh allocation"
    );
    // The persisted hwm is whatever the most recent flush captured;
    // it must be non-zero and ≤ n (the total allocations so far).
    // Either threshold (1024 ops or 200 ms elapsed) can fire first
    // depending on host wall-clock, so we don't assert exact equality.
    let persisted = cat.get_surrogate_hwm().unwrap();
    assert!(
        persisted > 0 && persisted <= n as u32,
        "expected persisted hwm in (0, {n}], got {persisted}"
    );
}

#[test]
fn assigns_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");
    let s_persisted: Surrogate;
    {
        let cat = SystemCatalog::open(&path).unwrap();
        let reg = fresh_registry();
        let wal = NoopWalAppender;
        let a = SurrogateAssigner::new(&reg, &cat, &wal);
        s_persisted = a.assign("users", b"alice").unwrap();
    }
    // Reopen the catalog — the binding row must survive.
    let cat = SystemCatalog::open(&path).unwrap();
    assert_eq!(
        cat.get_surrogate_for_pk("users", b"alice").unwrap(),
        Some(s_persisted)
    );
    assert_eq!(
        cat.get_pk_for_surrogate("users", s_persisted).unwrap(),
        Some(b"alice".to_vec())
    );
}
