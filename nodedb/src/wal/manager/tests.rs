use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{Lsn, TenantId, VShardId};

#[test]
fn append_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let t = TenantId::new(1);
    let v = VShardId::new(0);

    let lsn1 = wal.append_put(t, v, b"key1=value1").unwrap();
    let lsn2 = wal.append_put(t, v, b"key2=value2").unwrap();
    let lsn3 = wal.append_delete(t, v, b"key1").unwrap();

    assert_eq!(lsn1, Lsn::new(1));
    assert_eq!(lsn2, Lsn::new(2));
    assert_eq!(lsn3, Lsn::new(3));

    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].payload, b"key1=value1");
    assert_eq!(records[2].payload, b"key1");
}

#[test]
fn crdt_delta_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let t = TenantId::new(5);
    let v = VShardId::new(42);

    let lsn = wal.append_crdt_delta(t, v, b"loro-delta-bytes").unwrap();
    assert_eq!(lsn, Lsn::new(1));

    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].header.record_type, RecordType::CrdtDelta as u32);
    assert_eq!(records[0].header.tenant_id, 5);
    assert_eq!(records[0].header.vshard_id, 42);
    assert_eq!(records[0].payload, b"loro-delta-bytes");
}

#[test]
fn next_lsn_continues_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    {
        let wal = WalManager::open_for_testing(&path).unwrap();
        wal.append_put(TenantId::new(1), VShardId::new(0), b"a")
            .unwrap();
        wal.append_put(TenantId::new(1), VShardId::new(0), b"b")
            .unwrap();
        wal.sync().unwrap();
    }

    let wal = WalManager::open_for_testing(&path).unwrap();
    assert_eq!(wal.next_lsn(), Lsn::new(3));

    let lsn = wal
        .append_put(TenantId::new(1), VShardId::new(0), b"c")
        .unwrap();
    assert_eq!(lsn, Lsn::new(3));
}

#[test]
fn truncate_reclaims_space() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let t = TenantId::new(1);
    let v = VShardId::new(0);

    for i in 0..10u32 {
        wal.append_put(t, v, format!("val-{i}").as_bytes()).unwrap();
    }
    wal.sync().unwrap();

    let result = wal.truncate_before(Lsn::new(5)).unwrap();
    assert_eq!(result.segments_deleted, 0);

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 10);
}

#[test]
fn total_size_and_list_segments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();
    wal.append_put(TenantId::new(1), VShardId::new(0), b"data")
        .unwrap();
    wal.sync().unwrap();

    let size = wal.total_size_bytes().unwrap();
    assert!(size > 0);

    let segments = wal.list_segments().unwrap();
    assert_eq!(segments.len(), 1);
}
