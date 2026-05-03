//! Calvin scheduler restart idempotency test.
//!
//! Tests that:
//! 1. After applying N epochs, the WAL records the applied epochs correctly.
//! 2. On restart (new WalManager over the same directory), the recovery scanner
//!    reads back the correct `last_applied_epoch`.
//! 3. Applying the same epochs again (simulating log replay) is a no-op because
//!    `process_new_txn` skips epochs <= `last_applied_epoch`.
//!
//! The full Raft-log-read path (`MultiRaft::read_committed_entries` + scheduler
//! rebuild loop) requires a running cluster and is gated by `#[ignore]` — the
//! note explains what the test would verify and what is needed to enable it.

use tempfile::TempDir;

use nodedb::control::cluster::calvin::scheduler::read_last_applied_epoch;
use nodedb::types::VShardId;
use nodedb::wal::manager::WalManager;

// ── Helper ────────────────────────────────────────────────────────────────────

fn open_wal(dir: &TempDir) -> WalManager {
    WalManager::open_for_testing(dir.path()).expect("open wal")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Simulating 5 epochs: append CalvinApplied records, sync, then verify that a
/// freshly-opened WalManager over the same directory reads `last_applied_epoch=5`.
#[test]
fn scheduler_restart_reads_last_applied_epoch_after_five_epochs() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 3u32;

    // Simulate applying 5 epochs (epoch 1..=5, each at position 0).
    {
        let wal = open_wal(&dir);
        for epoch in 1u64..=5 {
            wal.append_calvin_applied(VShardId::new(vshard_id), epoch, 0)
                .unwrap();
        }
        // Flush to disk so the new WalManager can read them back.
        wal.sync().unwrap();
    }

    // Reopen (simulates a node restart — new WalManager, same directory).
    {
        let wal = open_wal(&dir);
        let last_epoch =
            read_last_applied_epoch(&wal, vshard_id).expect("recovery scan should succeed");
        assert_eq!(
            last_epoch, 5,
            "after five epochs, last_applied_epoch should be 5"
        );
    }
}

/// Verify that epochs applied out-of-order in the WAL are still reported
/// as the max value.  The recovery scanner must return the max, not the last
/// appended.
#[test]
fn scheduler_restart_returns_max_epoch_not_last_written() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 7u32;

    {
        let wal = open_wal(&dir);
        // Write in non-monotonic order to confirm the scanner finds the max.
        wal.append_calvin_applied(VShardId::new(vshard_id), 3, 0)
            .unwrap();
        wal.append_calvin_applied(VShardId::new(vshard_id), 1, 0)
            .unwrap();
        wal.append_calvin_applied(VShardId::new(vshard_id), 5, 0)
            .unwrap();
        wal.append_calvin_applied(VShardId::new(vshard_id), 2, 0)
            .unwrap();
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let last_epoch = read_last_applied_epoch(&wal, vshard_id).unwrap();
        assert_eq!(
            last_epoch, 5,
            "scanner should return the max epoch (5), not the last written (2)"
        );
    }
}

/// Greenfield: a WAL with no CalvinApplied records should return 0.
#[test]
fn scheduler_restart_greenfield_returns_zero() {
    let dir = TempDir::new().unwrap();
    let wal = open_wal(&dir);
    let last_epoch = read_last_applied_epoch(&wal, 1).unwrap();
    assert_eq!(last_epoch, 0, "greenfield WAL should return epoch 0");
}

/// Multiple vshards: recovery for one vshard should not see epochs from another.
#[test]
fn scheduler_restart_vshard_isolation() {
    let dir = TempDir::new().unwrap();

    {
        let wal = open_wal(&dir);
        wal.append_calvin_applied(VShardId::new(1), 10, 0).unwrap();
        wal.append_calvin_applied(VShardId::new(2), 99, 0).unwrap();
        wal.append_calvin_applied(VShardId::new(1), 20, 0).unwrap();
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let e1 = read_last_applied_epoch(&wal, 1).unwrap();
        let e2 = read_last_applied_epoch(&wal, 2).unwrap();
        assert_eq!(e1, 20, "vshard 1 should see its own max epoch (20)");
        assert_eq!(e2, 99, "vshard 2 should see its own max epoch (99)");
    }
}

/// Full scheduler rebuild integration test.
///
/// This test would:
/// 1. Create a SequencerStateMachine with pre-loaded epoch batches (epochs 1–5,
///    each containing a single SequencedTxn for vshard 1).
/// 2. Use `replay_epochs_for_vshard` to decode the Raft log entries for vshard 1.
/// 3. Feed each decoded SequencedTxn through a `Scheduler` (with a mock SPSC
///    bridge that records dispatch calls).
/// 4. Assert that after processing all 5 epochs, `scheduler.is_caught_up()`
///    returns true and the mock bridge received exactly 5 dispatch calls.
/// 5. Simulate a restart: create a new Scheduler with `last_applied_epoch` read
///    from the WAL (which should be 5) and `rebuild_target_epoch = 5`. Feed the
///    same 5 epochs again. Assert 0 new dispatches (deduplication by
///    `epoch <= last_applied_epoch`).
///
/// **Blocked on**: this test requires a running Scheduler (which takes a real
/// `Dispatcher` and `RequestTracker`) or a substantial mock harness.  Wiring
/// a full Scheduler in an integration test requires the same SPSC bridge setup
/// used by the cluster integration tests, which implies a running `TpcCore`.
/// Until a lightweight mock-dispatcher harness is provided (tracked separately),
/// this test is `#[ignore]`d.  The lock manager, driver, metrics, and recovery
/// unit tests in `src/control/cluster/calvin/scheduler/` already cover all the
/// logic; this integration test would add end-to-end confidence.
#[test]
#[ignore = "requires mock SPSC dispatcher harness — see test doc comment"]
fn full_scheduler_rebuild_is_idempotent() {
    // Implementation blocked on mock Dispatcher harness.
}
