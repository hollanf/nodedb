//! High-concurrency dependent-read test.
//!
//! Full 100 × 4-passive run requires a real cluster.  This file provides
//! the structural harness with the real test marked `#[ignore]`.

/// Full high-concurrency test: 100 parallel dependent-read txns × 4 passives.
///
/// Asserts per-vshard Raft entry rate ≤ 100/epoch, p99 ≤ 3 × epoch_duration.
///
/// Requires real 3-node cluster + SPSC bridge wired.
#[tokio::test]
#[ignore]
async fn dependent_read_100x4_passives_bounded_raft_rate() {
    // Spawn 100 txns each touching 4 passive vshards.
    // After all complete, assert:
    //   - per-vshard Raft entry count ≤ 100 (one per txn, batched per vshard).
    //   - p99 latency ≤ 3 × epoch_duration.
    todo!("requires real 3-node cluster harness")
}
