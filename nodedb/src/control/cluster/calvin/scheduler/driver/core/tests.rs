use std::collections::BTreeSet;
use std::sync::Arc;

use crate::control::cluster::calvin::scheduler::lock_manager::{
    AcquireOutcome, LockKey, LockManager, TxnId,
};

#[test]
fn two_non_conflicting_both_dispatch_immediately() {
    let mut lm = LockManager::new();

    let txn1 = TxnId::new(1, 0);
    let txn2 = TxnId::new(1, 1);

    let keys1: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("coll"),
        surrogate: 1,
    }]
    .into();
    let keys2: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("coll"),
        surrogate: 2,
    }]
    .into();

    let o1 = lm.acquire(txn1, keys1);
    let o2 = lm.acquire(txn2, keys2);

    assert_eq!(o1, AcquireOutcome::Ready, "txn1 should be ready");
    assert_eq!(
        o2,
        AcquireOutcome::Ready,
        "txn2 should be ready (disjoint keys)"
    );
}

#[test]
fn two_conflicting_second_dispatches_after_first_completes() {
    let mut lm = LockManager::new();

    let txn1 = TxnId::new(1, 0);
    let txn2 = TxnId::new(1, 1);
    let shared_key: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("coll"),
        surrogate: 42,
    }]
    .into();

    let o1 = lm.acquire(txn1, shared_key.clone());
    assert_eq!(o1, AcquireOutcome::Ready);

    let o2 = lm.acquire(txn2, shared_key.clone());
    assert_eq!(o2, AcquireOutcome::Blocked);

    let unblocked = lm.release(txn1);
    assert!(unblocked.contains(&txn2));

    assert!(lm.is_ready(txn2, &shared_key));
}

#[test]
fn many_mixed_deterministic_dispatch_order() {
    let mut lm = LockManager::new();
    let mut dispatched: Vec<TxnId> = Vec::new();

    let pairs = [(2, 0), (1, 1), (3, 0), (1, 0), (2, 1)];
    for (epoch, pos) in pairs {
        let tid = TxnId::new(epoch, pos);
        let keys: BTreeSet<LockKey> = [LockKey::Surrogate {
            collection: Arc::from(format!("c_{epoch}_{pos}")),
            surrogate: epoch as u32 * 10 + pos,
        }]
        .into();
        let outcome = lm.acquire(tid, keys);
        if outcome == AcquireOutcome::Ready {
            dispatched.push(tid);
        }
    }

    assert_eq!(
        dispatched.len(),
        5,
        "all non-conflicting txns should be ready"
    );

    let mut expected = pairs.map(|(e, p)| TxnId::new(e, p)).to_vec();
    expected.sort();
    let mut sorted_dispatched = dispatched.clone();
    sorted_dispatched.sort();
    assert_eq!(sorted_dispatched, expected);
}

#[test]
fn cross_epoch_raw_blocks_correctly() {
    let mut lm = LockManager::new();

    let txn_n = TxnId::new(1, 0);
    let txn_n1 = TxnId::new(2, 0);

    let key_k: BTreeSet<LockKey> = [LockKey::Surrogate {
        collection: Arc::from("orders"),
        surrogate: 100,
    }]
    .into();

    let o1 = lm.acquire(txn_n, key_k.clone());
    assert_eq!(o1, AcquireOutcome::Ready);

    let o2 = lm.acquire(txn_n1, key_k.clone());
    assert_eq!(o2, AcquireOutcome::Blocked);

    let unblocked = lm.release(txn_n);
    assert!(unblocked.contains(&txn_n1));
    assert!(lm.is_ready(txn_n1, &key_k));
}
