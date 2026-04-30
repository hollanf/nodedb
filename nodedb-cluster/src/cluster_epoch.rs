//! Cluster generation/epoch — a monotonic, leader-bumped fence token
//! stamped on every Raft RPC frame.
//!
//! # Purpose
//!
//! `cluster_epoch` is a process-wide `u64` that advances on every
//! cluster-membership-shifting event the metadata-group leader observes
//! (currently: becoming leader of the metadata group). Stamping it on every
//! outbound RPC and observing it on every inbound RPC lets every peer keep
//! a cluster-wide high-water mark and reject (or quarantine) frames from
//! peers stuck on a strictly older epoch — i.e. peers that missed a topology
//! transition and may be acting on stale state.
//!
//! # Mechanics
//!
//! * A process-global `AtomicU64`, [`LOCAL_CLUSTER_EPOCH`], holds the local
//!   high-water mark. On startup it is loaded from the cluster catalog
//!   (see [`ClusterCatalog::load_cluster_epoch`]).
//! * [`current_local_cluster_epoch`] reads it for the encoder.
//! * [`observe_peer_cluster_epoch`] is called by the decoder for every
//!   inbound frame; it bumps the local mark via `fetch_max` (monotonic).
//! * [`bump_local_cluster_epoch`] is called by the metadata-group leader
//!   when leadership transitions to it; it advances the local mark and
//!   persists the new value.
//!
//! Persistence is best-effort: a bump is committed in-memory atomically;
//! if the catalog write fails, the in-memory value is still advanced and
//! the failure is logged. (After a crash, the persisted value is a lower
//! bound — the new leader will re-bump beyond it.)
//!
//! # Why a global atomic
//!
//! Every encode/decode call site needs the current epoch. Threading it
//! through the existing 19 encode/decode functions and their callers
//! would touch hundreds of sites for what is, semantically, a single
//! per-process value. An `AtomicU64` is the right shape for this kind
//! of read-mostly, monotonic counter.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::ClusterCatalog;
use crate::error::Result;

/// Process-global cluster epoch high-water mark.
///
/// Initialized to 0 (genesis). Loaded from catalog at startup via
/// [`init_local_cluster_epoch_from_catalog`].
static LOCAL_CLUSTER_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Read the current local cluster epoch (the value an outbound RPC
/// should stamp). Cheap; lock-free.
pub fn current_local_cluster_epoch() -> u64 {
    LOCAL_CLUSTER_EPOCH.load(Ordering::Acquire)
}

/// Set the local epoch directly. Only intended for use by
/// [`init_local_cluster_epoch_from_catalog`] at startup and by tests.
/// Production code paths use [`observe_peer_cluster_epoch`] (monotonic
/// max) or [`bump_local_cluster_epoch`] (leader-side increment).
pub fn set_local_cluster_epoch(value: u64) {
    LOCAL_CLUSTER_EPOCH.store(value, Ordering::Release);
}

/// Observe an epoch carried by an inbound RPC. Advances the local mark
/// via `fetch_max` (so concurrent observations are safe and monotonic).
///
/// Returns the new local high-water mark.
pub fn observe_peer_cluster_epoch(peer_epoch: u64) -> u64 {
    let prev = LOCAL_CLUSTER_EPOCH.fetch_max(peer_epoch, Ordering::AcqRel);
    prev.max(peer_epoch)
}

/// Increment the local epoch by 1 and persist the new value to the
/// cluster catalog. Called by the metadata-group leader on a leadership
/// transition.
///
/// Returns the new epoch. The persistence failure path advances the
/// in-memory value anyway (so RPCs immediately reflect the bump) and
/// returns the persistence error to the caller.
pub fn bump_local_cluster_epoch(catalog: &ClusterCatalog) -> Result<u64> {
    let new_epoch = LOCAL_CLUSTER_EPOCH.fetch_add(1, Ordering::AcqRel) + 1;
    catalog.save_cluster_epoch(new_epoch)?;
    Ok(new_epoch)
}

/// Initialize the local epoch from the cluster catalog at process
/// startup. Idempotent — safe to call multiple times during boot.
pub fn init_local_cluster_epoch_from_catalog(catalog: &Arc<ClusterCatalog>) -> Result<u64> {
    let persisted = catalog.load_cluster_epoch()?.unwrap_or(0);
    // fetch_max so we don't regress past anything already observed
    // earlier in startup (e.g. an inbound frame on the join path).
    let prev = LOCAL_CLUSTER_EPOCH.fetch_max(persisted, Ordering::AcqRel);
    Ok(prev.max(persisted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// All tests in this module mutate the same global atomic, so they
    /// must be serialised. The lock is taken at the top of each test.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn reset() -> std::sync::MutexGuard<'static, ()> {
        let g = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        LOCAL_CLUSTER_EPOCH.store(0, Ordering::Release);
        g
    }

    #[test]
    fn observe_is_monotonic_max() {
        let _g = reset();
        assert_eq!(observe_peer_cluster_epoch(5), 5);
        assert_eq!(current_local_cluster_epoch(), 5);
        // Older peer epoch must not regress the local mark.
        assert_eq!(observe_peer_cluster_epoch(3), 5);
        assert_eq!(current_local_cluster_epoch(), 5);
        // Newer advances.
        assert_eq!(observe_peer_cluster_epoch(7), 7);
        assert_eq!(current_local_cluster_epoch(), 7);
    }

    #[test]
    fn set_overrides_for_init() {
        let _g = reset();
        set_local_cluster_epoch(42);
        assert_eq!(current_local_cluster_epoch(), 42);
    }

    #[test]
    fn observe_zero_is_noop() {
        let _g = reset();
        set_local_cluster_epoch(9);
        assert_eq!(observe_peer_cluster_epoch(0), 9);
        assert_eq!(current_local_cluster_epoch(), 9);
    }

    #[test]
    fn bump_increments_and_persists() {
        let _g = reset();
        let dir = tempfile::tempdir().unwrap();
        let catalog = ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap();
        set_local_cluster_epoch(10);
        let new_epoch = bump_local_cluster_epoch(&catalog).unwrap();
        assert_eq!(new_epoch, 11);
        assert_eq!(current_local_cluster_epoch(), 11);
        assert_eq!(catalog.load_cluster_epoch().unwrap(), Some(11));
    }

    #[test]
    fn init_from_catalog_loads_persisted_value() {
        let _g = reset();
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap());
        catalog.save_cluster_epoch(123).unwrap();
        let v = init_local_cluster_epoch_from_catalog(&catalog).unwrap();
        assert_eq!(v, 123);
        assert_eq!(current_local_cluster_epoch(), 123);
    }

    #[test]
    fn init_with_no_persisted_value_starts_at_zero() {
        let _g = reset();
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(ClusterCatalog::open(&dir.path().join("cluster.redb")).unwrap());
        let v = init_local_cluster_epoch_from_catalog(&catalog).unwrap();
        assert_eq!(v, 0);
        assert_eq!(current_local_cluster_epoch(), 0);
    }
}
