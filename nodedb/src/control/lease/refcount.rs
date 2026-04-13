//! Per-query descriptor lease refcount + scope guard.
//!
//! Descriptor leases are acquired at plan time and held
//! through execute. Two concurrent queries touching the same
//! descriptor share a single underlying raft lease — we track
//! a per-node refcount so only the first query pays the
//! acquire round-trip and only the last query to finish pays
//! the release round-trip. Intermediate queries hit the
//! fast-path increment / decrement with no raft traffic.
//!
//! The DDL drain path relies on this: when every in-flight
//! query using a descriptor finishes, the refcount hits zero,
//! the lease is actually released via a
//! `DescriptorLeaseRelease` raft entry, and drain's poll loop
//! observes the lease clear. Long-running queries naturally
//! bound the drain window — if a query exceeds
//! `DEFAULT_DRAIN_TIMEOUT` the ALTER fails with a drain-timeout
//! error and the operator retries.
//!
//! ## Guard semantics
//!
//! `QueryLeaseScope` is the owned collection of leases a
//! single query accumulated during planning. The scope drops
//! when the query's pgwire handler finishes executing (after
//! every response has been returned). Drop walks the scope,
//! decrements each refcount, and — for any entry whose count
//! hits zero — spawns a background task to propose the
//! release entry. The spawn is mandatory because `Drop` cannot
//! be async; the drop handler itself returns immediately.
//!
//! A dropped `QueryLeaseScope` therefore schedules (but does
//! not await) the release. Drain's poll loop observes the
//! release after the raft round-trip lands on the leader —
//! sub-10ms in a healthy cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use nodedb_cluster::DescriptorId;
use tracing::warn;

use crate::control::state::SharedState;

/// Host-side lease reference counts. One entry per descriptor
/// id this node currently holds a lease on; the value is the
/// number of in-flight queries holding the lease.
#[derive(Debug, Default)]
pub struct LeaseRefCount {
    counts: Mutex<HashMap<DescriptorId, u32>>,
}

impl LeaseRefCount {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the refcount for `id`. Returns the new count.
    /// If the returned count is `1`, the caller is the first
    /// holder and must perform the actual raft acquire. Higher
    /// values mean the lease is already held and the caller
    /// hit the fast path.
    pub fn increment(&self, id: &DescriptorId) -> u32 {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map.entry(id.clone()).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Decrement the refcount for `id`. Returns the new count.
    /// If the returned count is `0`, the caller was the last
    /// holder and must perform the actual raft release. The
    /// entry is removed from the map on the same call.
    pub fn decrement(&self, id: &DescriptorId) -> u32 {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = map.get_mut(id) {
            *entry = entry.saturating_sub(1);
            let c = *entry;
            if c == 0 {
                map.remove(id);
            }
            c
        } else {
            0
        }
    }

    /// Read the current refcount for `id` (for tests / diagnostics).
    pub fn current(&self, id: &DescriptorId) -> u32 {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.get(id).copied().unwrap_or(0)
    }

    /// Total number of descriptors with a non-zero refcount.
    pub fn distinct_count(&self) -> usize {
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.len()
    }
}

/// Owned collection of lease holds for one query.
///
/// Created by `OriginCatalog::take_lease_scope()` after
/// planning finishes; held by the pgwire handler through the
/// execute phase; released on drop.
pub struct QueryLeaseScope {
    /// Descriptors this query held a refcount on.
    descriptor_ids: Vec<DescriptorId>,
    /// Weak reference to the process-wide shared state so drop
    /// can reach the refcount map and the raft propose path
    /// without keeping the state alive past normal shutdown.
    shared: Weak<SharedState>,
}

impl QueryLeaseScope {
    /// Create an empty scope that releases nothing on drop.
    /// Used as a default / placeholder when the caller does
    /// not need lease tracking (e.g., internal sub-planners).
    pub fn empty() -> Self {
        Self {
            descriptor_ids: Vec::new(),
            shared: Weak::new(),
        }
    }

    /// Build a scope from a list of descriptor ids already
    /// incremented on the node's `lease_refcount`.
    pub fn new(descriptor_ids: Vec<DescriptorId>, shared: &Arc<SharedState>) -> Self {
        Self {
            descriptor_ids,
            shared: Arc::downgrade(shared),
        }
    }

    /// Number of descriptors held in this scope.
    pub fn len(&self) -> usize {
        self.descriptor_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.descriptor_ids.is_empty()
    }
}

impl Drop for QueryLeaseScope {
    fn drop(&mut self) {
        if self.descriptor_ids.is_empty() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        // Decrement each refcount and collect the ids whose
        // count just hit zero — those need the actual raft
        // release.
        let mut to_release = Vec::new();
        for id in self.descriptor_ids.drain(..) {
            let new_count = shared.lease_refcount.decrement(&id);
            if new_count == 0 {
                to_release.push(id);
            }
        }
        if to_release.is_empty() {
            return;
        }
        // Release is sync + uses `block_in_place`; spawning to
        // the tokio runtime lets `Drop` return immediately
        // while the release propose proceeds in the background.
        // This is best-effort: if the runtime is already shut
        // down (e.g., test teardown race) the spawn fails
        // silently and the lease drains via TTL.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let shared = Arc::clone(&shared);
            handle.spawn(async move {
                let shared_inner = Arc::clone(&shared);
                let descriptor_ids = to_release.clone();
                let result = tokio::task::spawn_blocking(move || {
                    shared_inner.release_descriptor_leases(descriptor_ids)
                })
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!(
                            error = %e,
                            count = to_release.len(),
                            "QueryLeaseScope drop: background release failed"
                        );
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            count = to_release.len(),
                            "QueryLeaseScope drop: spawn_blocking panicked"
                        );
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::DescriptorKind;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(1, DescriptorKind::Collection, name.to_string())
    }

    #[test]
    fn first_increment_returns_one() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        assert_eq!(rc.increment(&a), 1);
    }

    #[test]
    fn second_increment_returns_two() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a);
        assert_eq!(rc.increment(&a), 2);
    }

    #[test]
    fn decrement_to_zero_removes_entry() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a);
        assert_eq!(rc.decrement(&a), 0);
        assert_eq!(rc.current(&a), 0);
        assert_eq!(rc.distinct_count(), 0);
    }

    #[test]
    fn decrement_preserves_shared_lease() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        rc.increment(&a);
        rc.increment(&a);
        assert_eq!(rc.decrement(&a), 1);
        assert_eq!(rc.current(&a), 1);
        assert_eq!(rc.distinct_count(), 1);
    }

    #[test]
    fn distinct_descriptors_track_independently() {
        let rc = LeaseRefCount::new();
        let a = id("a");
        let b = id("b");
        rc.increment(&a);
        rc.increment(&b);
        assert_eq!(rc.distinct_count(), 2);
        rc.decrement(&a);
        assert_eq!(rc.distinct_count(), 1);
        assert_eq!(rc.current(&a), 0);
        assert_eq!(rc.current(&b), 1);
    }

    #[test]
    fn decrement_on_unknown_id_is_safe() {
        let rc = LeaseRefCount::new();
        assert_eq!(rc.decrement(&id("nothing")), 0);
    }

    #[test]
    fn empty_scope_drops_cleanly() {
        let scope = QueryLeaseScope::empty();
        drop(scope); // should not panic even without a runtime
    }
}
