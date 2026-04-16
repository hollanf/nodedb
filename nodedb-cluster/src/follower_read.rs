//! Follower-read decision gate.
//!
//! [`FollowerReadGate`] answers a single question: "given the
//! session's `ReadConsistency` and the local node's role + closed
//! timestamp for the target Raft group, can this read be served
//! locally without forwarding to the leader?"
//!
//! ## Decision table
//!
//! | Consistency           | Local role  | Closed TS fresh? | Serve locally? |
//! |-----------------------|-------------|------------------|----------------|
//! | Strong                | *           | *                | Only if leader |
//! | BoundedStaleness(d)   | Follower    | ≤ d              | Yes            |
//! | BoundedStaleness(d)   | Follower    | > d              | No → forward   |
//! | BoundedStaleness(d)   | Leader      | *                | Yes            |
//! | Eventual              | *           | *                | Yes            |
//!
//! The gate is stateless — it reads from shared handles to the
//! closed-timestamp tracker and the raft-status provider.

use std::sync::Arc;
use std::time::Duration;

use crate::closed_timestamp::ClosedTimestampTracker;

/// Consistency level for a single read — mirrors the `ReadConsistency`
/// enum in the `nodedb` crate without coupling `nodedb-cluster` to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadLevel {
    Strong,
    BoundedStaleness(Duration),
    Eventual,
}

/// Answers "can this read be served locally?"
pub struct FollowerReadGate {
    closed_ts: Arc<ClosedTimestampTracker>,
    /// Type-erased function that returns true if this node is the
    /// leader for the given group. Injection seam — production wraps
    /// `MultiRaft::group_statuses`, tests supply a closure.
    is_leader_fn: Box<dyn Fn(u64) -> bool + Send + Sync>,
}

impl FollowerReadGate {
    pub fn new(
        closed_ts: Arc<ClosedTimestampTracker>,
        is_leader_fn: Box<dyn Fn(u64) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            closed_ts,
            is_leader_fn,
        }
    }

    /// Returns `true` if the read can be served from this node's
    /// local replica without forwarding to the leader.
    pub fn can_serve_locally(&self, group_id: u64, level: ReadLevel) -> bool {
        match level {
            ReadLevel::Strong => (self.is_leader_fn)(group_id),
            ReadLevel::Eventual => true,
            ReadLevel::BoundedStaleness(max) => {
                if (self.is_leader_fn)(group_id) {
                    return true;
                }
                self.closed_ts.is_fresh_enough(group_id, max)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(leader_groups: &'static [u64]) -> FollowerReadGate {
        FollowerReadGate::new(
            Arc::new(ClosedTimestampTracker::new()),
            Box::new(move |gid| leader_groups.contains(&gid)),
        )
    }

    fn gate_with_tracker(
        leader_groups: &'static [u64],
        tracker: Arc<ClosedTimestampTracker>,
    ) -> FollowerReadGate {
        FollowerReadGate::new(tracker, Box::new(move |gid| leader_groups.contains(&gid)))
    }

    #[test]
    fn strong_requires_leader() {
        let g = gate(&[1]);
        assert!(g.can_serve_locally(1, ReadLevel::Strong));
        assert!(!g.can_serve_locally(2, ReadLevel::Strong));
    }

    #[test]
    fn eventual_always_local() {
        let g = gate(&[]);
        assert!(g.can_serve_locally(99, ReadLevel::Eventual));
    }

    #[test]
    fn bounded_staleness_leader_always_local() {
        let g = gate(&[1]);
        assert!(g.can_serve_locally(1, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }

    #[test]
    fn bounded_staleness_follower_fresh_enough() {
        let tracker = Arc::new(ClosedTimestampTracker::new());
        tracker.mark_applied(2);
        let g = gate_with_tracker(&[], tracker);
        assert!(g.can_serve_locally(2, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }

    #[test]
    fn bounded_staleness_follower_too_stale() {
        let tracker = Arc::new(ClosedTimestampTracker::new());
        let old = std::time::Instant::now() - Duration::from_secs(30);
        tracker.mark_applied_at(2, old);
        let g = gate_with_tracker(&[], tracker);
        assert!(!g.can_serve_locally(2, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }

    #[test]
    fn bounded_staleness_unknown_group_not_local() {
        let g = gate(&[]);
        assert!(!g.can_serve_locally(99, ReadLevel::BoundedStaleness(Duration::from_secs(5))));
    }
}
