//! Per-group closed-timestamp tracker.
//!
//! Every time a Raft group applies a committed entry, the applier
//! records the wall-clock instant as that group's "closed timestamp".
//! A follower whose closed timestamp for a group is within the
//! caller's staleness bound can serve reads locally — no gateway hop
//! to the leader.
//!
//! The tracker is intentionally simple: one `Instant` per group,
//! updated monotonically. There is no HLC or cross-node coordination
//! here — the closed timestamp is local to this node. Safety comes
//! from the fact that a follower's applied index can only advance
//! (Raft guarantees), so a read served at a given closed timestamp
//! sees a consistent prefix of the log.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Tracks the most recent apply instant per Raft group.
pub struct ClosedTimestampTracker {
    groups: RwLock<HashMap<u64, Instant>>,
}

impl ClosedTimestampTracker {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
        }
    }

    /// Record that `group_id` just applied one or more entries.
    /// Called by the raft-loop applier after each apply batch.
    pub fn mark_applied(&self, group_id: u64) {
        let mut g = self.groups.write().unwrap_or_else(|p| p.into_inner());
        g.insert(group_id, Instant::now());
    }

    /// Record that `group_id` just applied, using a caller-supplied
    /// instant. Exposed for deterministic testing with paused time.
    pub fn mark_applied_at(&self, group_id: u64, at: Instant) {
        let mut g = self.groups.write().unwrap_or_else(|p| p.into_inner());
        g.insert(group_id, at);
    }

    /// Check whether this node's replica of `group_id` has applied
    /// recently enough that a read with `max_staleness` can be
    /// served locally.
    ///
    /// Returns `false` if the group has never applied on this node
    /// (no closed timestamp recorded).
    pub fn is_fresh_enough(&self, group_id: u64, max_staleness: Duration) -> bool {
        let g = self.groups.read().unwrap_or_else(|p| p.into_inner());
        match g.get(&group_id) {
            Some(last) => last.elapsed() <= max_staleness,
            None => false,
        }
    }

    /// Return the age of the closed timestamp for a group, or `None`
    /// if the group has never applied on this node. Useful for
    /// observability (metrics, SHOW commands).
    pub fn staleness(&self, group_id: u64) -> Option<Duration> {
        let g = self.groups.read().unwrap_or_else(|p| p.into_inner());
        g.get(&group_id).map(|last| last.elapsed())
    }
}

impl Default for ClosedTimestampTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_group_is_not_fresh() {
        let tracker = ClosedTimestampTracker::new();
        assert!(!tracker.is_fresh_enough(99, Duration::from_secs(10)));
    }

    #[test]
    fn recently_applied_is_fresh() {
        let tracker = ClosedTimestampTracker::new();
        tracker.mark_applied(1);
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn stale_group_is_not_fresh() {
        let tracker = ClosedTimestampTracker::new();
        let old = Instant::now() - Duration::from_secs(30);
        tracker.mark_applied_at(1, old);
        assert!(!tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }

    #[test]
    fn staleness_returns_none_for_unknown() {
        let tracker = ClosedTimestampTracker::new();
        assert!(tracker.staleness(42).is_none());
    }

    #[test]
    fn staleness_returns_age_for_known() {
        let tracker = ClosedTimestampTracker::new();
        tracker.mark_applied(1);
        let s = tracker.staleness(1).unwrap();
        assert!(s < Duration::from_millis(100));
    }

    #[test]
    fn mark_applied_updates_monotonically() {
        let tracker = ClosedTimestampTracker::new();
        let old = Instant::now() - Duration::from_secs(10);
        tracker.mark_applied_at(1, old);
        assert!(!tracker.is_fresh_enough(1, Duration::from_secs(5)));
        tracker.mark_applied(1);
        assert!(tracker.is_fresh_enough(1, Duration::from_secs(5)));
    }
}
