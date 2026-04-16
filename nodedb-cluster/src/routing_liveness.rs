//! Liveness-driven routing invalidation.
//!
//! [`RoutingLivenessHook`] is a [`MembershipSubscriber`] that clears
//! the leader hint for every Raft group whose leaseholder has just
//! been marked `Suspect`, `Dead`, or `Left` by the SWIM failure
//! detector. After the hook fires, the next query that consults the
//! routing table observes `leader == 0` (the "no leader known"
//! sentinel) and falls through to a fresh leader discovery via the
//! existing `NotLeader`-triggered election path. Clients see at most
//! one retry: the stale hint, the failed dispatch, and a refreshed
//! leader lookup.
//!
//! The hook is storage-agnostic: it holds `Arc<RwLock<RoutingTable>>`
//! and a resolver closure that maps the string-keyed SWIM `NodeId`
//! to the numeric `u64` id used throughout the rest of the cluster
//! crate. Wiring layers (start_cluster, tests) supply the resolver
//! appropriate to their topology source.
//!
//! The hook is intentionally sync and cheap — a single `RwLock::write`,
//! a linear scan over group_members, and `set_leader(gid, 0)` for
//! each affected group. No I/O, no spawning. That keeps it safe to
//! call directly from the detector run loop.

use std::sync::{Arc, RwLock};

use nodedb_types::NodeId;
use tracing::debug;

use crate::routing::RoutingTable;
use crate::swim::MemberState;
use crate::swim::subscriber::MembershipSubscriber;

/// Resolver mapping SWIM `NodeId` → numeric `u64` routing-table id.
///
/// Returns `None` for members SWIM knows about but the routing table
/// does not (placeholder `seed:<addr>` entries before the first real
/// probe, transient learners, etc.). Those are silently ignored.
pub type NodeIdResolver = Arc<dyn Fn(&NodeId) -> Option<u64> + Send + Sync>;

/// Clears the leader hint for every group led by a node that SWIM
/// has marked Suspect/Dead/Left.
pub struct RoutingLivenessHook {
    routing: Arc<RwLock<RoutingTable>>,
    resolver: NodeIdResolver,
}

impl RoutingLivenessHook {
    pub fn new(routing: Arc<RwLock<RoutingTable>>, resolver: NodeIdResolver) -> Self {
        Self { routing, resolver }
    }
}

impl MembershipSubscriber for RoutingLivenessHook {
    fn on_state_change(&self, node_id: &NodeId, _old: Option<MemberState>, new: MemberState) {
        // Alive transitions are a no-op: the next query will refresh
        // the leader hint naturally on NotLeader. We only invalidate
        // when a leader has observably stopped being reachable.
        if !matches!(
            new,
            MemberState::Suspect | MemberState::Dead | MemberState::Left
        ) {
            return;
        }

        let Some(numeric_id) = (self.resolver)(node_id) else {
            // SWIM knows about a node the routing table doesn't — a
            // seed placeholder, a learner mid-join, or a node that
            // was never registered. Nothing to invalidate.
            return;
        };

        let mut rt = self.routing.write().unwrap_or_else(|p| p.into_inner());
        let affected: Vec<u64> = rt
            .group_members()
            .iter()
            .filter(|(_, info)| info.leader == numeric_id)
            .map(|(gid, _)| *gid)
            .collect();
        for gid in &affected {
            rt.set_leader(*gid, 0);
        }
        if !affected.is_empty() {
            debug!(
                ?node_id,
                ?new,
                numeric_id,
                groups_invalidated = affected.len(),
                "routing liveness hook cleared leader hints"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt_with_leaders(pairs: &[(u64, u64)], rf: usize) -> Arc<RwLock<RoutingTable>> {
        // Build a routing table with `pairs.len()` groups where group
        // `gid` has leader `leader`. Uses the uniform constructor to
        // pick a membership, then overrides the leader.
        let nodes: Vec<u64> = pairs.iter().map(|(_, l)| *l).collect();
        let mut rt = RoutingTable::uniform(pairs.len() as u64, &nodes, rf);
        for (gid, leader) in pairs {
            rt.set_leader(*gid, *leader);
        }
        Arc::new(RwLock::new(rt))
    }

    fn resolver_for(map: &'static [(&'static str, u64)]) -> NodeIdResolver {
        Arc::new(move |nid: &NodeId| {
            map.iter()
                .find(|(s, _)| *s == nid.as_str())
                .map(|(_, n)| *n)
        })
    }

    #[test]
    fn dead_transition_clears_leader_for_owned_groups() {
        let rt = rt_with_leaders(&[(0, 1), (1, 2), (2, 1), (3, 3)], 1);
        let hook =
            RoutingLivenessHook::new(rt.clone(), resolver_for(&[("a", 1), ("b", 2), ("c", 3)]));

        hook.on_state_change(
            &NodeId::new("a"),
            Some(MemberState::Alive),
            MemberState::Dead,
        );

        let guard = rt.read().unwrap();
        assert_eq!(guard.group_info(0).unwrap().leader, 0);
        assert_eq!(guard.group_info(1).unwrap().leader, 2);
        assert_eq!(guard.group_info(2).unwrap().leader, 0);
        assert_eq!(guard.group_info(3).unwrap().leader, 3);
    }

    #[test]
    fn suspect_transition_also_invalidates() {
        let rt = rt_with_leaders(&[(0, 7)], 1);
        let hook = RoutingLivenessHook::new(rt.clone(), resolver_for(&[("x", 7)]));
        hook.on_state_change(
            &NodeId::new("x"),
            Some(MemberState::Alive),
            MemberState::Suspect,
        );
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 0);
    }

    #[test]
    fn alive_transition_is_noop() {
        let rt = rt_with_leaders(&[(0, 5)], 1);
        let hook = RoutingLivenessHook::new(rt.clone(), resolver_for(&[("q", 5)]));
        hook.on_state_change(&NodeId::new("q"), None, MemberState::Alive);
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 5);
    }

    #[test]
    fn unresolved_node_id_is_ignored() {
        let rt = rt_with_leaders(&[(0, 1)], 1);
        let hook = RoutingLivenessHook::new(rt.clone(), resolver_for(&[("a", 1)]));
        // NodeId "seed:127.0.0.1:9000" is not in the resolver map.
        hook.on_state_change(
            &NodeId::new("seed:127.0.0.1:9000"),
            Some(MemberState::Alive),
            MemberState::Dead,
        );
        // Leader untouched because the resolver returned None.
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 1);
    }

    #[test]
    fn left_is_also_invalidating() {
        let rt = rt_with_leaders(&[(0, 2)], 1);
        let hook = RoutingLivenessHook::new(rt.clone(), resolver_for(&[("b", 2)]));
        hook.on_state_change(
            &NodeId::new("b"),
            Some(MemberState::Alive),
            MemberState::Left,
        );
        assert_eq!(rt.read().unwrap().group_info(0).unwrap().leader, 0);
    }
}
