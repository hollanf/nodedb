//! Elastic scaling glue — ties SWIM membership transitions to the
//! rebalancer loop so new/departing nodes trigger an immediate sweep
//! instead of waiting for the next 30 s tick.
//!
//! ## Add-node path
//!
//! 1. Node joins via the existing bootstrap/join RPC path.
//! 2. `CacheApplier` with live state applies `TopologyChange::Join`
//!    + `PromoteToVoter`, adding the node to the live topology.
//! 3. SWIM detects the new node as `Alive` through gossip.
//! 4. [`RebalancerKickHook`] (a [`MembershipSubscriber`]) fires
//!    [`Notify::notify_one`] on the rebalancer loop's kick handle.
//! 5. The loop wakes, collects metrics (including the new node's
//!    low load score), and dispatches moves to the new node.
//!
//! ## Remove-node path
//!
//! 1. Operator runs `cluster decommission N` (Phase E.4).
//! 2. The decommission flow strips the node from all groups and
//!    removes it from topology.
//! 3. SWIM detects the node as `Dead` / `Left`.
//! 4. The same kick hook wakes the rebalancer so it re-evaluates
//!    whether the remaining nodes are balanced.
//!
//! No new data types or traits — just a [`MembershipSubscriber`]
//! impl holding a shared `Arc<Notify>`.

use std::sync::Arc;

use nodedb_types::NodeId;
use tokio::sync::Notify;
use tracing::debug;

use crate::swim::member::MemberState;
use crate::swim::subscriber::MembershipSubscriber;

/// SWIM [`MembershipSubscriber`] that triggers an immediate
/// rebalancer sweep on membership-relevant transitions.
///
/// Relevant transitions are:
/// - `None → Alive` (first time a new node is seen — add path)
/// - `_ → Dead` / `_ → Left` (node departure — remove path)
/// - `_ → Alive` after `Dead`/`Left` (node recovery)
///
/// All other transitions (Alive → Suspect, Suspect → Alive) are
/// transient and do not change the set of Active nodes, so they
/// are ignored.
pub struct RebalancerKickHook {
    kick: Arc<Notify>,
}

impl RebalancerKickHook {
    pub fn new(kick: Arc<Notify>) -> Self {
        Self { kick }
    }
}

impl MembershipSubscriber for RebalancerKickHook {
    fn on_state_change(&self, node_id: &NodeId, old: Option<MemberState>, new: MemberState) {
        let relevant = match (old, new) {
            // First-time insert as Alive (new node joined).
            (None, MemberState::Alive) => true,
            // Node died or left.
            (_, MemberState::Dead) | (_, MemberState::Left) => true,
            // Node recovered from Dead/Left back to Alive.
            (Some(MemberState::Dead), MemberState::Alive)
            | (Some(MemberState::Left), MemberState::Alive) => true,
            _ => false,
        };
        if relevant {
            debug!(?node_id, ?old, ?new, "rebalancer kick: membership change");
            self.kick.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn counting_notify() -> (Arc<Notify>, Arc<AtomicU32>, tokio::task::JoinHandle<()>) {
        let notify = Arc::new(Notify::new());
        let counter = Arc::new(AtomicU32::new(0));
        let n = notify.clone();
        let c = counter.clone();
        let handle = tokio::spawn(async move {
            loop {
                n.notified().await;
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        (notify, counter, handle)
    }

    #[tokio::test]
    async fn kick_fires_on_new_node_alive() {
        let (notify, counter, handle) = counting_notify();
        let hook = RebalancerKickHook::new(notify);
        hook.on_state_change(
            &NodeId::try_new("new").expect("test fixture"),
            None,
            MemberState::Alive,
        );
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(counter.load(Ordering::SeqCst) >= 1);
        handle.abort();
    }

    #[tokio::test]
    async fn kick_fires_on_dead() {
        let (notify, counter, handle) = counting_notify();
        let hook = RebalancerKickHook::new(notify);
        hook.on_state_change(
            &NodeId::try_new("x").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Dead,
        );
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(counter.load(Ordering::SeqCst) >= 1);
        handle.abort();
    }

    #[tokio::test]
    async fn kick_fires_on_left() {
        let (notify, counter, handle) = counting_notify();
        let hook = RebalancerKickHook::new(notify);
        hook.on_state_change(
            &NodeId::try_new("x").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Left,
        );
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(counter.load(Ordering::SeqCst) >= 1);
        handle.abort();
    }

    #[test]
    fn kick_does_not_fire_on_suspect() {
        let notify = Arc::new(Notify::new());
        let hook = RebalancerKickHook::new(notify);
        hook.on_state_change(
            &NodeId::try_new("x").expect("test fixture"),
            Some(MemberState::Alive),
            MemberState::Suspect,
        );
    }
}
