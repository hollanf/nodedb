//! `MembershipSubscriber` — hook fired whenever SWIM observes a
//! member state transition.
//!
//! The failure detector invokes every registered subscriber *after*
//! applying an update to the [`MembershipList`](super::membership::MembershipList)
//! and dissemination queue, so subscribers see the post-merge view.
//!
//! Subscribers are synchronous and must not block — they typically do
//! cheap in-memory bookkeeping (e.g. clearing a routing leader hint).
//! Heavier work belongs on a dedicated task the subscriber spawns
//! itself.
//!
//! ## Lifecycle
//!
//! - `old = None` means "first time we've seen this node" (insert).
//! - `old = Some(state)` means the member existed and transitioned to
//!   a strictly different `new` state. The detector never calls the
//!   hook for no-op reapplies.
//! - `Left` is terminal — after it fires once the member is gone.

use nodedb_types::NodeId;

use super::member::MemberState;

/// Hook trait for observers that react to SWIM membership changes.
pub trait MembershipSubscriber: Send + Sync {
    /// Called after the membership list has accepted a state change
    /// for `node_id`. `old` is `None` on first-time insert.
    fn on_state_change(&self, node_id: &NodeId, old: Option<MemberState>, new: MemberState);
}
