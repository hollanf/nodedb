//! Compensation actions applied when a migration is aborted.
//!
//! A `MigrationAbort` entry carries an ordered list of these
//! compensations. The applier executes them in order; any failure
//! is fatal (no warn-and-continue — a partial abort is as broken
//! as a partial commit).

use serde::{Deserialize, Serialize};

/// A single compensation step emitted as part of a `MigrationAbort` entry.
///
/// Applied by `CacheApplier` in the order listed; all must succeed or
/// the abort itself is fatal.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum Compensation {
    /// Remove a learner that was added by Phase 1 but never promoted.
    ///
    /// Safe to apply idempotently: if the learner is already absent,
    /// the routing-table `remove_group_member` call is a no-op.
    RemoveLearner { group_id: u64, peer_id: u64 },

    /// Remove a voter that was promoted in Phase 2 but the cut-over
    /// never committed.
    ///
    /// Safe to apply idempotently for the same reason as `RemoveLearner`.
    RemoveVoter { group_id: u64, peer_id: u64 },

    /// Restore the routing hint for a group leader.
    ///
    /// Applied when the source-leader was changed by a Phase 2
    /// `PromoteLearner` but the `LeadershipTransfer` never committed.
    /// Tells the routing table to point back to the original leader.
    RestoreLeaderHint { group_id: u64, peer_id: u64 },

    /// Remove a ghost stub that was speculatively inserted before the
    /// cut-over commit, and the commit subsequently failed.
    RemoveGhostStub { vshard_id: u16 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compensation_zerompk_roundtrip() {
        let cases = [
            Compensation::RemoveLearner {
                group_id: 1,
                peer_id: 42,
            },
            Compensation::RemoveVoter {
                group_id: 2,
                peer_id: 7,
            },
            Compensation::RestoreLeaderHint {
                group_id: 3,
                peer_id: 99,
            },
            Compensation::RemoveGhostStub { vshard_id: 10 },
        ];
        for c in &cases {
            let bytes = zerompk::to_msgpack_vec(c).expect("encode");
            let decoded: Compensation = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(*c, decoded);
        }
    }
}
