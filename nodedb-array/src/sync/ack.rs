//! Per-replica acknowledgement watermarks for GC frontier tracking.
//!
//! [`AckVector`] tracks the highest [`Hlc`] that each peer has confirmed
//! applying. The minimum across all tracked replicas is the GC frontier:
//! ops below it have been applied everywhere and are safe to collapse into a
//! snapshot and prune from the log.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::sync::hlc::Hlc;
use crate::sync::replica_id::ReplicaId;

/// Per-replica ack watermark table.
///
/// Records grow monotonically — [`AckVector::record`] never moves a watermark
/// backward. [`AckVector::min_ack_hlc`] returns `None` when no peers are known
/// (cannot safely GC without knowing all peers have caught up).
#[derive(
    Clone, Debug, Default, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct AckVector {
    acks: HashMap<ReplicaId, Hlc>,
}

impl AckVector {
    /// Create an empty [`AckVector`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `replica` has applied all ops up to and including `hlc`.
    ///
    /// Monotonic: if the existing watermark for `replica` is already higher,
    /// the call is a silent no-op.
    pub fn record(&mut self, replica: ReplicaId, hlc: Hlc) {
        let entry = self.acks.entry(replica).or_insert(Hlc::ZERO);
        if hlc > *entry {
            *entry = hlc;
        }
    }

    /// Return the ack watermark for a specific replica, if known.
    pub fn ack_for(&self, replica: ReplicaId) -> Option<Hlc> {
        self.acks.get(&replica).copied()
    }

    /// Return the minimum ack HLC across all tracked replicas.
    ///
    /// Returns `None` if no replicas are tracked (GC must not proceed without
    /// knowing every peer's progress).
    pub fn min_ack_hlc(&self) -> Option<Hlc> {
        self.acks.values().copied().min()
    }

    /// Iterate over all replica IDs currently tracked.
    pub fn replicas(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.acks.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::replica_id::ReplicaId;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(0)).unwrap()
    }

    fn r(id: u64) -> ReplicaId {
        ReplicaId::new(id)
    }

    #[test]
    fn record_is_monotonic() {
        let mut av = AckVector::new();
        av.record(r(1), hlc(100));
        av.record(r(1), hlc(50)); // lower — must be ignored
        assert_eq!(av.ack_for(r(1)), Some(hlc(100)));

        av.record(r(1), hlc(200)); // higher — must advance
        assert_eq!(av.ack_for(r(1)), Some(hlc(200)));
    }

    #[test]
    fn min_returns_lowest() {
        let mut av = AckVector::new();
        av.record(r(1), hlc(300));
        av.record(r(2), hlc(100));
        av.record(r(3), hlc(200));
        assert_eq!(av.min_ack_hlc(), Some(hlc(100)));
    }

    #[test]
    fn empty_returns_none() {
        let av = AckVector::new();
        assert_eq!(av.min_ack_hlc(), None);
    }

    #[test]
    fn single_peer() {
        let mut av = AckVector::new();
        av.record(r(7), hlc(999));
        assert_eq!(av.min_ack_hlc(), Some(hlc(999)));
        assert_eq!(av.ack_for(r(7)), Some(hlc(999)));
        assert_eq!(av.ack_for(r(8)), None);
    }

    #[test]
    fn serialize_roundtrip() {
        let mut av = AckVector::new();
        av.record(r(1), hlc(10));
        av.record(r(2), hlc(20));
        let bytes = zerompk::to_msgpack_vec(&av).expect("serialize");
        let back: AckVector = zerompk::from_msgpack(&bytes).expect("deserialize");
        assert_eq!(av.min_ack_hlc(), back.min_ack_hlc());
        assert_eq!(av.ack_for(r(1)), back.ack_for(r(1)));
        assert_eq!(av.ack_for(r(2)), back.ack_for(r(2)));
    }
}
