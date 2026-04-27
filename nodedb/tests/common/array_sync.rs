//! Shared helpers for array CRDT sync integration tests.

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;

#[allow(dead_code)]
pub fn rep(id: u64) -> ReplicaId {
    ReplicaId::new(id)
}

#[allow(dead_code)]
pub fn hlc(ms: u64, rid: u64) -> Hlc {
    Hlc::new(ms, 0, rep(rid)).expect("valid HLC")
}
