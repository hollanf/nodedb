//! Control operation handlers — module root.
//! Submodules: snapshot (WAL, cancel, range scan, checkpoint),
//! crdt (all CRDT operations), convert (JSON→LoroValue).

pub mod calvin;
pub mod convert;
pub mod crdt;
pub mod reindex;
mod reindex_apply;
pub mod snapshot;
