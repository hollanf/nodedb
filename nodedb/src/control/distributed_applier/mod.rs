//! Distributed apply machinery — tracks pending Raft proposals and applies
//! committed entries to the local Data Plane.
//!
//! See [`crate::control::wal_replication`] for the full write flow description.

pub mod applier;
pub mod apply_loop;
pub mod propose_tracker;

pub use applier::{ApplyBatch, DistributedApplier, create_distributed_applier};
pub use apply_loop::run_apply_loop;
pub use propose_tracker::{ProposeResult, ProposeTracker};
