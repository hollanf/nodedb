//! Per-Raft-group applied-index watchers.
//!
//! A single primitive — [`AppliedIndexWatcher`] — tracks the highest
//! log index applied on this node for one Raft group. The
//! [`GroupAppliedWatchers`] registry indexes one watcher per group so
//! callers (proposers, consistent reads, lease renewals, recovery
//! checks) can wait on the apply watermark of *their* group rather
//! than only the metadata group.
//!
//! Bump points (single source of truth for "applied on this node"):
//!
//! 1. [`crate::raft_loop::tick`] after the per-group apply phase
//!    — covers regular committed-entry application for every locally
//!    mounted group, including the metadata group.
//! 2. [`crate::raft_loop::handle_rpc`] after `handle_install_snapshot`
//!    — covers the snapshot-install path where `last_applied` jumps
//!    to `last_included_index` without going through the apply loop.
//!
//! Both bump points read the canonical applied index from the Raft
//! node itself rather than mirroring it from a side channel, so the
//! watcher cannot drift from Raft's own state.

pub mod registry;
pub mod watcher;

pub use registry::GroupAppliedWatchers;
pub use watcher::{AppliedIndexWatcher, WaitOutcome};
