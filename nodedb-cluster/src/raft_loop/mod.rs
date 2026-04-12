//! Raft event loop — drives MultiRaft ticks and dispatches messages over the transport.
//!
//! This module glues [`crate::multi_raft::MultiRaft`] to
//! [`crate::transport::NexarTransport`]:
//! - Periodic ticks drive elections and heartbeats
//! - `Ready` output is dispatched over QUIC (messages, vote requests)
//! - Incoming RPCs are routed to the correct Raft group (including
//!   server-side `JoinRequest` handling — see `handle_rpc.rs`)
//! - Committed entries are applied via a [`CommitApplier`] callback
//! - RPC responses are fed back asynchronously (non-blocking tick loop)

pub mod handle_rpc;
pub mod loop_core;

pub use loop_core::{CommitApplier, RaftLoop, VShardEnvelopeHandler};
