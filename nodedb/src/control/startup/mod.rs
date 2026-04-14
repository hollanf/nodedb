//! Deterministic startup phase sequencer.
//!
//! Every node advances through a fixed sequence of
//! [`StartupPhase`] values from `Boot` to `GatewayEnable`. The
//! `main.rs` startup code calls [`Sequencer::advance_to`] at
//! each phase boundary, and client-facing listeners wait on
//! [`GatewayGuard::await_ready`] before processing the first
//! request. A phase regression or skip is a programming bug
//! and is rejected at the sequencer.
//!
//! See [`phase::StartupPhase`] for the canonical ordering.

pub mod error;
pub mod guard;
pub mod phase;
pub mod sequencer;
pub mod snapshot;

pub use error::SequencerError;
pub use guard::{GatewayGuard, GatewayRefusal};
pub use phase::{PHASE_COUNT, StartupPhase};
pub use sequencer::Sequencer;
pub use snapshot::{PhaseEntry, StartupStatus};
