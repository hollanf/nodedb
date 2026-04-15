//! Deterministic startup phase sequencer.
//!
//! Every node advances through a fixed sequence of [`StartupPhase`] values.
//! The **gate model** ([`StartupSequencer`]) is the canonical API: every
//! subsystem that must complete before a phase transition registers a
//! [`ReadyGate`] and fires it when it finishes startup work. The sequencer
//! advances automatically when all gates for a phase have fired.
//!
//! Observers — listeners, health checks — hold an [`Arc<StartupGate>`] and
//! call [`StartupGate::await_phase`] to block until a specific phase is
//! reached.
//!
//! [`StartupSequencer`]: startup_sequencer::StartupSequencer
//! [`StartupGate::await_phase`]: gate::StartupGate::await_phase

pub mod error;
pub mod gate;
pub mod health;
pub mod phase;
pub mod startup_sequencer;

pub use error::StartupError;
pub use gate::{ReadyGate, SequencerSnapshot, StartupGate};
pub use phase::{PHASE_COUNT, StartupPhase};
pub use startup_sequencer::StartupSequencer;
