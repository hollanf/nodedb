//! # synapsedb-crdt
//!
//! CRDT engine with SQL constraint validation for SynapseDB.
//!
//! ## The CRDT / SQL Paradox
//!
//! CRDTs are AP (Available + Partition-tolerant): agents compute optimistic
//! deltas offline and sync later. SQL constraints are CP (Consistent +
//! Partition-tolerant): UNIQUE indexes, foreign keys, etc. must hold globally.
//!
//! This crate bridges the gap:
//!
//! 1. **Optimistic local writes** — agents apply deltas to their local `LoroDoc`
//!    without constraint checks (AP behavior for availability).
//! 2. **Constraint validation at commit** — when deltas sync to the leader,
//!    constraints are validated against the committed state.
//! 3. **Dead-letter queue** — rejected deltas are routed to a DLQ with
//!    compensation hints so the application can recover gracefully.
//! 4. **Pre-validation** — optional fast-reject against the leader's state
//!    before the full Raft round-trip, reducing wasted consensus bandwidth.

pub mod constraint;
pub mod dead_letter;
pub mod error;
pub mod pre_validate;
pub mod state;
pub mod validator;

pub use constraint::{Constraint, ConstraintKind, ConstraintSet};
pub use dead_letter::{CompensationHint, DeadLetterQueue};
pub use error::{CrdtError, Result};
pub use state::CrdtState;
pub use validator::{ValidationOutcome, Validator};
