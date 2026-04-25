//! Planner analysis passes for selective-predicate → bitmap pushdown.
//!
//! These passes inspect `SqlPlan` join children and return hints about which
//! side qualifies for bitmap-producer emission. The actual `PhysicalPlan`
//! sub-plan construction happens in the `nodedb` convert layer, which calls
//! the hint API here before emitting the final `QueryOp::HashJoin`.

pub mod hashjoin;
pub mod predicate;

pub use hashjoin::BitmapJoinHints;
pub use predicate::BitmapHint;
