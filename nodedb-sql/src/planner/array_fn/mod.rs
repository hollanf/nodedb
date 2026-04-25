//! Planner for `NDARRAY_*` table-valued / scalar functions.
//!
//! The functions live in two AST shapes:
//!
//! * **Read** (`NDARRAY_SLICE`, `NDARRAY_PROJECT`, `NDARRAY_AGG`,
//!   `NDARRAY_ELEMENTWISE`) — `SELECT * FROM ndarray_xxx(...)`. Parsed
//!   by sqlparser as `TableFactor::Table { name, args: Some(_), .. }`
//!   (Postgres-style table-valued function). [`try_plan_array_table_fn`]
//!   intercepts these before catalog resolution.
//! * **Maintenance** (`NDARRAY_FLUSH`, `NDARRAY_COMPACT`) — bare
//!   `SELECT ndarray_flush(name)` with no FROM clause.
//!   [`try_plan_array_maint_fn`] intercepts these from the constant-
//!   query path.

mod helpers;
mod maint_fn;
mod table_fn;

#[cfg(test)]
mod tests;

pub use maint_fn::try_plan_array_maint_fn;
pub use table_fn::try_plan_array_table_fn;
