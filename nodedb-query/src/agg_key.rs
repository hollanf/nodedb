//! Canonical aggregate key generation.
//!
//! A single source of truth for naming aggregate output fields.
//! Used by the SQL planner (HAVING filter field names) and the
//! Data Plane aggregate handler (result row keys).
//!
//! Examples:
//!   `canonical_agg_key("count", "*")` → `"count(*)"`
//!   `canonical_agg_key("sum", "price")` → `"sum(price)"`
//!   `canonical_agg_key("avg", "score")` → `"avg(score)"`

/// Build the canonical key for an aggregate result field.
///
/// Format: `"{function}({arg})"` — e.g. `count(*)`, `sum(price)`.
/// This MUST be used everywhere an aggregate field is named:
/// - Aggregate result row keys (Data Plane)
/// - HAVING filter field names (SQL planner)
/// - Post-aggregation field references (Control Plane)
#[inline]
pub fn canonical_agg_key(function: &str, arg: &str) -> String {
    format!("{function}({arg})")
}
