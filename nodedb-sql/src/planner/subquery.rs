//! Subquery planning (IN, EXISTS, scalar subqueries).
//!
//! Subqueries in WHERE (e.g., `WHERE id IN (SELECT ...)`) are detected
//! during filter conversion and planned as nested SqlPlan nodes.
//! Currently, subquery support is handled at the expression level
//! in the resolver — the planner treats them as opaque sub-plans
//! that the execution engine evaluates.
