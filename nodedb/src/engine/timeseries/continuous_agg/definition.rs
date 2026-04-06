//! Continuous aggregate definition types.
//!
//! Re-exported from `nodedb_types::timeseries::continuous_agg`.
//! Types are defined in the shared crate so both Origin and Lite can use them.

pub use nodedb_types::timeseries::continuous_agg::{
    AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy,
};
