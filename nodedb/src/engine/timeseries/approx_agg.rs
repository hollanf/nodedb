//! Approximate aggregation functions — re-exported from nodedb-types.
//!
//! The canonical implementation lives in `nodedb_types::approx` so Origin,
//! Lite, and Cluster can all use the same HLL/TDigest/SpaceSaving.

pub use nodedb_types::approx::{HyperLogLog, SpaceSaving, TDigest};
