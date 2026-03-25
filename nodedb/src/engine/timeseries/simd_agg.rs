//! SIMD aggregation — re-exported from nodedb-query for shared use.
//!
//! The canonical implementation lives in `nodedb_query::simd_agg` so both
//! Origin and Lite share the same AVX-512/AVX2/NEON kernels.

pub use nodedb_query::simd_agg::*;
