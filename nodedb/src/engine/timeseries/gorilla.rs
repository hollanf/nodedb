//! Gorilla XOR encoding — re-exported from nodedb-types for shared use.
//!
//! The canonical implementation lives in `nodedb_types::gorilla` so both
//! NodeDB Origin and NodeDB-Lite share the same encoder/decoder.

pub use nodedb_types::gorilla::{GorillaDecoder, GorillaEncoder};
