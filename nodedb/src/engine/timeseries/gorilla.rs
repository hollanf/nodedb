//! Gorilla XOR encoding — re-exported from nodedb-codec for shared use.
//!
//! The canonical implementation lives in `nodedb_codec::gorilla` so both
//! NodeDB Origin and NodeDB-Lite share the same encoder/decoder.

pub use nodedb_types::gorilla::{GorillaDecoder, GorillaEncoder};
