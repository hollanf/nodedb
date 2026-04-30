//! Single source of truth for the `WIRE_FORMAT_VERSION` constant
//! shared between every crate that needs to stamp or interpret it.
//!
//! This is the *cluster-wide* wire format version, distinct from:
//! - `nodedb_cluster::wire::WIRE_VERSION` (the binary frame layout
//!   version of the `VShardEnvelope`),
//! - the v3 RPC frame header version in
//!   `nodedb_cluster::rpc_codec::header` (a private constant of that
//!   module).
//!
//! Bump this when the SPSC bridge, WAL, or RPC payload schemas change
//! in a way that requires a coordinated upgrade. Readers MUST reject
//! messages stamped with a higher version than their own; readers
//! SHOULD accept N-1 for rolling-upgrade compatibility.

/// Cluster-wide wire format version. Stamped on every `NodeInfo` and
/// returned by `nodedb::version::WIRE_FORMAT_VERSION` (a re-export).
pub const WIRE_FORMAT_VERSION: u16 = 4;

/// Minimum wire format version this build can read. Frames stamped
/// below this are rejected.
pub const MIN_WIRE_FORMAT_VERSION: u16 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_does_not_exceed_current() {
        assert!(MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION);
    }

    #[test]
    fn current_is_nonzero() {
        // Version 0 is reserved for "unknown / legacy" handling on
        // downstream paths and must never be the active version.
        assert!(WIRE_FORMAT_VERSION > 0);
    }
}
