//! VShardMessageType discriminant constants for array shard RPCs.
//!
//! These values mirror the `VShardMessageType` repr(u16) variants added
//! in `crate::wire`. Kept here as typed constants so coordinator and
//! handler code can refer to them without importing the full enum.

/// Coordinator → shard: execute a coord-range slice.
pub const ARRAY_SHARD_SLICE_REQ: u16 = 80;
/// Shard → coordinator: slice result rows.
pub const ARRAY_SHARD_SLICE_RESP: u16 = 81;
/// Coordinator → shard: compute a partial aggregate.
pub const ARRAY_SHARD_AGG_REQ: u16 = 82;
/// Shard → coordinator: partial aggregate result.
pub const ARRAY_SHARD_AGG_RESP: u16 = 83;
/// Coordinator → shard: write cell batch.
pub const ARRAY_SHARD_PUT_REQ: u16 = 84;
/// Shard → coordinator: put acknowledgement.
pub const ARRAY_SHARD_PUT_RESP: u16 = 85;
/// Coordinator → shard: delete cells by exact coords.
pub const ARRAY_SHARD_DELETE_REQ: u16 = 86;
/// Shard → coordinator: delete acknowledgement.
pub const ARRAY_SHARD_DELETE_RESP: u16 = 87;
/// Coordinator → shard: surrogate bitmap scan.
pub const ARRAY_SHARD_SURROGATE_BITMAP_REQ: u16 = 88;
/// Shard → coordinator: surrogate bitmap result.
pub const ARRAY_SHARD_SURROGATE_BITMAP_RESP: u16 = 89;
