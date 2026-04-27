//! Array CRDT sync wire envelopes.
//!
//! Wire types carry opaque msgpack-encoded payloads (encoded by
//! `nodedb-array::sync::op_codec`) plus the minimum routing fields
//! (array name, HLC bytes). The wire crate does not depend on
//! `nodedb-array`; engine code on the receiving side decodes
//! payloads via `nodedb-array::sync::op_codec::decode_op` etc.

pub mod ack;
pub mod catchup_request;
pub mod delta;
pub mod delta_batch;
pub mod reject;
pub mod schema;
pub mod snapshot;
pub mod snapshot_chunk;

pub use ack::ArrayAckMsg;
pub use catchup_request::ArrayCatchupRequestMsg;
pub use delta::ArrayDeltaMsg;
pub use delta_batch::ArrayDeltaBatchMsg;
pub use reject::{ArrayRejectMsg, ArrayRejectReason};
pub use schema::ArraySchemaSyncMsg;
pub use snapshot::ArraySnapshotMsg;
pub use snapshot_chunk::ArraySnapshotChunkMsg;
