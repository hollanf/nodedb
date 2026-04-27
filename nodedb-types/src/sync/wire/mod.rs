//! Sync wire protocol: frame format and message types.
//!
//! Frame format: `[msg_type: 1B][length: 4B LE][rkyv/msgpack body]`
//!
//! Message types:
//! - `0x01` Handshake (client → server)
//! - `0x02` HandshakeAck (server → client)
//! - `0x10` DeltaPush (client → server)
//! - `0x11` DeltaAck (server → client)
//! - `0x12` DeltaReject (server → client)
//! - `0x14` CollectionPurged (server → client)
//! - `0x20` ShapeSubscribe (client → server)
//! - `0x21` ShapeSnapshot (server → client)
//! - `0x22` ShapeDelta (server → client)
//! - `0x23` ShapeUnsubscribe (client → server)
//! - `0x30` VectorClockSync (bidirectional)
//! - `0x40` TimeseriesPush (client → server)
//! - `0x41` TimeseriesAck (server → client)
//! - `0x50` ResyncRequest (bidirectional)
//! - `0x52` Throttle (client → server)
//! - `0x60` TokenRefresh (client → server)
//! - `0x61` TokenRefreshAck (server → client)
//! - `0x70` DefinitionSync (server → client)
//! - `0x80` PresenceUpdate (client → server)
//! - `0x81` PresenceBroadcast (server → all subscribers)
//! - `0x82` PresenceLeave (server → all subscribers)
//! - `0x90` ArrayDelta (client → server)
//! - `0x91` ArrayDeltaBatch (client → server)
//! - `0x92` ArraySnapshot (server → client)
//! - `0x93` ArraySnapshotChunk (server → client)
//! - `0x94` ArraySchema (bidirectional)
//! - `0x95` ArrayAck (client → server)
//! - `0x96` ArrayReject (server → client)
//! - `0x97` ArrayCatchupRequest (client → server)
//! - `0xFF` Ping/Pong (bidirectional)

pub mod array;
pub mod delta;
pub mod frame;
pub mod presence;
pub mod resync;
pub mod session;
pub mod shape;
pub mod timeseries;

#[cfg(test)]
mod tests;

pub use array::{
    ArrayAckMsg, ArrayCatchupRequestMsg, ArrayDeltaBatchMsg, ArrayDeltaMsg, ArrayRejectMsg,
    ArrayRejectReason, ArraySchemaSyncMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};
pub use delta::{CollectionPurgedMsg, DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg};
pub use frame::{SyncFrame, SyncMessageType};
pub use presence::{PeerPresence, PresenceBroadcastMsg, PresenceLeaveMsg, PresenceUpdateMsg};
pub use resync::{ResyncReason, ResyncRequestMsg, ThrottleMsg};
pub use session::{
    HandshakeAckMsg, HandshakeMsg, PingPongMsg, TokenRefreshAckMsg, TokenRefreshMsg,
};
pub use shape::{
    ShapeDeltaMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, ShapeUnsubscribeMsg, VectorClockSyncMsg,
};
pub use timeseries::{DefinitionSyncMsg, TimeseriesAckMsg, TimeseriesPushMsg};
