//! Wire frame format and message-type discriminants.

/// Sync message type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum SyncMessageType {
    Handshake = 0x01,
    HandshakeAck = 0x02,
    DeltaPush = 0x10,
    DeltaAck = 0x11,
    DeltaReject = 0x12,
    /// Collection purged notification (server → client, 0x14).
    /// Sent when an Origin collection is hard-deleted (UNDROP window
    /// expired or explicit `DROP COLLECTION ... PURGE`). The client
    /// must drop local Loro state and remove the collection's redb
    /// record; future deltas for the collection are not sync-eligible.
    CollectionPurged = 0x14,
    ShapeSubscribe = 0x20,
    ShapeSnapshot = 0x21,
    ShapeDelta = 0x22,
    ShapeUnsubscribe = 0x23,
    VectorClockSync = 0x30,
    /// Timeseries metric batch push (client → server, 0x40).
    TimeseriesPush = 0x40,
    /// Timeseries push acknowledgment (server → client, 0x41).
    TimeseriesAck = 0x41,
    /// Re-sync request (bidirectional, 0x50).
    /// Sent when sequence gaps or checksum failures are detected.
    ResyncRequest = 0x50,
    /// Downstream throttle (client → server, 0x52).
    /// Sent when Lite's incoming queue is overwhelmed.
    Throttle = 0x52,
    /// Token refresh request (client → server, 0x60).
    TokenRefresh = 0x60,
    /// Token refresh acknowledgment (server → client, 0x61).
    TokenRefreshAck = 0x61,
    /// Definition sync (server → client, 0x70).
    /// Carries function/trigger/procedure definitions from Origin to Lite.
    DefinitionSync = 0x70,
    /// Presence update (client → server, 0x80).
    PresenceUpdate = 0x80,
    /// Presence broadcast (server → all subscribers except sender, 0x81).
    PresenceBroadcast = 0x81,
    /// Presence leave (server → all subscribers, 0x82).
    PresenceLeave = 0x82,
    /// Array CRDT delta (single op, client → server, 0x90).
    ArrayDelta = 0x90,
    /// Array CRDT delta batch (multiple ops, client → server, 0x91).
    ArrayDeltaBatch = 0x91,
    /// Array snapshot header (server → client, 0x92).
    ArraySnapshot = 0x92,
    /// Array snapshot chunk (server → client, 0x93).
    ArraySnapshotChunk = 0x93,
    /// Array schema CRDT sync (bidirectional, 0x94).
    ArraySchema = 0x94,
    /// Array ack — advances GC frontier (client → server, 0x95).
    ArrayAck = 0x95,
    /// Array reject (server → client, 0x96). Compensation hint.
    ArrayReject = 0x96,
    /// Array catchup request (client → server, 0x97).
    ArrayCatchupRequest = 0x97,
    PingPong = 0xFF,
}

impl SyncMessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Handshake),
            0x02 => Some(Self::HandshakeAck),
            0x10 => Some(Self::DeltaPush),
            0x11 => Some(Self::DeltaAck),
            0x12 => Some(Self::DeltaReject),
            0x14 => Some(Self::CollectionPurged),
            0x20 => Some(Self::ShapeSubscribe),
            0x21 => Some(Self::ShapeSnapshot),
            0x22 => Some(Self::ShapeDelta),
            0x23 => Some(Self::ShapeUnsubscribe),
            0x30 => Some(Self::VectorClockSync),
            0x40 => Some(Self::TimeseriesPush),
            0x41 => Some(Self::TimeseriesAck),
            0x50 => Some(Self::ResyncRequest),
            0x52 => Some(Self::Throttle),
            0x60 => Some(Self::TokenRefresh),
            0x61 => Some(Self::TokenRefreshAck),
            0x70 => Some(Self::DefinitionSync),
            0x80 => Some(Self::PresenceUpdate),
            0x81 => Some(Self::PresenceBroadcast),
            0x82 => Some(Self::PresenceLeave),
            0x90 => Some(Self::ArrayDelta),
            0x91 => Some(Self::ArrayDeltaBatch),
            0x92 => Some(Self::ArraySnapshot),
            0x93 => Some(Self::ArraySnapshotChunk),
            0x94 => Some(Self::ArraySchema),
            0x95 => Some(Self::ArrayAck),
            0x96 => Some(Self::ArrayReject),
            0x97 => Some(Self::ArrayCatchupRequest),
            0xFF => Some(Self::PingPong),
            _ => None,
        }
    }
}

/// Wire frame: wraps a message type + serialized body.
///
/// Layout: `[msg_type: 1B][length: 4B LE][body: N bytes]`
/// Total header: 5 bytes.
///
/// `#[non_exhaustive]` — additional header fields (e.g. compression flag,
/// session token) may be added without breaking downstream consumers.
#[non_exhaustive]
#[derive(Clone)]
pub struct SyncFrame {
    pub msg_type: SyncMessageType,
    pub body: Vec<u8>,
}

impl SyncFrame {
    pub const HEADER_SIZE: usize = 5;

    /// Serialize a frame to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let len = self.body.len() as u32;
        let mut buf = Vec::with_capacity(Self::HEADER_SIZE + self.body.len());
        buf.push(self.msg_type as u8);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&self.body);
        buf
    }

    /// Deserialize a frame from bytes.
    ///
    /// Returns `None` if the data is too short or the message type is unknown.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::HEADER_SIZE {
            return None;
        }
        let msg_type = SyncMessageType::from_u8(data[0])?;
        let len = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
        if data.len() < Self::HEADER_SIZE + len {
            return None;
        }
        let body = data[Self::HEADER_SIZE..Self::HEADER_SIZE + len].to_vec();
        Some(Self { msg_type, body })
    }

    /// Create a frame with a MessagePack-serialized body.
    pub fn new_msgpack<T: zerompk::ToMessagePack>(
        msg_type: SyncMessageType,
        value: &T,
    ) -> Option<Self> {
        let body = zerompk::to_msgpack_vec(value).ok()?;
        Some(Self { msg_type, body })
    }

    /// Try to encode a value into a SyncFrame body.
    ///
    /// Returns `None` and logs an error on serialization failure — callers
    /// should propagate via `?`. The protocol must never ship a frame
    /// whose body did not serialize successfully.
    pub fn try_encode<T: zerompk::ToMessagePack>(
        msg_type: SyncMessageType,
        value: &T,
    ) -> Option<Self> {
        match zerompk::to_msgpack_vec(value) {
            Ok(body) => Some(Self { msg_type, body }),
            Err(e) => {
                tracing::error!(
                    msg_type = msg_type as u8,
                    error = %e,
                    "failed to encode sync frame body; dropping response"
                );
                None
            }
        }
    }

    /// Deserialize the body from MessagePack.
    pub fn decode_body<T: zerompk::FromMessagePackOwned>(&self) -> Option<T> {
        zerompk::from_msgpack(&self.body).ok()
    }
}
