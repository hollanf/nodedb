//! Array CRDT rejection wire message and reason codes.

use serde::{Deserialize, Serialize};

/// Reason codes for array op rejection (Origin → Lite, 0x96).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[non_exhaustive]
pub enum ArrayRejectReason {
    /// The op was encoded with a schema version newer than Origin knows about.
    SchemaTooNew = 1,
    /// The target array does not exist on Origin.
    ArrayUnknown = 2,
    /// The op payload shape is invalid or failed deserialization.
    ShapeInvalid = 3,
    /// The engine itself rejected the op (constraint violation, etc.).
    EngineRejected = 4,
    /// The op's HLC falls below Origin's GC retention floor.
    RetentionFloor = 5,
    /// The replica has exceeded its storage or rate quota.
    QuotaExceeded = 6,
    /// Wire protocol version mismatch between Lite and Origin.
    WireVersionMismatch = 7,
}

impl ArrayRejectReason {
    /// Convert a raw `u8` discriminant to `ArrayRejectReason`.
    ///
    /// Returns `None` for unrecognized values.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::SchemaTooNew),
            2 => Some(Self::ArrayUnknown),
            3 => Some(Self::ShapeInvalid),
            4 => Some(Self::EngineRejected),
            5 => Some(Self::RetentionFloor),
            6 => Some(Self::QuotaExceeded),
            7 => Some(Self::WireVersionMismatch),
            _ => None,
        }
    }
}

/// Array reject message (server → client, 0x96).
///
/// Sent by Origin as a compensation hint when it rejects a Lite op.
/// The originating Lite uses `op_hlc_bytes` to locate the offending op
/// in its local log and roll it back.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayRejectMsg {
    /// Name of the array the rejected op targeted.
    pub array: String,
    /// HLC of the rejected op (18-byte layout matching
    /// `nodedb_array::sync::Hlc::to_bytes()`). Used by the Lite peer
    /// to identify and roll back the offending local op.
    pub op_hlc_bytes: [u8; 18],
    /// Structured reason for the rejection.
    pub reason: ArrayRejectReason,
    /// Human-readable detail string for debugging or user-facing messages.
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArrayRejectMsg {
            array: "leaderboard".to_string(),
            op_hlc_bytes: [0x11u8; 18],
            reason: ArrayRejectReason::EngineRejected,
            detail: "unique constraint violated".to_string(),
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArrayRejectMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }

    #[test]
    fn reject_reason_round_trips() {
        let variants = [
            ArrayRejectReason::SchemaTooNew,
            ArrayRejectReason::ArrayUnknown,
            ArrayRejectReason::ShapeInvalid,
            ArrayRejectReason::EngineRejected,
            ArrayRejectReason::RetentionFloor,
            ArrayRejectReason::QuotaExceeded,
            ArrayRejectReason::WireVersionMismatch,
        ];
        for variant in variants {
            let disc = variant as u8;
            let recovered = ArrayRejectReason::from_u8(disc);
            assert_eq!(recovered, Some(variant), "failed for discriminant {disc}");
        }
    }

    #[test]
    fn reject_reason_unknown_returns_none() {
        assert_eq!(ArrayRejectReason::from_u8(0), None);
        assert_eq!(ArrayRejectReason::from_u8(8), None);
        assert_eq!(ArrayRejectReason::from_u8(255), None);
    }
}
