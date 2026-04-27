//! Array catchup request wire message.

use serde::{Deserialize, Serialize};

/// Array catchup request message (client → server, 0x97).
///
/// Sent by a Lite peer to ask Origin for a snapshot covering all ops since
/// `from_hlc_bytes`. Use `Hlc::ZERO.to_bytes()` (all zeros) for "all history"
/// (e.g. first connect). Also sent after receiving
/// `ArrayRejectReason::ArrayUnknown` or when the Lite GC horizon is behind
/// Origin's retention floor.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayCatchupRequestMsg {
    /// Name of the array to catch up on.
    pub array: String,
    /// Start HLC (18-byte layout). Origin will send all ops with HLC ≥ this value.
    /// Use all-zero bytes (`[0u8; 18]`) to request full history from the beginning.
    pub from_hlc_bytes: [u8; 18],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArrayCatchupRequestMsg {
            array: "feed".to_string(),
            from_hlc_bytes: [0u8; 18],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArrayCatchupRequestMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }
}
