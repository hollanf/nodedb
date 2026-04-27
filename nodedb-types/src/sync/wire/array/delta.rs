//! Array CRDT delta wire message (single op).

use serde::{Deserialize, Serialize};

/// Array CRDT delta message (client → server, 0x90).
///
/// Carries a single zerompk-encoded `ArrayOp` in `op_payload`.
/// Routing peers can dispatch by `array` without decoding the payload.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayDeltaMsg {
    /// Name of the target array.
    pub array: String,
    /// A single zerompk-encoded `ArrayOp`. Decoded by `nodedb-array::sync::op_codec`.
    pub op_payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArrayDeltaMsg {
            array: "my_array".to_string(),
            op_payload: vec![0x01, 0x02, 0x03],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArrayDeltaMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }
}
