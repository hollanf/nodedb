//! Array CRDT delta batch wire message (multiple ops in one frame).

use serde::{Deserialize, Serialize};

/// Array CRDT delta batch message (client → server, 0x91).
///
/// Bulk frame for high-throughput periods. All ops in a batch must target
/// the same `array`. Each entry in `op_payloads` is a zerompk-encoded
/// `ArrayOp`; the engine decodes them via `nodedb-array::sync::op_codec`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayDeltaBatchMsg {
    /// Name of the target array. All ops apply to this array.
    pub array: String,
    /// Ordered list of zerompk-encoded `ArrayOp` payloads.
    pub op_payloads: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArrayDeltaBatchMsg {
            array: "scores".to_string(),
            op_payloads: vec![vec![0xAA, 0xBB], vec![0xCC, 0xDD]],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArrayDeltaBatchMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }

    #[test]
    fn empty_batch_roundtrips() {
        let msg = ArrayDeltaBatchMsg {
            array: "empty_array".to_string(),
            op_payloads: vec![],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArrayDeltaBatchMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
        assert!(decoded.op_payloads.is_empty());
    }
}
