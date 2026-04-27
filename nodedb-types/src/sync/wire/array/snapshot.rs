//! Array snapshot header wire message.

use serde::{Deserialize, Serialize};

/// Array snapshot header message (server → client, 0x92).
///
/// Sent before the chunk stream to announce an incoming snapshot transfer.
/// `header_payload` is a zerompk-encoded `nodedb_array::sync::SnapshotHeader`.
/// The receiving engine decodes it via `nodedb-array::sync::op_codec`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArraySnapshotMsg {
    /// Name of the array being snapshotted.
    pub array: String,
    /// Zerompk-encoded `nodedb_array::sync::SnapshotHeader`.
    pub header_payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArraySnapshotMsg {
            array: "timeline".to_string(),
            header_payload: vec![0x10, 0x20, 0x30, 0x40],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArraySnapshotMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }
}
