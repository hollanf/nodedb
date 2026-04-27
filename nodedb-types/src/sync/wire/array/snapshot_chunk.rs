//! Array snapshot chunk wire message.

use serde::{Deserialize, Serialize};

/// Array snapshot chunk message (server → client, 0x93).
///
/// Carries one chunk of a multi-part snapshot transfer.
/// `snapshot_hlc_bytes` uses the same 18-byte layout as
/// `nodedb_array::sync::Hlc::to_bytes()` and associates this chunk with
/// the header sent in `ArraySnapshotMsg`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArraySnapshotChunkMsg {
    /// Name of the array this chunk belongs to.
    pub array: String,
    /// 18-byte HLC timestamp matching `nodedb_array::sync::Hlc::to_bytes()` layout.
    /// Used to associate this chunk with the corresponding `ArraySnapshotMsg` header.
    pub snapshot_hlc_bytes: [u8; 18],
    /// Zero-based index of this chunk within the snapshot.
    pub chunk_index: u32,
    /// Total number of chunks in this snapshot transfer.
    pub total_chunks: u32,
    /// Raw chunk payload bytes.
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArraySnapshotChunkMsg {
            array: "events".to_string(),
            snapshot_hlc_bytes: [1u8; 18],
            chunk_index: 0,
            total_chunks: 5,
            payload: vec![0xFF, 0xEE, 0xDD],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArraySnapshotChunkMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }

    #[test]
    fn roundtrip_preserves_hlc_bytes() {
        let mut hlc = [0u8; 18];
        hlc[0] = 0xDE;
        hlc[8] = 0xAD;
        hlc[17] = 0xBE;
        let msg = ArraySnapshotChunkMsg {
            array: "chunk_test".to_string(),
            snapshot_hlc_bytes: hlc,
            chunk_index: 3,
            total_chunks: 10,
            payload: vec![],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArraySnapshotChunkMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(decoded.snapshot_hlc_bytes, hlc);
        assert_eq!(decoded.chunk_index, 3);
        assert_eq!(decoded.total_chunks, 10);
    }
}
