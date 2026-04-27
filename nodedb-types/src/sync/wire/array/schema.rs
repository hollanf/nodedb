//! Array schema CRDT sync wire message.

use serde::{Deserialize, Serialize};

/// Array schema CRDT sync message (bidirectional, 0x94).
///
/// Carries the Loro snapshot of an array's schema document, exported via
/// `SchemaDoc::export_snapshot()`. `replica_id` identifies the originator
/// so the receiver can apply the snapshot to the correct peer state.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArraySchemaSyncMsg {
    /// Name of the array whose schema is being synced.
    pub array: String,
    /// Originating replica's numeric ID.
    pub replica_id: u64,
    /// 18-byte HLC timestamp of this schema version.
    /// Same layout as `nodedb_array::sync::Hlc::to_bytes()`.
    pub schema_hlc_bytes: [u8; 18],
    /// Loro snapshot bytes exported via `SchemaDoc::export_snapshot()`.
    pub snapshot_payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_msgpack() {
        let msg = ArraySchemaSyncMsg {
            array: "products".to_string(),
            replica_id: 42,
            schema_hlc_bytes: [7u8; 18],
            snapshot_payload: vec![0x01, 0x02, 0x03, 0x04, 0x05],
        };
        let encoded = zerompk::to_msgpack_vec(&msg).expect("encode");
        let decoded: ArraySchemaSyncMsg = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(msg, decoded);
    }
}
