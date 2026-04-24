//! `EdgeValuePayload` — wraps property blob with bitemporal valid-time bounds.

use serde::{Deserialize, Serialize};

/// Bitemporal edge value payload. Wraps the caller's MessagePack property
/// blob with valid-time bounds.
///
/// `valid_until_ms` uses [`nodedb_types::OPEN_UPPER`] (`i64::MAX`) as the
/// open-upper sentinel.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EdgeValuePayload {
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub properties: Vec<u8>,
}

impl EdgeValuePayload {
    pub fn new(valid_from_ms: i64, valid_until_ms: i64, properties: Vec<u8>) -> Self {
        Self {
            valid_from_ms,
            valid_until_ms,
            properties,
        }
    }

    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("encode EdgeValuePayload: {e}"),
        })
    }

    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("decode EdgeValuePayload: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::keys::is_sentinel;
    use super::*;

    #[test]
    fn payload_msgpack_roundtrip() {
        let p = EdgeValuePayload::new(1_000, 2_000, b"props".to_vec());
        let bytes = p.encode().unwrap();
        // zerompk encodes derived structs as fixarray — 3 fields → 0x93.
        assert_eq!(bytes[0], 0x93);
        // Sentinels (0xFF, 0xFE) stay disjoint from fixarray range (0x90..=0x9f).
        assert!(!is_sentinel(&bytes[..1]));
        let decoded = EdgeValuePayload::decode(&bytes).unwrap();
        assert_eq!(decoded, p);
    }
}
