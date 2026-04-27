//! Stable per-replica identity.
//!
//! Each array-engine replica (Lite instance or Origin shard) is assigned a
//! [`ReplicaId`] on first open and stored under `Namespace::Meta::"replica_id"`.
//! The id is derived from the low 64 bits of a UUID v7, giving wall-clock
//! ordering with sufficient entropy.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{ArrayError, ArrayResult};

/// Stable 64-bit replica identity.
///
/// Represented as the low 64 bits of a UUID v7. Total order matches `u64`
/// numeric order, so HLC tiebreaks are deterministic.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicaId(pub u64);

impl ReplicaId {
    /// Construct a [`ReplicaId`] from a known `u64` value (e.g. loaded from
    /// persistent storage).
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Generate a fresh, probabilistically unique [`ReplicaId`] using UUID v7.
    ///
    /// Takes the low 64 bits of the UUID's 128-bit representation. UUID v7
    /// encodes a millisecond timestamp in the high bits, so the low 64 bits
    /// carry a mix of sub-ms precision and random bits — unique enough for
    /// replica identity.
    pub fn generate() -> Self {
        Self(Uuid::now_v7().as_u128() as u64)
    }

    /// Return the underlying `u64`.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for ReplicaId {
    /// Format as a 16-character lowercase hex string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for ReplicaId {
    type Err = ArrayError;

    /// Parse a 16-character lowercase hex string back into a [`ReplicaId`].
    fn from_str(s: &str) -> ArrayResult<Self> {
        u64::from_str_radix(s, 16)
            .map(ReplicaId)
            .map_err(|_| ArrayError::InvalidReplicaId {
                detail: format!("invalid replica_id hex: {s}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_is_unique() {
        let a = ReplicaId::generate();
        let b = ReplicaId::generate();
        // Statistically guaranteed; would require a UUID collision to fail.
        assert_ne!(a, b);
    }

    #[test]
    fn roundtrip_hex() {
        let id = ReplicaId::new(0xdeadbeef_cafebabe);
        let s = id.to_string();
        assert_eq!(s, "deadbeefcafebabe");
        let parsed: ReplicaId = s.parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn parse_invalid_returns_err() {
        let result: ArrayResult<ReplicaId> = "not_hex_at_all!".parse();
        assert!(matches!(result, Err(ArrayError::InvalidReplicaId { .. })));
    }

    #[test]
    fn as_u64_round_trips() {
        let val = 0x0102030405060708_u64;
        let id = ReplicaId::new(val);
        assert_eq!(id.as_u64(), val);
    }

    #[test]
    fn serialize_roundtrip() {
        let id = ReplicaId::generate();
        let bytes = zerompk::to_msgpack_vec(&id).expect("serialize");
        let back: ReplicaId = zerompk::from_msgpack(&bytes).expect("deserialize");
        assert_eq!(id, back);
    }
}
