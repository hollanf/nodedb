//! Virtual shard identifier.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a virtual shard (0..1023). Data is hashed to vShards by shard key.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct VShardId(pub(super) u32);

impl VShardId {
    /// Total number of virtual shards in the system.
    pub const COUNT: u32 = 1024;

    pub const fn new(id: u32) -> Self {
        assert!(id < Self::COUNT, "vShard ID must be < 1024");
        Self(id)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Compute vShard from a collection name.
    ///
    /// Uses a simple DJB-like hash (multiply-31) for deterministic
    /// collection-to-shard routing.
    pub fn from_collection(collection: &str) -> Self {
        let hash = collection
            .as_bytes()
            .iter()
            .fold(0u32, |h, &b| h.wrapping_mul(31).wrapping_add(b as u32));
        Self::new(hash % Self::COUNT)
    }

    /// Compute vShard from a shard key via consistent hashing.
    pub fn from_key(key: &[u8]) -> Self {
        // FxHash-style fast hash, modulo 1024.
        let mut h: u64 = 0;
        for &b in key {
            h = h.wrapping_mul(0x100000001B3).wrapping_add(b as u64);
        }
        Self::new((h % Self::COUNT as u64) as u32)
    }
}

impl fmt::Display for VShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "vshard:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vshard_id_above_u16_max_roundtrip() {
        let v = VShardId(0);
        assert_eq!(v.as_u32(), 0u32);

        let v = VShardId(1023);
        assert_eq!(v.as_u32(), 1023u32);
    }

    #[test]
    fn vshard_new_above_old_u16_max_would_panic_but_inner_holds_u32() {
        let v = VShardId(0x0001_0000);
        assert_eq!(v.as_u32(), 0x0001_0000u32);
    }

    #[test]
    fn vshard_from_key_deterministic() {
        let a = VShardId::from_key(b"user:alice");
        let b = VShardId::from_key(b"user:alice");
        assert_eq!(a, b);
        assert!(a.as_u32() < VShardId::COUNT);
    }

    #[test]
    fn vshard_from_key_distributes() {
        let mut seen = std::collections::HashSet::new();
        for i in 0u32..1000 {
            let key = format!("tenant:{i}");
            seen.insert(VShardId::from_key(key.as_bytes()).as_u32());
        }
        assert!(
            seen.len() > 100,
            "poor distribution: only {} vShards hit",
            seen.len()
        );
    }
}
