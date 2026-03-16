use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a tenant. All data is tenant-scoped by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(u32);

impl TenantId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tenant:{}", self.0)
    }
}

/// Identifies a virtual shard (0..1023). Data is hashed to vShards by shard key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VShardId(u16);

impl VShardId {
    /// Total number of virtual shards in the system.
    pub const COUNT: u16 = 1024;

    pub const fn new(id: u16) -> Self {
        assert!(id < Self::COUNT, "vShard ID must be < 1024");
        Self(id)
    }

    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Compute vShard from a shard key via consistent hashing.
    pub fn from_key(key: &[u8]) -> Self {
        // FxHash-style fast hash, modulo 1024.
        let mut h: u64 = 0;
        for &b in key {
            h = h.wrapping_mul(0x100000001B3).wrapping_add(b as u64);
        }
        Self((h % Self::COUNT as u64) as u16)
    }
}

impl fmt::Display for VShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "vshard:{}", self.0)
    }
}

/// Globally unique request identifier. Monotonic per connection, unique for >= 24h.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(u64);

impl RequestId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "req:{}", self.0)
    }
}

/// Identifies a document/row across all engines.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(String);

impl DocumentId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_display() {
        let t = TenantId::new(42);
        assert_eq!(t.to_string(), "tenant:42");
        assert_eq!(t.as_u32(), 42);
    }

    #[test]
    fn vshard_from_key_deterministic() {
        let a = VShardId::from_key(b"user:alice");
        let b = VShardId::from_key(b"user:alice");
        assert_eq!(a, b);
        assert!(a.as_u16() < VShardId::COUNT);
    }

    #[test]
    fn vshard_from_key_distributes() {
        let mut seen = std::collections::HashSet::new();
        for i in 0u32..1000 {
            let key = format!("tenant:{i}");
            seen.insert(VShardId::from_key(key.as_bytes()).as_u16());
        }
        // 1000 keys should hit at least 100 distinct vShards.
        assert!(
            seen.len() > 100,
            "poor distribution: only {} vShards hit",
            seen.len()
        );
    }

    #[test]
    fn request_id_roundtrip() {
        let r = RequestId::new(123456789);
        assert_eq!(r.as_u64(), 123456789);
        assert_eq!(r.to_string(), "req:123456789");
    }

    #[test]
    fn document_id_str() {
        let d = DocumentId::new("doc-abc-123");
        assert_eq!(d.as_str(), "doc-abc-123");
    }
}
