use std::fmt;

use serde::{Deserialize, Serialize};

// ── Re-export shared types from nodedb-types ──
pub use nodedb_types::id::{DocumentId, TenantId, VShardId};

/// Globally unique request identifier. Monotonic per connection, unique for >= 24h.
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
)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_display() {
        let t = TenantId::new(42);
        assert_eq!(t.to_string(), "tenant:42");
        assert_eq!(t.as_u64(), 42);
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
