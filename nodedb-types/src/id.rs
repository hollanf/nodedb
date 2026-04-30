//! Core identifier types used across NodeDB Origin and Lite.
//!
//! Strong typing prevents mixing up raw integers/strings. All IDs are
//! `serde` + `rkyv` serializable and safe for WASM targets.

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
pub struct VShardId(u32);

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

/// Identifies a tenant. All data is tenant-scoped by construction.
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
pub struct TenantId(u64);

impl TenantId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for TenantId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tenant:{}", self.0)
    }
}

/// Identifies a collection (table/namespace).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct CollectionId(String);

impl CollectionId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a document/row across all engines.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
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
        f.write_str(&self.0)
    }
}

/// Identifies a graph node. Separate from `DocumentId` because graph nodes
/// can exist independently of documents (e.g., concept nodes in a knowledge graph).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a graph edge. Returned by `graph_insert_edge`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct EdgeId(String);

impl EdgeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Generate an edge ID from source, target, and label.
    pub fn from_components(src: &str, dst: &str, label: &str) -> Self {
        Self(format!("{src}--{label}-->{dst}"))
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a shape subscription (globally unique per Origin).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ShapeId(String);

impl ShapeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ShapeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for ShapeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ShapeId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
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
    fn tenant_id_above_u32_max_roundtrip() {
        let large = u32::MAX as u64 + 1;
        let t = TenantId::new(large);
        assert_eq!(t.as_u64(), large);
        assert_eq!(t.to_string(), format!("tenant:{large}"));
    }

    #[test]
    fn tenant_id_from_u64() {
        let t: TenantId = TenantId::from(4_294_967_296u64);
        assert_eq!(t.as_u64(), 4_294_967_296u64);
    }

    #[test]
    fn vshard_id_above_u16_max_roundtrip() {
        // VShardId::new panics if id >= COUNT (1024), so we test the widened
        // inner type by creating a value just above old u16 max indirectly via
        // direct construction — use a valid id and verify as_u32 preserves it.
        // The key new test: id 0x0001_0000 is > u16::MAX; since COUNT=1024 this
        // value would panic. Instead test at the exact boundary of the new type.
        let v = VShardId(0); // inner field test via as_u32
        assert_eq!(v.as_u32(), 0u32);

        // Large u32 IDs below COUNT should round-trip correctly.
        let v = VShardId(1023);
        assert_eq!(v.as_u32(), 1023u32);
    }

    #[test]
    fn vshard_new_above_old_u16_max_would_panic_but_inner_holds_u32() {
        // Demonstrate that the inner type is u32: construct directly (bypassing
        // the assert) and confirm as_u32() returns the full u32 value.
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

    #[test]
    fn collection_id() {
        let c = CollectionId::new("embeddings");
        assert_eq!(c.as_str(), "embeddings");
        assert_eq!(c.to_string(), "embeddings");
    }

    #[test]
    fn document_id_str() {
        let d = DocumentId::new("doc-abc-123");
        assert_eq!(d.as_str(), "doc-abc-123");
    }

    #[test]
    fn node_id() {
        let n = NodeId::new("concept:rust");
        assert_eq!(n.as_str(), "concept:rust");
    }

    #[test]
    fn edge_id_from_components() {
        let e = EdgeId::from_components("alice", "bob", "KNOWS");
        assert_eq!(e.as_str(), "alice--KNOWS-->bob");
    }

    #[test]
    fn shape_id_from_str() {
        let s = ShapeId::from("shape-001");
        assert_eq!(s.as_str(), "shape-001");
    }

    #[test]
    fn serde_roundtrip() {
        let tid = TenantId::new(7);
        let json = sonic_rs::to_string(&tid).unwrap();
        let decoded: TenantId = sonic_rs::from_str(&json).unwrap();
        assert_eq!(tid, decoded);
    }

    #[test]
    fn serde_roundtrip_above_u32_max() {
        let tid = TenantId::new(u32::MAX as u64 + 1);
        let json = sonic_rs::to_string(&tid).unwrap();
        let decoded: TenantId = sonic_rs::from_str(&json).unwrap();
        assert_eq!(tid, decoded);
    }
}
