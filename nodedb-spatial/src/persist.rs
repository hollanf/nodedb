//! R-tree checkpoint/restore for durable persistence.
//!
//! Follows the same pattern as the vector engine's HNSW checkpoint:
//! serialize all entries to bytes (MessagePack) → write to storage →
//! restore via bulk_load on cold start.
//!
//! Storage key scheme (in redb under Namespace::Spatial):
//! - `{collection}\x00{field}\x00rtree` → serialized R-tree entries
//! - `{collection}\x00{field}\x00meta` → SpatialIndexMeta

use nodedb_types::BoundingBox;
use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::rtree::{RTree, RTreeEntry};

/// Metadata for a persisted spatial index.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct SpatialIndexMeta {
    /// Collection this index belongs to.
    pub collection: String,
    /// Geometry field name being indexed.
    pub field: String,
    /// Index type.
    pub index_type: SpatialIndexType,
    /// Number of entries at last checkpoint.
    pub entry_count: u64,
    /// Bounding box of all indexed geometries (spatial extent).
    pub extent: Option<BoundingBox>,
}

/// Type of spatial index.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[msgpack(c_enum)]
#[repr(u8)]
pub enum SpatialIndexType {
    RTree = 0,
    Geohash = 1,
}

impl SpatialIndexType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RTree => "rtree",
            Self::Geohash => "geohash",
        }
    }
}

impl std::fmt::Display for SpatialIndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Magic header for rkyv-serialized R-tree snapshots (6 bytes).
const RTREE_RKYV_MAGIC: &[u8; 6] = b"RKSPT\0";
/// Current format version for rkyv-serialized R-tree snapshots.
pub const RTREE_FORMAT_VERSION: u8 = 1;

/// Serialized R-tree snapshot (legacy MessagePack).
#[derive(Debug, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct RTreeSnapshot {
    pub entries: Vec<RTreeEntry>,
}

/// rkyv-serialized R-tree snapshot.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct RTreeSnapshotRkyv {
    entries: Vec<RTreeEntry>,
}

impl RTree {
    /// Serialize the R-tree to rkyv bytes (with magic header) for checkpointing.
    pub fn checkpoint_to_bytes(&self) -> Result<Vec<u8>, RTreeCheckpointError> {
        let snapshot = RTreeSnapshotRkyv {
            entries: self.entries().into_iter().cloned().collect(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .map_err(|e| RTreeCheckpointError::RkyvSerialize(e.to_string()))?;
        let mut buf = Vec::with_capacity(RTREE_RKYV_MAGIC.len() + 1 + rkyv_bytes.len());
        buf.extend_from_slice(RTREE_RKYV_MAGIC);
        buf.push(RTREE_FORMAT_VERSION);
        buf.extend_from_slice(&rkyv_bytes);
        Ok(buf)
    }

    /// Restore an R-tree from checkpoint bytes.
    ///
    /// Auto-detects format: rkyv (magic `RKSPT\0`) or legacy MessagePack.
    /// Uses bulk_load (STR packing) for optimal node packing.
    ///
    /// Returns `Err(RTreeCheckpointError::UnsupportedVersion)` when the magic
    /// matches but the version byte is not `RTREE_FORMAT_VERSION`. Buffers
    /// without the magic prefix fall through to legacy MessagePack decode.
    pub fn from_checkpoint(bytes: &[u8]) -> Result<Self, RTreeCheckpointError> {
        let header_len = RTREE_RKYV_MAGIC.len() + 1; // magic + version byte
        if bytes.len() > header_len && &bytes[..RTREE_RKYV_MAGIC.len()] == RTREE_RKYV_MAGIC {
            let version = bytes[RTREE_RKYV_MAGIC.len()];
            if version != RTREE_FORMAT_VERSION {
                return Err(RTreeCheckpointError::UnsupportedVersion {
                    found: version,
                    expected: RTREE_FORMAT_VERSION,
                });
            }
            let rkyv_bytes = &bytes[header_len..];
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(rkyv_bytes.len());
            aligned.extend_from_slice(rkyv_bytes);
            let snapshot: RTreeSnapshotRkyv =
                rkyv::from_bytes::<RTreeSnapshotRkyv, rkyv::rancor::Error>(&aligned)
                    .map_err(|e| RTreeCheckpointError::RkyvDeserialize(e.to_string()))?;
            return Ok(RTree::bulk_load(snapshot.entries));
        }
        // Legacy MessagePack fallback.
        let snapshot: RTreeSnapshot =
            zerompk::from_msgpack(bytes).map_err(RTreeCheckpointError::Deserialize)?;
        Ok(RTree::bulk_load(snapshot.entries))
    }
}

/// Build the storage key for an R-tree checkpoint.
///
/// Format: `{collection}\0{field}\0rtree`
pub fn rtree_storage_key(collection: &str, field: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + field.len() + 8);
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.extend_from_slice(b"rtree");
    key
}

/// Build the storage key for spatial index metadata.
///
/// Format: `{collection}\0{field}\0meta`
pub fn meta_storage_key(collection: &str, field: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + field.len() + 7);
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.extend_from_slice(b"meta");
    key
}

/// Serialize index metadata to bytes.
pub fn serialize_meta(meta: &SpatialIndexMeta) -> Result<Vec<u8>, RTreeCheckpointError> {
    zerompk::to_msgpack_vec(meta).map_err(RTreeCheckpointError::Serialize)
}

/// Deserialize index metadata from bytes.
pub fn deserialize_meta(bytes: &[u8]) -> Result<SpatialIndexMeta, RTreeCheckpointError> {
    zerompk::from_msgpack(bytes).map_err(RTreeCheckpointError::Deserialize)
}

/// Errors during R-tree checkpoint operations.
#[derive(Debug, thiserror::Error)]
pub enum RTreeCheckpointError {
    #[error("R-tree checkpoint serialization failed: {0}")]
    Serialize(zerompk::Error),
    #[error("R-tree checkpoint deserialization failed: {0}")]
    Deserialize(zerompk::Error),
    #[error("R-tree rkyv serialization failed: {0}")]
    RkyvSerialize(String),
    #[error("R-tree rkyv deserialization failed: {0}")]
    RkyvDeserialize(String),
    #[error("unsupported R-tree checkpoint version {found}; expected {expected}")]
    UnsupportedVersion { found: u8, expected: u8 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::from_point(lng, lat),
        }
    }

    #[test]
    fn checkpoint_roundtrip_empty() {
        let tree = RTree::new();
        let bytes = tree.checkpoint_to_bytes().unwrap();
        let restored = RTree::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 0);
    }

    #[test]
    fn checkpoint_roundtrip_entries() {
        let mut tree = RTree::new();
        for i in 0..100 {
            tree.insert(make_entry(i, (i as f64) * 0.5, (i as f64) * 0.3));
        }
        assert_eq!(tree.len(), 100);

        let bytes = tree.checkpoint_to_bytes().unwrap();
        let restored = RTree::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 100);

        // All entries should be searchable.
        let all = restored.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 100);
    }

    #[test]
    fn checkpoint_preserves_ids() {
        let mut tree = RTree::new();
        tree.insert(make_entry(42, 10.0, 20.0));
        tree.insert(make_entry(99, 30.0, 40.0));

        let bytes = tree.checkpoint_to_bytes().unwrap();
        let restored = RTree::from_checkpoint(&bytes).unwrap();

        let results = restored.search(&BoundingBox::new(5.0, 15.0, 15.0, 25.0));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 42);
    }

    #[test]
    fn corrupted_bytes_returns_error() {
        assert!(RTree::from_checkpoint(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn meta_roundtrip() {
        let meta = SpatialIndexMeta {
            collection: "buildings".to_string(),
            field: "geom".to_string(),
            index_type: SpatialIndexType::RTree,
            entry_count: 1000,
            extent: Some(BoundingBox::new(-180.0, -90.0, 180.0, 90.0)),
        };
        let bytes = serialize_meta(&meta).unwrap();
        let restored = deserialize_meta(&bytes).unwrap();
        assert_eq!(restored.collection, "buildings");
        assert_eq!(restored.entry_count, 1000);
        assert_eq!(restored.index_type, SpatialIndexType::RTree);
    }

    #[test]
    fn storage_key_format() {
        let key = rtree_storage_key("buildings", "geom");
        assert_eq!(key, b"buildings\0geom\0rtree");

        let meta_key = meta_storage_key("buildings", "geom");
        assert_eq!(meta_key, b"buildings\0geom\0meta");
    }

    #[test]
    fn checkpoint_size_reasonable() {
        let mut tree = RTree::new();
        for i in 0..1000 {
            tree.insert(make_entry(i, (i as f64) * 0.01, (i as f64) * 0.01));
        }
        let bytes = tree.checkpoint_to_bytes().unwrap();
        // Each entry: id(8) + 4 f64(32) = ~40 bytes + rkyv overhead.
        // 1000 entries ≈ 40-60KB is reasonable.
        assert!(
            bytes.len() < 100_000,
            "checkpoint too large: {} bytes",
            bytes.len()
        );
        assert!(
            bytes.len() > 10_000,
            "checkpoint too small: {} bytes",
            bytes.len()
        );
    }

    #[test]
    fn golden_header_layout() {
        let mut tree = RTree::new();
        tree.insert(make_entry(1, 10.0, 20.0));
        let bytes = tree.checkpoint_to_bytes().unwrap();
        // Magic at bytes[0..6].
        assert_eq!(&bytes[0..6], b"RKSPT\0");
        // Version byte at bytes[6].
        assert_eq!(bytes[6], super::RTREE_FORMAT_VERSION);
        // rkyv payload follows immediately.
        assert!(bytes.len() > 7);
    }

    #[test]
    fn version_mismatch_returns_error() {
        let mut tree = RTree::new();
        tree.insert(make_entry(1, 10.0, 20.0));
        let mut bytes = tree.checkpoint_to_bytes().unwrap();
        // Corrupt the version byte to an unsupported value.
        bytes[6] = 0;
        match RTree::from_checkpoint(&bytes) {
            Err(RTreeCheckpointError::UnsupportedVersion { found, expected }) => {
                assert_eq!(found, 0);
                assert_eq!(expected, super::RTREE_FORMAT_VERSION);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected UnsupportedVersion error, got Ok"),
        }
    }
}
