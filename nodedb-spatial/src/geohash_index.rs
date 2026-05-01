//! Geohash index column for Point geometry fields.
//!
//! Automatically computes and stores a geohash string when a Point geometry
//! is indexed. Enables fast approximate proximity queries via string prefix
//! matching on B-tree indexes — no R-tree needed for simple "nearby" queries.
//!
//! When a query asks "find all points within N km", the geohash index
//! computes the query point's geohash + its 8 neighbors, then does prefix
//! range scans on the B-tree index. This is O(1) per cell vs O(log N) for
//! R-tree, and works on any storage backend that supports ordered key scans.

use std::collections::{BTreeMap, HashMap};

use nodedb_types::geometry::Geometry;
use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::geohash::{geohash_encode, geohash_neighbors};

/// Default geohash precision (6 = ~1.2 km cells).
pub const DEFAULT_PRECISION: u8 = 6;

/// Per-collection geohash index.
///
/// Maintains a mapping of document ID → geohash string for all Point
/// geometries in a collection. Non-Point geometries are silently skipped
/// (geohash only works for points in v1).
pub struct GeohashIndex {
    /// Collection name.
    pub collection: String,
    /// Geometry field name.
    pub field: String,
    /// Geohash precision (1-12).
    pub precision: u8,
    /// doc_id → geohash string.
    entries: HashMap<String, String>,
    /// Reverse index: geohash_prefix → set of doc_ids (for range queries).
    prefix_index: BTreeMap<String, Vec<String>>,
}

impl GeohashIndex {
    pub fn new(collection: &str, field: &str, precision: u8) -> Self {
        Self {
            collection: collection.to_string(),
            field: field.to_string(),
            precision: precision.clamp(1, 12),
            entries: HashMap::new(),
            prefix_index: BTreeMap::new(),
        }
    }

    /// Index a geometry. Only Point geometries are indexed; others are skipped.
    /// Returns the geohash string if indexed, None if skipped.
    pub fn index_document(&mut self, doc_id: &str, geometry: &Geometry) -> Option<String> {
        let Geometry::Point { coordinates } = geometry else {
            return None;
        };
        let lng = coordinates[0];
        let lat = coordinates[1];

        // Remove old entry if exists.
        self.remove_document(doc_id);

        let hash = geohash_encode(lng, lat, self.precision);
        self.entries.insert(doc_id.to_string(), hash.clone());
        self.prefix_index
            .entry(hash.clone())
            .or_default()
            .push(doc_id.to_string());
        Some(hash)
    }

    /// Remove a document from the geohash index.
    pub fn remove_document(&mut self, doc_id: &str) {
        if let Some(old_hash) = self.entries.remove(doc_id)
            && let Some(ids) = self.prefix_index.get_mut(&old_hash)
        {
            ids.retain(|id| id != doc_id);
            if ids.is_empty() {
                self.prefix_index.remove(&old_hash);
            }
        }
    }

    /// Find all document IDs whose geohash matches the given prefix.
    ///
    /// This is the core operation: a geohash prefix of length N matches all
    /// points within a cell of size determined by N. Shorter prefix = larger
    /// area.
    pub fn prefix_search(&self, prefix: &str) -> Vec<&str> {
        let mut results = Vec::new();
        // BTreeMap range scan: O(log N + K) where K = matching entries.
        let end = prefix_successor(prefix);
        let range = match &end {
            Some(e) => self.prefix_index.range(prefix.to_string()..e.clone()),
            None => self.prefix_index.range(prefix.to_string()..),
        };
        for (_hash, doc_ids) in range {
            for id in doc_ids {
                results.push(id.as_str());
            }
        }
        results
    }

    /// Find all document IDs near a point, using geohash prefix matching.
    ///
    /// Computes the geohash of the query point and its 8 neighbors, then
    /// collects all documents in those cells. This gives approximate results
    /// — the caller should apply exact distance filtering afterward.
    ///
    /// `search_precision` controls the cell size: lower = larger area, more
    /// results but less precise. Default: same as index precision.
    pub fn nearby_search(&self, lng: f64, lat: f64, search_precision: Option<u8>) -> Vec<&str> {
        let precision = search_precision.unwrap_or(self.precision);
        let center_hash = geohash_encode(lng, lat, precision);

        let mut results = Vec::new();

        // Search center cell.
        results.extend(self.prefix_search(&center_hash));

        // Search 8 neighbor cells.
        let neighbors = geohash_neighbors(&center_hash);
        for (_, neighbor_hash) in &neighbors {
            results.extend(self.prefix_search(neighbor_hash));
        }

        // Deduplicate.
        results.sort_unstable();
        results.dedup();
        results
    }

    /// Get the geohash for a specific document.
    pub fn get_geohash(&self, doc_id: &str) -> Option<&str> {
        self.entries.get(doc_id).map(|s| s.as_str())
    }

    /// Number of indexed documents.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Checkpoint: serialize all entries for persistence.
    ///
    /// When `kek` is `Some`, the msgpack payload is wrapped in an AES-256-GCM
    /// encrypted SEGV envelope. When `None`, raw msgpack bytes are returned.
    pub fn checkpoint_to_bytes(
        &self,
        #[cfg(feature = "encryption")] kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
        #[cfg(not(feature = "encryption"))] _kek: Option<&[u8; 32]>,
    ) -> Result<Vec<u8>, crate::persist::RTreeCheckpointError> {
        let snap = GeohashSnapshot {
            collection: self.collection.clone(),
            field: self.field.clone(),
            precision: self.precision,
            entries: self.entries.clone(),
        };
        let msgpack = zerompk::to_msgpack_vec(&snap)
            .map_err(crate::persist::RTreeCheckpointError::Serialize)?;

        #[cfg(feature = "encryption")]
        if let Some(key) = kek {
            return crate::persist::encrypt_geohash_payload(key, &msgpack);
        }

        Ok(msgpack)
    }

    /// Restore from checkpoint.
    ///
    /// `kek` controls the expected framing:
    /// - `None` → file must be plaintext msgpack. If encrypted (`SEGV`), returns
    ///   `Err(MissingKek)`.
    /// - `Some(key)` → encryption is **required**. Plaintext returns `Err(KekRequired)`.
    pub fn from_checkpoint(
        bytes: &[u8],
        #[cfg(feature = "encryption")] kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
        #[cfg(not(feature = "encryption"))] _kek: Option<&[u8; 32]>,
    ) -> Result<Self, crate::persist::RTreeCheckpointError> {
        use crate::persist::RTreeCheckpointError;

        let is_encrypted = bytes.len() >= 4 && bytes[0..4] == *b"SEGV";

        let msgpack: Vec<u8>;
        let msgpack_ref: &[u8];

        #[cfg(feature = "encryption")]
        {
            if is_encrypted {
                if let Some(key) = kek {
                    msgpack = crate::persist::decrypt_geohash_payload(key, bytes)?;
                    msgpack_ref = &msgpack;
                } else {
                    return Err(RTreeCheckpointError::MissingKek);
                }
            } else if kek.is_some() {
                return Err(RTreeCheckpointError::KekRequired);
            } else {
                msgpack_ref = bytes;
            }
        }

        #[cfg(not(feature = "encryption"))]
        {
            if is_encrypted {
                return Err(RTreeCheckpointError::MissingKek);
            }
            msgpack_ref = bytes;
        }

        let snap: GeohashSnapshot =
            zerompk::from_msgpack(msgpack_ref).map_err(RTreeCheckpointError::Deserialize)?;
        let mut index = Self::new(&snap.collection, &snap.field, snap.precision);
        // Rebuild prefix_index from entries.
        for (doc_id, hash) in &snap.entries {
            index.entries.insert(doc_id.clone(), hash.clone());
            index
                .prefix_index
                .entry(hash.clone())
                .or_default()
                .push(doc_id.clone());
        }
        Ok(index)
    }
}

/// Compute the lexicographic successor of a prefix for BTreeMap range queries.
/// "abc" → "abd". Returns None if the prefix is all 0xFF bytes.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack)]
struct GeohashSnapshot {
    collection: String,
    field: String,
    precision: u8,
    entries: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_point() {
        let mut idx = GeohashIndex::new("places", "location", 6);
        let hash = idx
            .index_document("doc1", &Geometry::point(-73.9857, 40.758))
            .unwrap();
        assert_eq!(hash.len(), 6);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn skip_non_point() {
        let mut idx = GeohashIndex::new("areas", "boundary", 6);
        let poly = Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]);
        assert!(idx.index_document("doc1", &poly).is_none());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn prefix_search_finds_nearby() {
        let mut idx = GeohashIndex::new("places", "loc", 6);
        // Two nearby points in NYC — should share a geohash prefix.
        idx.index_document("a", &Geometry::point(-73.985, 40.758));
        idx.index_document("b", &Geometry::point(-73.986, 40.759));
        // A far point.
        idx.index_document("c", &Geometry::point(139.69, 35.69));

        let hash_a = idx.get_geohash("a").unwrap();
        // Search with the first 4 chars of a's geohash.
        let prefix = &hash_a[..4];
        let results = idx.prefix_search(prefix);
        assert!(results.contains(&"a"));
        assert!(results.contains(&"b"));
        assert!(!results.contains(&"c"));
    }

    #[test]
    fn nearby_search_includes_neighbors() {
        let mut idx = GeohashIndex::new("places", "loc", 6);
        // Points along the boundary of two geohash cells.
        for i in 0..20 {
            let lng = -73.98 + (i as f64) * 0.001;
            idx.index_document(&format!("d{i}"), &Geometry::point(lng, 40.758));
        }

        let results = idx.nearby_search(-73.98, 40.758, None);
        assert!(!results.is_empty());
    }

    #[test]
    fn upsert_removes_old() {
        let mut idx = GeohashIndex::new("places", "loc", 6);
        idx.index_document("doc1", &Geometry::point(0.0, 0.0));
        idx.index_document("doc1", &Geometry::point(50.0, 50.0));
        assert_eq!(idx.len(), 1);

        // Old geohash should not find it.
        let old_hash = geohash_encode(0.0, 0.0, 6);
        let old_results = idx.prefix_search(&old_hash);
        assert!(!old_results.contains(&"doc1"));
    }

    #[test]
    fn remove_document() {
        let mut idx = GeohashIndex::new("places", "loc", 6);
        idx.index_document("doc1", &Geometry::point(10.0, 20.0));
        idx.remove_document("doc1");
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut idx = GeohashIndex::new("places", "loc", 6);
        for i in 0..50 {
            idx.index_document(&format!("d{i}"), &Geometry::point(i as f64, i as f64));
        }

        let bytes = idx.checkpoint_to_bytes(None).unwrap();
        let restored = GeohashIndex::from_checkpoint(&bytes, None).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.collection, "places");
        assert_eq!(restored.precision, 6);

        // Verify search still works.
        let hash = restored.get_geohash("d25").unwrap();
        assert_eq!(hash.len(), 6);
    }

    #[cfg(feature = "encryption")]
    fn make_test_kek() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x77u8; 32]).unwrap()
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn spatial_geohash_checkpoint_encrypted_at_rest() {
        let kek = make_test_kek();
        let mut idx = GeohashIndex::new("places", "loc", 6);
        for i in 0..30 {
            idx.index_document(&format!("d{i}"), &Geometry::point(i as f64, i as f64 * 0.5));
        }

        let enc_bytes = idx.checkpoint_to_bytes(Some(&kek)).unwrap();

        // Encrypted blob starts with SEGV, not raw msgpack.
        assert_eq!(&enc_bytes[0..4], b"SEGV");

        // Round-trip: decrypt and verify all entries survive.
        let restored = GeohashIndex::from_checkpoint(&enc_bytes, Some(&kek)).unwrap();
        assert_eq!(restored.len(), 30);
        assert_eq!(restored.collection, "places");
        assert_eq!(restored.precision, 6);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn spatial_geohash_refuses_plaintext_when_kek_required() {
        let kek = make_test_kek();
        let mut idx = GeohashIndex::new("places", "loc", 6);
        idx.index_document("doc1", &Geometry::point(10.0, 20.0));

        let plain_bytes = idx.checkpoint_to_bytes(None).unwrap();

        assert!(matches!(
            GeohashIndex::from_checkpoint(&plain_bytes, Some(&kek)),
            Err(crate::persist::RTreeCheckpointError::KekRequired)
        ));
    }
}
