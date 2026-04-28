//! HNSW checkpoint serialization and deserialization.
//!
//! Supports rkyv (current format) and legacy MessagePack for backward compat.

use std::cell::RefCell;

use crate::distance::DistanceMetric;
use crate::hnsw::arena::BeamSearchArena;
use crate::hnsw::flat_neighbors::FlatNeighborStore;
use crate::hnsw::graph::{ARENA_INITIAL_CAPACITY, HnswIndex, Node, Xorshift64};

/// Magic header for rkyv-serialized HNSW snapshots (6 bytes).
const HNSW_RKYV_MAGIC: &[u8; 6] = b"RKHNS\0";

/// rkyv-serialized HNSW snapshot. SoA layout for better rkyv compatibility
/// (flat Vecs instead of Vec<struct>).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct HnswSnapshotRkyv {
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub metric: u8,
    pub entry_point: Option<u32>,
    pub max_layer: usize,
    pub rng_state: u64,
    /// Per-node vectors (SoA).
    pub node_vectors: Vec<Vec<f32>>,
    /// Per-node neighbor lists (SoA).
    pub node_neighbors: Vec<Vec<Vec<u32>>>,
    /// Per-node deleted flags (SoA).
    pub node_deleted: Vec<bool>,
}

impl HnswIndex {
    /// Serialize the index to rkyv bytes (with magic header) for storage.
    ///
    /// Magic header `RKHNS\0` allows `from_checkpoint` to detect format.
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = HnswSnapshotRkyv {
            dim: self.dim,
            m: self.params.m,
            m0: self.params.m0,
            ef_construction: self.params.ef_construction,
            metric: self.params.metric as u8,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            rng_state: self.rng.0,
            node_vectors: self.nodes.iter().map(|n| n.vector.clone()).collect(),
            node_neighbors: if let Some(ref flat) = self.flat_neighbors {
                flat.to_nested(self.nodes.len())
            } else {
                self.nodes.iter().map(|n| n.neighbors.clone()).collect()
            },
            node_deleted: self.nodes.iter().map(|n| n.deleted).collect(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .expect("HNSW rkyv serialization should not fail");
        let mut buf = Vec::with_capacity(HNSW_RKYV_MAGIC.len() + rkyv_bytes.len());
        buf.extend_from_slice(HNSW_RKYV_MAGIC);
        buf.extend_from_slice(&rkyv_bytes);
        buf
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Auto-detects format: rkyv (magic `RKHNS\0`) or legacy MessagePack.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > HNSW_RKYV_MAGIC.len() && &bytes[..HNSW_RKYV_MAGIC.len()] == HNSW_RKYV_MAGIC
        {
            return Self::from_rkyv_checkpoint(&bytes[HNSW_RKYV_MAGIC.len()..]);
        }
        Self::from_msgpack_checkpoint(bytes)
    }

    /// Restore from rkyv-serialized bytes.
    fn from_rkyv_checkpoint(bytes: &[u8]) -> Option<Self> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let snap: HnswSnapshotRkyv =
            rkyv::from_bytes::<HnswSnapshotRkyv, rkyv::rancor::Error>(&aligned).ok()?;
        Self::from_hnsw_snapshot(snap)
    }

    /// Restore from legacy MessagePack bytes.
    fn from_msgpack_checkpoint(bytes: &[u8]) -> Option<Self> {
        use zerompk::{FromMessagePack, ToMessagePack};

        #[derive(ToMessagePack, FromMessagePack)]
        struct Snapshot {
            dim: usize,
            m: usize,
            m0: usize,
            ef_construction: usize,
            metric: u8,
            entry_point: Option<u32>,
            max_layer: usize,
            rng_state: u64,
            nodes: Vec<NodeSnap>,
        }

        #[derive(ToMessagePack, FromMessagePack)]
        struct NodeSnap {
            vector: Vec<f32>,
            neighbors: Vec<Vec<u32>>,
            deleted: bool,
        }

        let snap: Snapshot = zerompk::from_msgpack(bytes).ok()?;
        Self::from_hnsw_snapshot(HnswSnapshotRkyv {
            dim: snap.dim,
            m: snap.m,
            m0: snap.m0,
            ef_construction: snap.ef_construction,
            metric: snap.metric,
            entry_point: snap.entry_point,
            max_layer: snap.max_layer,
            rng_state: snap.rng_state,
            node_vectors: snap.nodes.iter().map(|n| n.vector.clone()).collect(),
            node_neighbors: snap.nodes.iter().map(|n| n.neighbors.clone()).collect(),
            node_deleted: snap.nodes.iter().map(|n| n.deleted).collect(),
        })
    }

    /// Reconstruct HnswIndex from deserialized snapshot fields.
    fn from_hnsw_snapshot(snap: HnswSnapshotRkyv) -> Option<Self> {
        use nodedb_types::hnsw::HnswParams;

        let metric = match snap.metric {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        };

        let flat = FlatNeighborStore::from_nested(&snap.node_neighbors);

        let nodes: Vec<Node> = snap
            .node_vectors
            .into_iter()
            .zip(snap.node_deleted)
            .map(|(vector, deleted)| Node {
                vector,
                neighbors: Vec::new(),
                deleted,
            })
            .collect();

        let initial_capacity = snap.ef_construction.max(ARENA_INITIAL_CAPACITY);
        Some(Self {
            dim: snap.dim,
            params: HnswParams {
                m: snap.m,
                m0: snap.m0,
                ef_construction: snap.ef_construction,
                metric,
            },
            nodes,
            entry_point: snap.entry_point,
            max_layer: snap.max_layer,
            rng: Xorshift64::new(snap.rng_state),
            flat_neighbors: Some(flat),
            arena: RefCell::new(BeamSearchArena::new(initial_capacity)),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn make_index() -> HnswIndex {
        HnswIndex::with_seed(
            3,
            HnswParams {
                m: 4,
                m0: 8,
                ef_construction: 32,
                metric: DistanceMetric::L2,
            },
            12345,
        )
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut idx = make_index();
        for i in 0..50 {
            idx.insert(vec![(i as f32) * 0.1, (i as f32) * 0.2, (i as f32) * 0.3])
                .unwrap();
        }

        let bytes = idx.checkpoint_to_bytes();
        assert!(!bytes.is_empty());

        let restored = HnswIndex::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.dim(), 3);
        assert_eq!(restored.entry_point(), idx.entry_point());
        assert_eq!(restored.max_layer(), idx.max_layer());

        let query = vec![1.0, 2.0, 3.0];
        let orig_results = idx.search(&query, 5, 32);
        let rest_results = restored.search(&query, 5, 32);
        assert_eq!(orig_results.len(), rest_results.len());
        for (a, b) in orig_results.iter().zip(rest_results.iter()) {
            assert_eq!(a.id, b.id);
            assert!((a.distance - b.distance).abs() < 1e-6);
        }
    }
}
