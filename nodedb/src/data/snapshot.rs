//! Data Plane core snapshot — serializes all engine state for transfer.
//!
//! Used by InstallSnapshot (lagging follower recovery) and vShard migration
//! (Phase 1 base copy). Captures the full state of a Data Plane core:
//! - SparseEngine documents and indexes (from redb)
//! - EdgeStore edges and reverse edges (from redb)
//! - HnswIndex vectors and graph structure (in-memory)
//! - CRDT engine state per tenant (loro export)
//! - Watermark LSN

/// Serializable snapshot of a single HNSW index.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HnswSnapshot {
    #[serde(default)]
    pub tenant_id: u32,
    pub collection: String,
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    /// 0=L2, 1=Cosine, 2=InnerProduct
    pub metric: u8,
    pub entry_point: Option<u32>,
    pub max_layer: usize,
    pub rng_state: u64,
    /// All vectors: [node_0_vector, node_1_vector, ...]
    pub vectors: Vec<Vec<f32>>,
    /// Per-node neighbor lists: neighbors[node][layer] = [neighbor_ids]
    pub neighbors: Vec<Vec<Vec<u32>>>,
}

/// Serializable snapshot of a CRDT tenant's state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CrdtSnapshot {
    pub tenant_id: u32,
    pub peer_id: u64,
    /// Loro binary snapshot (from LoroDoc::export_snapshot).
    pub snapshot_bytes: Vec<u8>,
}

/// Serializable key-value pair from a redb table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KvPair {
    pub key: String,
    pub value: Vec<u8>,
}

/// Complete snapshot of a Data Plane core's state.
///
/// Designed for serialization via MessagePack and transfer over the network
/// as InstallSnapshot data or VShardEnvelope::SegmentChunk payloads.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CoreSnapshot {
    /// Core/vShard watermark LSN.
    pub watermark: u64,

    /// All documents from SparseEngine.
    pub sparse_documents: Vec<KvPair>,
    /// All secondary indexes from SparseEngine.
    pub sparse_indexes: Vec<KvPair>,

    /// All edges from EdgeStore.
    pub edges: Vec<KvPair>,
    /// All reverse edges from EdgeStore.
    pub reverse_edges: Vec<KvPair>,

    /// All HNSW vector indexes.
    pub hnsw_indexes: Vec<HnswSnapshot>,

    /// All CRDT tenant states.
    pub crdt_snapshots: Vec<CrdtSnapshot>,
}

impl CoreSnapshot {
    pub fn empty() -> Self {
        Self {
            watermark: 0,
            sparse_documents: Vec::new(),
            sparse_indexes: Vec::new(),
            edges: Vec::new(),
            reverse_edges: Vec::new(),
            hnsw_indexes: Vec::new(),
            crdt_snapshots: Vec::new(),
        }
    }

    /// Serialize to bytes for network transfer.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("CoreSnapshot: {e}"),
        })
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        rmp_serde::from_slice(data).ok()
    }

    /// Approximate size in bytes (for progress tracking).
    pub fn approx_size(&self) -> usize {
        let sparse = self
            .sparse_documents
            .iter()
            .map(|kv| kv.key.len() + kv.value.len())
            .sum::<usize>()
            + self
                .sparse_indexes
                .iter()
                .map(|kv| kv.key.len() + kv.value.len())
                .sum::<usize>();
        let edges = self
            .edges
            .iter()
            .map(|kv| kv.key.len() + kv.value.len())
            .sum::<usize>()
            + self
                .reverse_edges
                .iter()
                .map(|kv| kv.key.len() + kv.value.len())
                .sum::<usize>();
        let vectors: usize = self
            .hnsw_indexes
            .iter()
            .map(|h| h.vectors.iter().map(|v| v.len() * 4).sum::<usize>())
            .sum();
        let crdt: usize = self
            .crdt_snapshots
            .iter()
            .map(|c| c.snapshot_bytes.len())
            .sum();
        sparse + edges + vectors + crdt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_roundtrip() {
        let snap = CoreSnapshot::empty();
        let bytes = snap.to_bytes().unwrap();
        let decoded = CoreSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.watermark, 0);
        assert!(decoded.sparse_documents.is_empty());
        assert!(decoded.hnsw_indexes.is_empty());
    }

    #[test]
    fn snapshot_with_data_roundtrip() {
        let snap = CoreSnapshot {
            watermark: 42,
            sparse_documents: vec![
                KvPair {
                    key: "users:u1".into(),
                    value: b"alice".to_vec(),
                },
                KvPair {
                    key: "users:u2".into(),
                    value: b"bob".to_vec(),
                },
            ],
            sparse_indexes: vec![KvPair {
                key: "users:name:alice:u1".into(),
                value: vec![],
            }],
            edges: vec![KvPair {
                key: "u1\0knows\0u2".into(),
                value: b"{}".to_vec(),
            }],
            reverse_edges: vec![KvPair {
                key: "u2\0knows\0u1".into(),
                value: vec![],
            }],
            hnsw_indexes: vec![HnswSnapshot {
                tenant_id: 1,
                collection: "embeddings".into(),
                dim: 3,
                m: 16,
                m0: 32,
                ef_construction: 200,
                metric: 1, // Cosine
                entry_point: Some(0),
                max_layer: 1,
                rng_state: 12345,
                vectors: vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                neighbors: vec![
                    vec![vec![1], vec![]], // node 0: layer 0 neighbors=[1], layer 1 neighbors=[]
                    vec![vec![0]],         // node 1: layer 0 neighbors=[0]
                ],
            }],
            crdt_snapshots: vec![CrdtSnapshot {
                tenant_id: 1,
                peer_id: 100,
                snapshot_bytes: vec![0xAB, 0xCD],
            }],
        };

        let bytes = snap.to_bytes().unwrap();
        let decoded = CoreSnapshot::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.watermark, 42);
        assert_eq!(decoded.sparse_documents.len(), 2);
        assert_eq!(decoded.sparse_documents[0].key, "users:u1");
        assert_eq!(decoded.edges.len(), 1);
        assert_eq!(decoded.hnsw_indexes.len(), 1);
        assert_eq!(decoded.hnsw_indexes[0].vectors.len(), 2);
        assert_eq!(decoded.hnsw_indexes[0].dim, 3);
        assert_eq!(decoded.crdt_snapshots.len(), 1);
        assert!(decoded.approx_size() > 0);
    }

    #[test]
    fn hnsw_snapshot_all_metrics() {
        for metric in [0u8, 1, 2] {
            let snap = CoreSnapshot {
                hnsw_indexes: vec![HnswSnapshot {
                    tenant_id: 1,
                    collection: "test".into(),
                    dim: 2,
                    m: 8,
                    m0: 16,
                    ef_construction: 100,
                    metric,
                    entry_point: None,
                    max_layer: 0,
                    rng_state: 0,
                    vectors: vec![],
                    neighbors: vec![],
                }],
                ..CoreSnapshot::empty()
            };
            let bytes = snap.to_bytes().unwrap();
            let decoded = CoreSnapshot::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.hnsw_indexes[0].metric, metric);
        }
    }
}
