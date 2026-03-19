//! Snapshot export for CoreLoop state.

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Export the current state of all engines into a serializable snapshot.
    ///
    /// This captures the complete Data Plane state for this core:
    /// redb tables (sparse + edge), in-memory HNSW indexes, and CRDT state.
    pub fn export_snapshot(&self) -> crate::Result<crate::data::snapshot::CoreSnapshot> {
        use crate::data::snapshot::*;

        let sparse_documents: Vec<KvPair> = self
            .sparse
            .export_documents()?
            .into_iter()
            .map(|(k, v)| KvPair { key: k, value: v })
            .collect();

        let sparse_indexes: Vec<KvPair> = self
            .sparse
            .export_indexes()?
            .into_iter()
            .map(|(k, v)| KvPair { key: k, value: v })
            .collect();

        let edges: Vec<KvPair> = self
            .edge_store
            .export_edges()?
            .into_iter()
            .map(|(k, v)| KvPair { key: k, value: v })
            .collect();

        let reverse_edges: Vec<KvPair> = self
            .edge_store
            .export_reverse_edges()?
            .into_iter()
            .map(|(k, v)| KvPair { key: k, value: v })
            .collect();

        let hnsw_indexes: Vec<HnswSnapshot> = self
            .vector_indexes
            .iter()
            .map(|(name, idx)| {
                let (tenant_id, collection) = name
                    .split_once(':')
                    .map(|(t, c)| (t.parse::<u32>().unwrap_or(0), c.to_string()))
                    .unwrap_or((0, name.clone()));
                let metric = match idx.params().metric {
                    crate::engine::vector::distance::DistanceMetric::L2 => 0u8,
                    crate::engine::vector::distance::DistanceMetric::Cosine => 1,
                    crate::engine::vector::distance::DistanceMetric::InnerProduct => 2,
                };
                HnswSnapshot {
                    tenant_id,
                    collection,
                    dim: idx.dim(),
                    m: idx.params().m,
                    m0: idx.params().m0,
                    ef_construction: idx.params().ef_construction,
                    metric,
                    entry_point: idx.entry_point(),
                    max_layer: idx.max_layer(),
                    rng_state: idx.rng_state(),
                    vectors: idx.export_vectors(),
                    neighbors: idx.export_neighbors(),
                }
            })
            .collect();

        let crdt_snapshots: Vec<CrdtSnapshot> = self
            .crdt_engines
            .iter()
            .map(|(tid, engine)| CrdtSnapshot {
                tenant_id: tid.as_u32(),
                peer_id: engine.peer_id(),
                snapshot_bytes: engine.export_snapshot_bytes(),
            })
            .collect();

        Ok(CoreSnapshot {
            watermark: self.watermark.as_u64(),
            sparse_documents,
            sparse_indexes,
            edges,
            reverse_edges,
            hnsw_indexes,
            crdt_snapshots,
        })
    }
}
