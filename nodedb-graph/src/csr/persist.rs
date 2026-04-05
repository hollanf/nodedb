//! CSR checkpoint serialization/deserialization.
//!
//! Supports two serialization formats:
//! - **MessagePack** (zerompk): Legacy format, backwards-compatible.
//! - **rkyv**: ~3x faster serialization/deserialization. Detected on load
//!   by magic bytes (`RKCSR\0` header). Future: mmap zero-copy access.
//!
//! Used by both Origin (via redb storage) and Lite (via embedded checkpoint).

use std::collections::HashMap;

use zerompk::{FromMessagePack, ToMessagePack};

use super::index::CsrIndex;

/// Magic header for rkyv-serialized CSR snapshots (6 bytes).
const RKYV_MAGIC: &[u8; 6] = b"RKCSR\0";

#[derive(ToMessagePack, FromMessagePack)]
struct CsrSnapshotMsgpack {
    nodes: Vec<String>,
    labels: Vec<String>,
    out_offsets: Vec<u32>,
    out_targets: Vec<u32>,
    out_labels: Vec<u16>,
    in_offsets: Vec<u32>,
    in_targets: Vec<u32>,
    in_labels: Vec<u16>,
    buffer_out: Vec<Vec<(u16, u32)>>,
    buffer_in: Vec<Vec<(u16, u32)>>,
    deleted: Vec<(u32, u16, u32)>,
    has_weights: bool,
    out_weights: Option<Vec<f64>>,
    in_weights: Option<Vec<f64>>,
    buffer_out_weights: Vec<Vec<f64>>,
    buffer_in_weights: Vec<Vec<f64>>,
}

/// rkyv-serialized CSR snapshot for fast save/load.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CsrSnapshotRkyv {
    nodes: Vec<String>,
    labels: Vec<String>,
    out_offsets: Vec<u32>,
    out_targets: Vec<u32>,
    out_labels: Vec<u16>,
    in_offsets: Vec<u32>,
    in_targets: Vec<u32>,
    in_labels: Vec<u16>,
    buffer_out: Vec<Vec<(u16, u32)>>,
    buffer_in: Vec<Vec<(u16, u32)>>,
    deleted: Vec<(u32, u16, u32)>,
    has_weights: bool,
    out_weights: Option<Vec<f64>>,
    in_weights: Option<Vec<f64>>,
    buffer_out_weights: Vec<Vec<f64>>,
    buffer_in_weights: Vec<Vec<f64>>,
}

impl CsrIndex {
    /// Serialize the index to rkyv bytes (with magic header) for storage.
    ///
    /// rkyv is ~3x faster than MessagePack for both serialization and
    /// deserialization. The magic header allows `from_checkpoint` to
    /// auto-detect the format for backward compatibility.
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = CsrSnapshotRkyv {
            nodes: self.id_to_node.clone(),
            labels: self.id_to_label.clone(),
            out_offsets: self.out_offsets.clone(),
            out_targets: self.out_targets.clone(),
            out_labels: self.out_labels.clone(),
            in_offsets: self.in_offsets.clone(),
            in_targets: self.in_targets.clone(),
            in_labels: self.in_labels.clone(),
            buffer_out: self.buffer_out.clone(),
            buffer_in: self.buffer_in.clone(),
            deleted: self.deleted_edges.iter().copied().collect(),
            has_weights: self.has_weights,
            out_weights: self.out_weights.clone(),
            in_weights: self.in_weights.clone(),
            buffer_out_weights: self.buffer_out_weights.clone(),
            buffer_in_weights: self.buffer_in_weights.clone(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .expect("CSR rkyv serialization should not fail");
        let mut buf = Vec::with_capacity(RKYV_MAGIC.len() + rkyv_bytes.len());
        buf.extend_from_slice(RKYV_MAGIC);
        buf.extend_from_slice(&rkyv_bytes);
        buf
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Auto-detects format: rkyv (magic header `RKCSR\0`) or legacy MessagePack.
    /// Backwards-compatible with old checkpoints.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > RKYV_MAGIC.len() && &bytes[..RKYV_MAGIC.len()] == RKYV_MAGIC {
            return Self::from_rkyv_checkpoint(&bytes[RKYV_MAGIC.len()..]);
        }
        Self::from_msgpack_checkpoint(bytes)
    }

    /// Restore from rkyv-serialized bytes.
    fn from_rkyv_checkpoint(bytes: &[u8]) -> Option<Self> {
        // rkyv requires aligned data for zero-copy access.
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let snap: CsrSnapshotRkyv =
            rkyv::from_bytes::<CsrSnapshotRkyv, rkyv::rancor::Error>(&aligned).ok()?;
        Some(Self::from_snapshot_fields(snap))
    }

    /// Restore from legacy MessagePack bytes.
    fn from_msgpack_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snap: CsrSnapshotMsgpack = zerompk::from_msgpack(bytes).ok()?;
        Some(Self::from_snapshot_fields(CsrSnapshotRkyv {
            nodes: snap.nodes,
            labels: snap.labels,
            out_offsets: snap.out_offsets,
            out_targets: snap.out_targets,
            out_labels: snap.out_labels,
            in_offsets: snap.in_offsets,
            in_targets: snap.in_targets,
            in_labels: snap.in_labels,
            buffer_out: snap.buffer_out,
            buffer_in: snap.buffer_in,
            deleted: snap.deleted,
            has_weights: snap.has_weights,
            out_weights: snap.out_weights,
            in_weights: snap.in_weights,
            buffer_out_weights: snap.buffer_out_weights,
            buffer_in_weights: snap.buffer_in_weights,
        }))
    }

    /// Reconstruct CsrIndex from deserialized snapshot fields.
    fn from_snapshot_fields(snap: CsrSnapshotRkyv) -> Self {
        let node_to_id: HashMap<String, u32> = snap
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        let label_to_id: HashMap<String, u16> = snap
            .labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.clone(), i as u16))
            .collect();

        let node_count = snap.nodes.len();
        let access_counts = (0..node_count).map(|_| std::cell::Cell::new(0)).collect();

        let buffer_out_weights = if snap.buffer_out_weights.len() == node_count {
            snap.buffer_out_weights
        } else {
            vec![Vec::new(); node_count]
        };
        let buffer_in_weights = if snap.buffer_in_weights.len() == node_count {
            snap.buffer_in_weights
        } else {
            vec![Vec::new(); node_count]
        };

        Self {
            node_to_id,
            id_to_node: snap.nodes,
            label_to_id,
            id_to_label: snap.labels,
            out_offsets: snap.out_offsets,
            out_targets: snap.out_targets,
            out_labels: snap.out_labels,
            out_weights: snap.out_weights,
            in_offsets: snap.in_offsets,
            in_targets: snap.in_targets,
            in_labels: snap.in_labels,
            in_weights: snap.in_weights,
            buffer_out: snap.buffer_out,
            buffer_in: snap.buffer_in,
            buffer_out_weights,
            buffer_in_weights,
            deleted_edges: snap.deleted.into_iter().collect(),
            has_weights: snap.has_weights,
            access_counts,
            query_epoch: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr::index::Direction;

    #[test]
    fn checkpoint_roundtrip_unweighted() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b");
        csr.add_edge("b", "KNOWS", "c");
        csr.compact();

        let bytes = csr.checkpoint_to_bytes();
        let restored = CsrIndex::from_checkpoint(&bytes).expect("roundtrip");
        assert_eq!(restored.node_count(), 3);
        assert_eq!(restored.edge_count(), 2);
        assert!(!restored.has_weights());

        let n = restored.neighbors("a", Some("KNOWS"), Direction::Out);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "b");
    }

    #[test]
    fn checkpoint_roundtrip_weighted() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.5);
        csr.add_edge_weighted("b", "R", "c", 7.0);
        csr.add_edge("c", "R", "d");
        csr.compact();

        let bytes = csr.checkpoint_to_bytes();
        let restored = CsrIndex::from_checkpoint(&bytes).expect("roundtrip");
        assert!(restored.has_weights());
        assert_eq!(restored.edge_weight("a", "R", "b"), Some(2.5));
        assert_eq!(restored.edge_weight("b", "R", "c"), Some(7.0));
        assert_eq!(restored.edge_weight("c", "R", "d"), Some(1.0));
    }

    #[test]
    fn checkpoint_roundtrip_with_buffer() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        // Don't compact — edges in buffer.
        let bytes = csr.checkpoint_to_bytes();
        let restored = CsrIndex::from_checkpoint(&bytes).expect("roundtrip");
        assert_eq!(restored.edge_count(), 1);
    }
}
