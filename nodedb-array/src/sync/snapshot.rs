//! Snapshot wire types and chunk codec for array CRDT sync.
//!
//! A [`TileSnapshot`] captures the full state of a tile coord-range at a
//! specific HLC. Snapshots are used for catch-up replication (first connect,
//! long disconnect, after log GC) so receivers can resume the op-stream from
//! [`TileSnapshot::snapshot_hlc`] without replaying the full op history.
//!
//! The actual snapshot *builder* (which walks engine state) lands in Phase F
//! (Origin). This file ships only the wire shape and the streaming chunk codec.

use serde::{Deserialize, Serialize};

use crate::error::{ArrayError, ArrayResult};
use crate::sync::hlc::Hlc;
use crate::types::coord::value::CoordValue;

// ─── Coordinate range ────────────────────────────────────────────────────────

/// A half-open coordinate range `[lo, hi)`.
///
/// Convention (matches the engine): `lo` is inclusive, `hi` is exclusive.
/// Both bounds have one element per array dimension, in the same order as
/// [`crate::schema::ArraySchema::dims`].
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CoordRange {
    /// Inclusive lower bound (one value per dimension).
    pub lo: Vec<CoordValue>,
    /// Exclusive upper bound (one value per dimension).
    pub hi: Vec<CoordValue>,
}

// ─── Snapshot types ───────────────────────────────────────────────────────────

/// Full state snapshot for a tile coord-range at a specific HLC.
///
/// `tile_blob` is an opaque compressed tile payload produced by the origin
/// engine's structural codec. Receivers write it directly into their tile
/// store without re-encoding.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TileSnapshot {
    /// Name of the target array collection.
    pub array: String,
    /// Coord range covered by this snapshot (half-open: lo inclusive, hi exclusive).
    pub coord_range: CoordRange,
    /// Compressed tile payload produced by the structural codec.
    pub tile_blob: Vec<u8>,
    /// HLC at which this snapshot was taken; ops with `hlc >= snapshot_hlc`
    /// should be applied on top after the snapshot is loaded.
    pub snapshot_hlc: Hlc,
    /// HLC of the array schema that was in effect when this snapshot was taken.
    pub schema_hlc: Hlc,
}

/// One chunk of a streaming [`TileSnapshot`].
///
/// Large tile blobs are split into fixed-size chunks for transmission.
/// Reassemble with [`assemble_chunks`].
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotChunk {
    /// Name of the target array collection.
    pub array: String,
    /// Zero-based index of this chunk within the stream.
    pub chunk_index: u32,
    /// Total number of chunks in the stream.
    pub total_chunks: u32,
    /// Raw bytes of this chunk (a slice of the encoded `tile_blob`).
    pub payload: Vec<u8>,
    /// HLC at which the snapshot was taken; echoed on every chunk for routing.
    pub snapshot_hlc: Hlc,
}

/// Metadata header sent before the chunk stream begins.
///
/// Receivers use this to pre-allocate storage and validate completeness.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotHeader {
    /// Name of the target array collection.
    pub array: String,
    /// Coord range covered by this snapshot.
    pub coord_range: CoordRange,
    /// Number of [`SnapshotChunk`]s that will follow.
    pub total_chunks: u32,
    /// HLC at which the snapshot was taken.
    pub snapshot_hlc: Hlc,
    /// HLC of the schema in effect when this snapshot was taken.
    pub schema_hlc: Hlc,
}

// ─── Codec ───────────────────────────────────────────────────────────────────

/// Encode a [`TileSnapshot`] to MessagePack bytes.
///
/// Use [`decode_snapshot`] to reverse. Errors map to
/// [`ArrayError::SegmentCorruption`].
pub fn encode_snapshot(s: &TileSnapshot) -> ArrayResult<Vec<u8>> {
    zerompk::to_msgpack_vec(s).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("snapshot encode failed: {e}"),
    })
}

/// Decode a [`TileSnapshot`] from MessagePack bytes produced by [`encode_snapshot`].
///
/// Errors map to [`ArrayError::SegmentCorruption`].
pub fn decode_snapshot(b: &[u8]) -> ArrayResult<TileSnapshot> {
    zerompk::from_msgpack(b).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("snapshot decode failed: {e}"),
    })
}

// ─── Chunking ────────────────────────────────────────────────────────────────

/// Split a [`TileSnapshot`] into a header plus a sequence of fixed-size chunks.
///
/// `max_chunk_bytes` must be at least 64. Returns
/// [`ArrayError::InvalidOp`] if it is smaller.
///
/// `total_chunks = ceil(blob.len() / max_chunk_bytes).max(1)` — there is
/// always at least one chunk, even for an empty blob.
pub fn split_into_chunks(
    s: &TileSnapshot,
    max_chunk_bytes: usize,
) -> ArrayResult<(SnapshotHeader, Vec<SnapshotChunk>)> {
    if max_chunk_bytes < 64 {
        return Err(ArrayError::InvalidOp {
            detail: "max_chunk_bytes too small".into(),
        });
    }

    let blob = &s.tile_blob;
    let total_chunks = if blob.is_empty() {
        1
    } else {
        blob.len().div_ceil(max_chunk_bytes).max(1) as u32
    };

    let header = SnapshotHeader {
        array: s.array.clone(),
        coord_range: s.coord_range.clone(),
        total_chunks,
        snapshot_hlc: s.snapshot_hlc,
        schema_hlc: s.schema_hlc,
    };

    let chunks: Vec<SnapshotChunk> = if blob.is_empty() {
        vec![SnapshotChunk {
            array: s.array.clone(),
            chunk_index: 0,
            total_chunks: 1,
            payload: Vec::new(),
            snapshot_hlc: s.snapshot_hlc,
        }]
    } else {
        blob.chunks(max_chunk_bytes)
            .enumerate()
            .map(|(i, slice)| SnapshotChunk {
                array: s.array.clone(),
                chunk_index: i as u32,
                total_chunks,
                payload: slice.to_vec(),
                snapshot_hlc: s.snapshot_hlc,
            })
            .collect()
    };

    Ok((header, chunks))
}

/// Reassemble a [`TileSnapshot`] from a header and a slice of chunks.
///
/// Sorts chunks by `chunk_index`, validates that exactly `0..total_chunks`
/// are present (no gaps, no duplicates), concatenates their payloads, and
/// reconstructs the snapshot.
///
/// Validation failures → [`ArrayError::SegmentCorruption`].
pub fn assemble_chunks(
    header: &SnapshotHeader,
    chunks: &mut [SnapshotChunk],
) -> ArrayResult<TileSnapshot> {
    chunks.sort_by_key(|c| c.chunk_index);

    let expected = header.total_chunks as usize;

    if chunks.len() != expected {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("expected {} chunks, got {}", expected, chunks.len()),
        });
    }

    for (i, chunk) in chunks.iter().enumerate() {
        if chunk.chunk_index as usize != i {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("chunk index gap: expected {i}, got {}", chunk.chunk_index),
            });
        }
    }

    let tile_blob: Vec<u8> = chunks
        .iter()
        .flat_map(|c| c.payload.iter().copied())
        .collect();

    Ok(TileSnapshot {
        array: header.array.clone(),
        coord_range: header.coord_range.clone(),
        tile_blob,
        snapshot_hlc: header.snapshot_hlc,
        schema_hlc: header.schema_hlc,
    })
}

// ─── Sink trait ──────────────────────────────────────────────────────────────

/// Receiver for completed snapshots during GC compaction.
///
/// Implementations live in `nodedb-lite` (redb-backed) and `nodedb`
/// (WAL-backed) and are wired in later phases. This trait gives the pure
/// GC logic in [`crate::sync::gc`] a place to write snapshots before
/// dropping the underlying ops from the log.
pub trait SnapshotSink: Send + Sync {
    /// Persist a completed snapshot.
    fn write_snapshot(&self, snapshot: &TileSnapshot) -> ArrayResult<()>;
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::replica_id::ReplicaId;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
    }

    fn range() -> CoordRange {
        CoordRange {
            lo: vec![CoordValue::Int64(0)],
            hi: vec![CoordValue::Int64(100)],
        }
    }

    fn snapshot(blob_len: usize) -> TileSnapshot {
        TileSnapshot {
            array: "test".into(),
            coord_range: range(),
            tile_blob: vec![0xAB; blob_len],
            snapshot_hlc: hlc(1000),
            schema_hlc: hlc(500),
        }
    }

    #[test]
    fn coord_range_roundtrip() {
        let r = range();
        let bytes = zerompk::to_msgpack_vec(&r).unwrap();
        let back: CoordRange = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn tile_snapshot_roundtrip() {
        let s = snapshot(256);
        let bytes = encode_snapshot(&s).unwrap();
        let back = decode_snapshot(&bytes).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn split_then_assemble_roundtrip() {
        let s = snapshot(1000);
        let (header, mut chunks) = split_into_chunks(&s, 300).unwrap();
        // ceil(1000 / 300) = 4
        assert_eq!(header.total_chunks, 4);
        assert_eq!(chunks.len(), 4);
        let reassembled = assemble_chunks(&header, &mut chunks).unwrap();
        assert_eq!(reassembled.tile_blob, s.tile_blob);
        assert_eq!(reassembled.array, s.array);
        assert_eq!(reassembled.snapshot_hlc, s.snapshot_hlc);
        assert_eq!(reassembled.schema_hlc, s.schema_hlc);
    }

    #[test]
    fn assemble_rejects_missing_chunk() {
        let s = snapshot(1000);
        let (header, mut chunks) = split_into_chunks(&s, 300).unwrap();
        // Remove chunk at index 2 to create a gap.
        chunks.retain(|c| c.chunk_index != 2);
        let result = assemble_chunks(&header, &mut chunks);
        assert!(matches!(result, Err(ArrayError::SegmentCorruption { .. })));
    }

    #[test]
    fn assemble_rejects_duplicate_chunk() {
        let s = snapshot(600);
        let (header, mut chunks) = split_into_chunks(&s, 300).unwrap();
        // Duplicate chunk 0 to produce 3 chunks for a 2-chunk stream.
        let dup = chunks[0].clone();
        chunks.push(dup);
        let result = assemble_chunks(&header, &mut chunks);
        assert!(matches!(result, Err(ArrayError::SegmentCorruption { .. })));
    }

    #[test]
    fn split_rejects_too_small_max() {
        let s = snapshot(100);
        let result = split_into_chunks(&s, 63);
        assert!(matches!(result, Err(ArrayError::InvalidOp { .. })));
    }

    #[test]
    fn split_empty_blob_produces_one_chunk() {
        let s = snapshot(0);
        let (header, chunks) = split_into_chunks(&s, 64).unwrap();
        assert_eq!(header.total_chunks, 1);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].payload.is_empty());
    }
}
