//! Cell-to-tile partitioning for distributed array writes.
//!
//! Both `coord_put` and `coord_delete` receive a flat list of cells (or
//! coord tuples) paired with their pre-computed Hilbert prefixes. This
//! module groups them by tile bucket (the top `prefix_bits` of the Hilbert
//! prefix) and maps each bucket to its owning vShard via
//! `array_vshard_for_tile`.
//!
//! Callers pre-attach Hilbert prefixes because the query planner already
//! holds the schema and can compute them at planning time; doing the same
//! computation again inside the coordinator would require an additional
//! schema lookup that `nodedb-cluster` intentionally avoids (it carries no
//! dependency on `nodedb-array`).
//!
//! ## Output shape
//!
//! `partition_put_cells` returns a `Vec<(vshard_id, representative_hilbert_prefix,
//! cells_msgpack)>` where `cells_msgpack` is a zerompk-encoded
//! `Vec<raw-cell-bytes>` (the same bytes the shard handler passes to
//! `exec_put`).
//!
//! `partition_delete_coords` returns a `Vec<(vshard_id, coords_msgpack)>` where
//! `coords_msgpack` is a zerompk-encoded `Vec<raw-coord-bytes>`.

use std::collections::HashMap;

use crate::error::{ClusterError, Result};

use super::routing::array_vshard_for_tile;

/// One bucket produced by `partition_put_cells`.
pub struct PutBucket {
    pub vshard_id: u32,
    /// Hilbert prefix of the first cell added to this bucket. Used by the
    /// shard handler for routing validation (`validate_put_routing`).
    pub representative_hilbert_prefix: u64,
    /// zerompk-encoded `Vec<raw-cell-bytes>` for all cells in this bucket.
    pub cells_msgpack: Vec<u8>,
}

/// One bucket produced by `partition_delete_coords`.
pub struct DeleteBucket {
    pub vshard_id: u32,
    /// Hilbert prefix of the first coord added to this bucket. Used by the
    /// shard handler for routing validation (`validate_delete_routing`).
    pub representative_hilbert_prefix: u64,
    /// zerompk-encoded `Vec<raw-coord-bytes>` for all coords in this bucket.
    pub coords_msgpack: Vec<u8>,
}

/// Group `(hilbert_prefix, cell_msgpack)` pairs by tile bucket.
///
/// `cells` — each element is `(hilbert_prefix, zerompk-encoded single cell)`.
/// `prefix_bits` — routing granularity (1–16).
///
/// Returns one `PutBucket` per unique owning vShard. The order of buckets
/// in the output is unspecified but deterministic within a call.
pub fn partition_put_cells(cells: &[(u64, Vec<u8>)], prefix_bits: u8) -> Result<Vec<PutBucket>> {
    if cells.is_empty() {
        return Ok(Vec::new());
    }

    // vshard_id → (representative_hilbert_prefix, per-cell byte blobs)
    let mut buckets: HashMap<u32, (u64, Vec<Vec<u8>>)> = HashMap::new();

    for (hilbert_prefix, cell_bytes) in cells {
        let vshard_id = array_vshard_for_tile(*hilbert_prefix, prefix_bits)?;
        let entry = buckets
            .entry(vshard_id)
            .or_insert((*hilbert_prefix, Vec::new()));
        entry.1.push(cell_bytes.clone());
    }

    buckets
        .into_iter()
        .map(|(vshard_id, (representative, cell_blobs))| {
            let cells_msgpack = encode_blob_vec(&cell_blobs)?;
            Ok(PutBucket {
                vshard_id,
                representative_hilbert_prefix: representative,
                cells_msgpack,
            })
        })
        .collect()
}

/// Group `(hilbert_prefix, coord_msgpack)` pairs by tile bucket.
///
/// `coords` — each element is `(hilbert_prefix, zerompk-encoded single coord)`.
/// `prefix_bits` — routing granularity (1–16).
///
/// Returns one `DeleteBucket` per unique owning vShard.
pub fn partition_delete_coords(
    coords: &[(u64, Vec<u8>)],
    prefix_bits: u8,
) -> Result<Vec<DeleteBucket>> {
    if coords.is_empty() {
        return Ok(Vec::new());
    }

    // vshard_id → (representative_hilbert_prefix, coord byte blobs)
    let mut buckets: HashMap<u32, (u64, Vec<Vec<u8>>)> = HashMap::new();

    for (hilbert_prefix, coord_bytes) in coords {
        let vshard_id = array_vshard_for_tile(*hilbert_prefix, prefix_bits)?;
        let entry = buckets
            .entry(vshard_id)
            .or_insert((*hilbert_prefix, Vec::new()));
        entry.1.push(coord_bytes.clone());
    }

    buckets
        .into_iter()
        .map(|(vshard_id, (representative, coord_blobs))| {
            let coords_msgpack = encode_blob_vec(&coord_blobs)?;
            Ok(DeleteBucket {
                vshard_id,
                representative_hilbert_prefix: representative,
                coords_msgpack,
            })
        })
        .collect()
}

/// Encode a `Vec<Vec<u8>>` (collection of opaque blobs) as a zerompk array.
///
/// Each blob is emitted as raw msgpack bytes (already encoded by the caller)
/// wrapped in a msgpack bin or str value. We re-encode the entire collection
/// as a msgpack array of byte-arrays so the shard handler can deserialise it
/// as `Vec<raw-bytes>` and forward to the engine.
fn encode_blob_vec(blobs: &[Vec<u8>]) -> Result<Vec<u8>> {
    // zerompk::to_msgpack_vec requires Sized, so pass as a Vec reference.
    let owned: Vec<Vec<u8>> = blobs.to_vec();
    zerompk::to_msgpack_vec(&owned).map_err(|e| ClusterError::Codec {
        detail: format!("partition encode blob vec: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a fake (hilbert_prefix, cell_bytes) pair. The cell_bytes are
    // opaque for the partitioner — any non-empty Vec<u8> works.
    fn cell(prefix: u64, tag: u8) -> (u64, Vec<u8>) {
        (prefix, vec![tag])
    }

    // Build a fake coord pair.
    fn coord(prefix: u64, tag: u8) -> (u64, Vec<u8>) {
        (prefix, vec![tag])
    }

    // Helper: compute expected vshard for a given hilbert prefix and prefix_bits.
    fn expected_vshard(prefix: u64, prefix_bits: u8) -> u32 {
        array_vshard_for_tile(prefix, prefix_bits).unwrap()
    }

    #[test]
    fn partition_put_empty_returns_empty() {
        let buckets = partition_put_cells(&[], 8).unwrap();
        assert!(buckets.is_empty());
    }

    #[test]
    fn partition_delete_empty_returns_empty() {
        let buckets = partition_delete_coords(&[], 8).unwrap();
        assert!(buckets.is_empty());
    }

    #[test]
    fn partition_put_single_cell_one_bucket() {
        // prefix_bits=10, stride=1 → vshard == bucket == top 10 bits.
        // All-zero hilbert prefix → bucket 0 → vshard 0.
        let cells = vec![cell(0, 0xAA)];
        let buckets = partition_put_cells(&cells, 10).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].vshard_id, 0);
        assert_eq!(buckets[0].representative_hilbert_prefix, 0);
    }

    #[test]
    fn partition_put_cells_by_tile_three_buckets() {
        // With prefix_bits=10 (stride=1), every distinct top-10-bit value
        // is its own vshard. Use three well-separated prefixes.
        //
        //  prefix 0x0000_0000_0000_0000 → bucket 0  → vshard 0
        //  prefix 0x0040_0000_0000_0000 → top 10 bits = 1 → vshard 1
        //  prefix 0x0080_0000_0000_0000 → top 10 bits = 2 → vshard 2
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;
        let p2 = 0x0080_0000_0000_0000u64;

        let cells = vec![
            cell(p0, 0x01),
            cell(p1, 0x02),
            cell(p0, 0x03), // same bucket as first
            cell(p2, 0x04),
            cell(p1, 0x05), // same bucket as second
        ];

        let mut buckets = partition_put_cells(&cells, 10).unwrap();
        // Sort by vshard_id for deterministic assertion.
        buckets.sort_by_key(|b| b.vshard_id);
        assert_eq!(buckets.len(), 3);

        assert_eq!(buckets[0].vshard_id, expected_vshard(p0, 10));
        assert_eq!(buckets[1].vshard_id, expected_vshard(p1, 10));
        assert_eq!(buckets[2].vshard_id, expected_vshard(p2, 10));

        // Bucket 0 and 1 each have 2 cells, bucket 2 has 1.
        let blobs0: Vec<Vec<u8>> = zerompk::from_msgpack(&buckets[0].cells_msgpack).unwrap();
        let blobs1: Vec<Vec<u8>> = zerompk::from_msgpack(&buckets[1].cells_msgpack).unwrap();
        let blobs2: Vec<Vec<u8>> = zerompk::from_msgpack(&buckets[2].cells_msgpack).unwrap();
        assert_eq!(blobs0.len(), 2);
        assert_eq!(blobs1.len(), 2);
        assert_eq!(blobs2.len(), 1);
    }

    #[test]
    fn partition_delete_coords_by_tile() {
        let p0 = 0x0000_0000_0000_0000u64;
        let p1 = 0x0040_0000_0000_0000u64;

        let coords = vec![coord(p0, 0xAA), coord(p1, 0xBB), coord(p0, 0xCC)];

        let mut buckets = partition_delete_coords(&coords, 10).unwrap();
        buckets.sort_by_key(|b| b.vshard_id);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].vshard_id, expected_vshard(p0, 10));
        assert_eq!(buckets[1].vshard_id, expected_vshard(p1, 10));

        let blobs0: Vec<Vec<u8>> = zerompk::from_msgpack(&buckets[0].coords_msgpack).unwrap();
        let blobs1: Vec<Vec<u8>> = zerompk::from_msgpack(&buckets[1].coords_msgpack).unwrap();
        assert_eq!(blobs0.len(), 2);
        assert_eq!(blobs1.len(), 1);
    }

    #[test]
    fn partition_put_invalid_prefix_bits_errors() {
        let cells = vec![cell(0, 0x01)];
        assert!(partition_put_cells(&cells, 0).is_err());
        assert!(partition_put_cells(&cells, 17).is_err());
    }
}
