//! vShard routing for array tiles.
//!
//! Arrays are sharded by Hilbert-curve prefix: the top `prefix_bits`
//! of a tile's `hilbert_prefix` determine which vShard bucket owns it.
//!
//! With `prefix_bits = P` there are `2^P` distinct buckets numbered
//! `0 .. 2^P - 1`. Each bucket maps to a contiguous range of vShards
//! within the cluster's `VSHARD_COUNT`-wide space. The stride is
//! `VSHARD_COUNT / 2^P` (integer division). Bucket `b` owns vShards
//! `[b * stride .. (b + 1) * stride)`. The primary vShard for a bucket
//! is the first one in that range (`b * stride`).

use crate::error::{ClusterError, Result};
use crate::routing::VSHARD_COUNT;

// Array Hilbert-range ownership is derived entirely from vShard ownership:
// bucket→vshard is a deterministic function of `prefix_bits`, and
// vshard→node is maintained by `RoutingTable` (updated by committed
// `RoutingChange::ReassignVShard` entries). No separate array-specific
// routing variant is needed in the metadata log — array migration is "free"
// because a vShard rebalance already atomically re-points all array tile
// ownership through the same `RoutingTable` cut-over path.

/// Determine the primary owning vShard for a single tile identified by
/// its raw Hilbert-prefix value.
///
/// `hilbert_prefix` — the tile's u64 Hilbert key.
/// `prefix_bits` — number of high-order bits used for routing (1–16).
///
/// Returns the vShard ID in `[0, VSHARD_COUNT)`.
pub fn array_vshard_for_tile(hilbert_prefix: u64, prefix_bits: u8) -> Result<u32> {
    validate_prefix_bits(prefix_bits)?;
    let bucket = prefix_bucket(hilbert_prefix, prefix_bits);
    Ok(bucket_to_vshard(bucket, prefix_bits))
}

/// Determine all vShards that intersect a set of Hilbert-prefix ranges.
///
/// `slice_hilbert_ranges` — list of `(lo, hi)` inclusive Hilbert-prefix
/// ranges covering the slice's MBR. An empty slice means "unbounded"
/// (returns all vShards up to `total_shards`).
///
/// `prefix_bits` — routing granularity (1–16).
/// `total_shards` — number of active vShards; returned IDs are
/// clamped to `[0, total_shards)`.
///
/// Returns a sorted, deduplicated list of vShard IDs.
pub fn array_vshards_for_slice(
    slice_hilbert_ranges: &[(u64, u64)],
    prefix_bits: u8,
    total_shards: u32,
) -> Result<Vec<u32>> {
    if total_shards == 0 {
        return Ok(Vec::new());
    }
    validate_prefix_bits(prefix_bits)?;

    let stride = vshard_stride(prefix_bits);
    let mut out: Vec<u32> = Vec::new();

    // Unbounded slice: return one canonical primary vShard per bucket.
    // Within a bucket all vShards serve the same Hilbert range, so fanning
    // out to every member would multiply the result by `stride`.
    if slice_hilbert_ranges.is_empty() {
        let mut vshard_id: u32 = 0;
        while vshard_id < total_shards && vshard_id < VSHARD_COUNT {
            out.push(vshard_id);
            vshard_id = vshard_id.saturating_add(stride);
            if stride == 0 {
                break;
            }
        }
        return Ok(out);
    }

    for &(lo, hi) in slice_hilbert_ranges {
        let lo_bucket = prefix_bucket(lo, prefix_bits);
        // `hi` is inclusive, so we take the bucket that contains `hi`.
        let hi_bucket = prefix_bucket(hi, prefix_bits);

        for bucket in lo_bucket..=hi_bucket {
            // One canonical primary vShard per bucket. All other vShards in
            // `[vshard_start, vshard_start + stride)` serve identical Hilbert
            // ranges; fanning out to all of them double-counts on aggregate.
            let vshard_start = bucket_to_vshard(bucket, prefix_bits);
            if vshard_start < total_shards {
                out.push(vshard_start);
            }
        }
    }

    out.sort_unstable();
    out.dedup();
    Ok(out)
}

// ── Internal helpers ──────────────────────────────────────────────────────

fn validate_prefix_bits(prefix_bits: u8) -> Result<()> {
    if prefix_bits == 0 || prefix_bits > 16 {
        return Err(ClusterError::Codec {
            detail: format!("array routing: prefix_bits must be 1-16, got {prefix_bits}"),
        });
    }
    Ok(())
}

/// Extract the top `prefix_bits` bits of a Hilbert prefix as a bucket index.
fn prefix_bucket(hilbert_prefix: u64, prefix_bits: u8) -> u32 {
    // Shift right so the top `prefix_bits` bits are in the low-order position.
    let shift = 64u8.saturating_sub(prefix_bits);
    (hilbert_prefix >> shift) as u32
}

/// Map a bucket index to the primary vShard in that bucket's range.
fn bucket_to_vshard(bucket: u32, prefix_bits: u8) -> u32 {
    let stride = vshard_stride(prefix_bits);
    bucket.saturating_mul(stride)
}

/// Number of vShards per Hilbert bucket.
///
/// With `P` prefix bits there are `2^P` buckets and `VSHARD_COUNT`
/// total vShards. Stride = `VSHARD_COUNT >> P`. When `P >= log2(VSHARD_COUNT)`
/// the stride is 1 (one vShard per bucket or less).
fn vshard_stride(prefix_bits: u8) -> u32 {
    // VSHARD_COUNT is a power of two (1024 = 2^10).
    // Right-shifting by prefix_bits gives the stride, floored at 1.
    let shifted = VSHARD_COUNT >> (prefix_bits as u32);
    shifted.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::VSHARD_COUNT;

    #[test]
    fn tile_routing_invalid_prefix_bits() {
        assert!(array_vshard_for_tile(0, 0).is_err());
        assert!(array_vshard_for_tile(0, 17).is_err());
    }

    #[test]
    fn tile_routing_top_bit_prefix1() {
        // With prefix_bits=1: stride=512. Bucket 0 → vshard 0, bucket 1 → vshard 512.
        let shard_low = array_vshard_for_tile(0x0000_0000_0000_0000, 1).unwrap();
        let shard_high = array_vshard_for_tile(0x8000_0000_0000_0000, 1).unwrap();
        assert_eq!(shard_low, 0);
        assert_eq!(shard_high, 512);
    }

    #[test]
    fn tile_routing_prefix10_is_direct() {
        // With prefix_bits=10: stride=1, bucket == vshard_id.
        // Top 10 bits of 0x0100_0000_0000_0000 = 0b0000_0000_01 = 1.
        let vshard = array_vshard_for_tile(0x0040_0000_0000_0000, 10).unwrap();
        // 0x0040... >> (64-10) = 0x0040... >> 54 = 1
        assert_eq!(vshard, 1);
    }

    #[test]
    fn tile_routing_prefix8_stride4() {
        // With prefix_bits=8: stride=4. Bucket 0 → vshard 0, bucket 1 → vshard 4.
        let shard0 = array_vshard_for_tile(0x0000_0000_0000_0000, 8).unwrap();
        // Top 8 bits of 0x0100... = 1.
        let shard1 = array_vshard_for_tile(0x0100_0000_0000_0000, 8).unwrap();
        assert_eq!(shard0, 0);
        assert_eq!(shard1, 4);
    }

    #[test]
    fn tile_routing_output_in_vshard_range() {
        // Every vshard returned must be < VSHARD_COUNT.
        for bits in 1u8..=10 {
            for &prefix in &[0u64, u64::MAX / 3, u64::MAX / 2, u64::MAX] {
                let vshard = array_vshard_for_tile(prefix, bits).unwrap();
                assert!(
                    vshard < VSHARD_COUNT,
                    "bits={bits} prefix={prefix:#x} → vshard={vshard} >= {VSHARD_COUNT}"
                );
            }
        }
    }

    #[test]
    fn slice_routing_zero_shards() {
        let shards = array_vshards_for_slice(&[(0, u64::MAX)], 8, 0).unwrap();
        assert!(shards.is_empty());
    }

    #[test]
    fn slice_routing_empty_ranges_returns_all() {
        // Empty range list = unbounded slice → one canonical primary per bucket.
        // With prefix_bits=8 (stride=4) and total_shards=4, only primary 0 fits.
        let shards = array_vshards_for_slice(&[], 8, 4).unwrap();
        assert_eq!(shards, vec![0]);
    }

    #[test]
    fn slice_routing_full_range_returns_all_active() {
        // Full range covers all 256 buckets → one primary per bucket.
        // total_shards=4 only fits primary 0.
        let shards = array_vshards_for_slice(&[(0, u64::MAX)], 8, 4).unwrap();
        assert_eq!(shards, vec![0]);
    }

    #[test]
    fn slice_routing_single_bucket_prefix8() {
        // Hilbert range [0, 0] covers only bucket 0 → primary vShard 0.
        let shards = array_vshards_for_slice(&[(0, 0)], 8, 16).unwrap();
        assert_eq!(shards, vec![0]);
    }

    #[test]
    fn slice_routing_two_adjacent_buckets() {
        // prefix_bits=8, stride=4. One canonical primary per bucket.
        // Range 1 → bucket 0 → primary vShard 0.
        // Range 2 → bucket 1 → primary vShard 4.
        let shards = array_vshards_for_slice(
            &[
                (0x0000_0000_0000_0000, 0x00FF_FFFF_FFFF_FFFF),
                (0x0100_0000_0000_0000, 0x01FF_FFFF_FFFF_FFFF),
            ],
            8,
            1024,
        )
        .unwrap();
        assert_eq!(shards, vec![0, 4]);
    }

    #[test]
    fn slice_routing_clamped_to_total_shards() {
        // With total_shards=2, only the primary for bucket 0 (= vShard 0) fits.
        let shards = array_vshards_for_slice(&[(0, u64::MAX)], 8, 2).unwrap();
        assert_eq!(shards, vec![0]);
    }

    #[test]
    fn slice_routing_dedup_overlapping_ranges() {
        // Two ranges that map to the same bucket → one canonical primary vShard.
        let shards = array_vshards_for_slice(
            &[
                (0x0000_0000_0000_0000, 0x0000_0000_0000_0001),
                (0x0000_0000_0000_0002, 0x0000_0000_0000_0003),
            ],
            8,
            16,
        )
        .unwrap();
        // Both ranges are in bucket 0 → primary vShard 0.
        assert_eq!(shards, vec![0]);
    }

    #[test]
    fn vshard_stride_values() {
        // VSHARD_COUNT=1024.
        assert_eq!(vshard_stride(1), 512);
        assert_eq!(vshard_stride(8), 4);
        assert_eq!(vshard_stride(10), 1);
        assert_eq!(vshard_stride(16), 1); // floored at 1
    }
}
