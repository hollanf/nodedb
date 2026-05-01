//! On-disk format constants and shared types for the NDVS vector segment.

use std::sync::atomic::{AtomicU64, Ordering};

// ── Layout constants ──────────────────────────────────────────────────────────

pub(super) const MAGIC: [u8; 4] = *b"NDVS";
pub(super) const FORMAT_VERSION: u16 = 1;
pub(super) const DTYPE_F32: u8 = 0;

/// Header size in bytes (32). Padded to 8-byte alignment so the surrogate
/// ID block (u64) is naturally aligned regardless of vector dimension.
pub(super) const HEADER_SIZE: usize = 32;

/// Compute the 8-byte-aligned padding inserted after the vector data block
/// so the surrogate ID block lands on an 8-byte boundary.
///
/// Returns 0 when `vec_bytes` is already a multiple of 8, else 4.
#[inline]
pub(super) const fn vec_pad(vec_bytes: usize) -> usize {
    (8 - (vec_bytes % 8)) % 8
}

/// Footer size in bytes (46).
///
/// Layout:
/// ```text
/// [0..2]   format_version  (u16 LE)
/// [2..34]  created_by      (32-byte null-padded version string)
/// [34..38] checksum        (u32 LE CRC32C over header + data body)
/// [38..42] footer_size     (u32 LE, always 46)
/// [42..46] trailing_magic  (b"NDVS")
/// ```
pub(super) const FOOTER_SIZE: usize = 46;

// ── Codec slot ────────────────────────────────────────────────────────────────

/// At-rest compression codec for the vector data block.
///
/// Stored as a `u8` in the segment header (byte 21). Only `None` is supported
/// in v2. The decode site in the reader matches exhaustively so the right
/// hook for future compression codecs is obvious.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum VectorSegmentCodec {
    /// No compression — raw packed `[f32; D] × N`.
    None = 0,
}

impl VectorSegmentCodec {
    pub(super) fn from_u8(v: u8) -> std::io::Result<Self> {
        match v {
            0 => Ok(Self::None),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown VectorSegmentCodec byte {other}"),
            )),
        }
    }
}

// ── Drop-time page-cache policy ───────────────────────────────────────────────

/// Drop-time page-cache policy for a vector segment.
///
/// HNSW traversal touches a small fraction of a segment's pages. When the
/// segment is dropped we hint the kernel that the residual pages can be
/// evicted so they don't crowd hotter engines' working sets.
#[derive(Debug, Clone, Copy)]
pub struct VectorSegmentDropPolicy {
    dontneed_on_drop: bool,
}

impl VectorSegmentDropPolicy {
    pub const fn new(dontneed_on_drop: bool) -> Self {
        Self { dontneed_on_drop }
    }
    pub const fn keep_resident() -> Self {
        Self {
            dontneed_on_drop: false,
        }
    }
    pub const fn dontneed_on_drop(self) -> bool {
        self.dontneed_on_drop
    }
}

impl Default for VectorSegmentDropPolicy {
    fn default() -> Self {
        Self {
            dontneed_on_drop: true,
        }
    }
}

// ── Test hooks ────────────────────────────────────────────────────────────────

/// Module-scoped counters for observing madvise behaviour in tests.
///
/// These lightweight atomics are always compiled — the same counters are
/// useful to Event-Plane metrics, not just tests.
pub mod test_hooks {
    use super::{AtomicU64, Ordering};
    pub(crate) static DONTNEED_COUNT: AtomicU64 = AtomicU64::new(0);
    pub(crate) static RANDOM_COUNT: AtomicU64 = AtomicU64::new(0);

    pub fn dontneed_count() -> u64 {
        DONTNEED_COUNT.load(Ordering::Relaxed)
    }
    pub fn random_count() -> u64 {
        RANDOM_COUNT.load(Ordering::Relaxed)
    }
}
