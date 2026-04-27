//! Vamana on-disk layout description.
//!
//! **No I/O code lives here.** This module describes the byte layout that
//! a future io_uring integration (in `nodedb-wal`) will read and write.
//! Offset arithmetic is provided so callers can compute SSD byte positions
//! for full-precision vector fetches without materialising any buffers.
//!
//! # File layout
//!
//! ```text
//! Offset  Size   Field
//! ------  ----   -----
//!      0     8   magic = b"NDBVAMAN"
//!      8     4   version: u32
//!     12     4   dim: u32
//!     16     4   r: u32  (max degree)
//!     20     4   alpha: f32
//!     24     8   num_nodes: u64
//!     32     8   entry: u64  (entry-point node index)
//!     40     8   adjacency_offset: u64  (byte offset to adjacency block)
//!     48     8   vectors_offset: u64    (byte offset to vectors block)
//!     56    …    reserved / padding to 64-byte header boundary
//!
//! --- adjacency block (mmap-resident, hot path) ---
//!     [u32 × r × num_nodes]  neighbors, zero-padded when degree < r
//!
//! --- vectors block (SSD-resident, io_uring fetch on demand) ---
//!     [f32 × dim × num_nodes]  full-precision FP32 vectors
//! ```
//!
//! The adjacency block fits in RAM alongside the compressed codec vectors.
//! The vectors block is fetched on demand via io_uring at a fixed, computable
//! offset (see `vector_offset`).

/// Magic bytes identifying a NodeDB Vamana index file.
pub const VAMANA_MAGIC: &[u8; 8] = b"NDBVAMAN";

/// Current on-disk format version.
pub const VAMANA_VERSION: u32 = 1;

/// Size of the fixed file header in bytes.
pub const HEADER_BYTES: u64 = 64;

/// Vamana on-disk layout descriptor.
///
/// Populated when opening or creating an index file; used to compute
/// byte offsets for io_uring scatter/gather reads.
#[derive(Debug, Clone, PartialEq)]
pub struct VamanaStorageLayout {
    /// Vector dimensionality.
    pub dim: u32,
    /// Maximum out-degree per node.
    pub r: u32,
    /// α-pruning factor (stored for validation when reopening the file).
    pub alpha: f32,
    /// Total number of nodes.
    pub num_nodes: u64,
    /// Internal index of the entry-point node.
    pub entry: u64,
    /// Byte offset of the adjacency block from the start of the file.
    pub adjacency_offset: u64,
    /// Byte offset of the vectors block from the start of the file.
    pub vectors_offset: u64,
}

impl VamanaStorageLayout {
    /// Build a layout descriptor for a freshly written index.
    ///
    /// Offsets are computed deterministically from `dim`, `r`, and
    /// `num_nodes`:
    ///
    /// * `adjacency_offset` = `HEADER_BYTES`
    /// * `vectors_offset`   = `adjacency_offset + r * 4 * num_nodes`
    pub fn new(dim: u32, r: u32, alpha: f32, num_nodes: u64, entry: u64) -> Self {
        let adjacency_offset = HEADER_BYTES;
        let adjacency_bytes = r as u64 * 4 * num_nodes; // u32 per slot
        let vectors_offset = adjacency_offset + adjacency_bytes;
        Self {
            dim,
            r,
            alpha,
            num_nodes,
            entry,
            adjacency_offset,
            vectors_offset,
        }
    }

    /// Total size of the adjacency block in bytes.
    pub fn adjacency_bytes(&self) -> u64 {
        self.r as u64 * 4 * self.num_nodes
    }

    /// Total size of the vectors block in bytes.
    pub fn vectors_bytes(&self) -> u64 {
        self.dim as u64 * 4 * self.num_nodes // f32 = 4 bytes
    }
}

/// Return the byte offset of the full-precision vector for node `idx`.
///
/// This is the offset to pass to an io_uring `IORING_OP_READ` or `pread`
/// call to fetch `dim * 4` bytes of FP32 data for the given node.
///
/// # Example
///
/// ```
/// use nodedb_vector::vamana::storage::{VamanaStorageLayout, vector_offset};
///
/// let layout = VamanaStorageLayout::new(4, 64, 1.2, 100, 0);
/// let off = vector_offset(&layout, 5);
/// // adjacency block: 64 * 4 * 100 = 25600 bytes
/// // header:          64 bytes
/// // vectors start:   64 + 25600 = 25664
/// // vector 5:        25664 + 5 * 4 * 4 = 25664 + 80 = 25744
/// assert_eq!(off, 25744);
/// ```
pub fn vector_offset(layout: &VamanaStorageLayout, idx: u64) -> u64 {
    layout.vectors_offset + idx * layout.dim as u64 * 4
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_offset_manual_computation() {
        // dim=4, r=64, num_nodes=100
        let layout = VamanaStorageLayout::new(4, 64, 1.2, 100, 0);

        // adjacency_offset = HEADER_BYTES = 64
        assert_eq!(layout.adjacency_offset, 64);

        // adjacency_bytes = 64 * 4 * 100 = 25_600
        assert_eq!(layout.adjacency_bytes(), 25_600);

        // vectors_offset = 64 + 25_600 = 25_664
        assert_eq!(layout.vectors_offset, 25_664);

        // vector_offset(idx=5) = 25_664 + 5 * 4 * 4 = 25_664 + 80 = 25_744
        assert_eq!(vector_offset(&layout, 5), 25_744);
    }

    #[test]
    fn vector_offset_idx_zero_equals_vectors_offset() {
        let layout = VamanaStorageLayout::new(8, 32, 1.2, 50, 0);
        assert_eq!(vector_offset(&layout, 0), layout.vectors_offset);
    }

    #[test]
    fn vectors_bytes_correct() {
        let layout = VamanaStorageLayout::new(128, 64, 1.2, 1000, 0);
        // 128 dims * 4 bytes * 1000 nodes = 512_000
        assert_eq!(layout.vectors_bytes(), 512_000);
    }

    #[test]
    fn magic_bytes_length() {
        assert_eq!(VAMANA_MAGIC.len(), 8);
    }
}
