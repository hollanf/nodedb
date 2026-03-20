//! Memory-mapped vector segment for L1 NVMe tiering.
//!
//! Stores FP32 vectors contiguously in a file, memory-mapped for read access.
//! Used when the memory governor's vector budget is exhausted — vectors are
//! written to NVMe instead of kept in RAM. The HNSW graph structure and SQ8
//! quantized data remain in RAM (L0) for traversal; only the full-precision
//! FP32 vectors used for reranking live on NVMe.
//!
//! Layout: `[dim:u32][count:u32][v0_f0..v0_fD][v1_f0..v1_fD]...]`
//! Header: 8 bytes. Each vector: `dim * 4` bytes.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

/// Memory-mapped vector segment file.
///
/// Not `Send` or `Sync` — owned by a single Data Plane core.
pub struct MmapVectorSegment {
    /// Path to the segment file.
    path: PathBuf,
    /// File handle (kept open for the mmap lifetime).
    _fd: std::fs::File,
    /// mmap base pointer.
    base: *const u8,
    /// Total mmap size in bytes.
    mmap_size: usize,
    /// Vector dimensionality.
    dim: usize,
    /// Number of vectors stored.
    count: usize,
    /// Offset in bytes to the first vector (after header).
    data_offset: usize,
}

/// Header size: `dim (u32) + count (u32) = 8 bytes`.
const HEADER_SIZE: usize = 8;

impl MmapVectorSegment {
    /// Create a new segment file and write vectors to it.
    ///
    /// Vectors are written contiguously as FP32. The file is then
    /// memory-mapped read-only for subsequent access.
    pub fn create(path: &Path, dim: usize, vectors: &[&[f32]]) -> std::io::Result<Self> {
        use std::io::Write;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let count = vectors.len();

        // Write header + vectors to file.
        let mut fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Header: dim (u32 LE) + count (u32 LE).
        fd.write_all(&(dim as u32).to_le_bytes())?;
        fd.write_all(&(count as u32).to_le_bytes())?;

        // Vectors: contiguous FP32 little-endian.
        for v in vectors {
            debug_assert_eq!(v.len(), dim);
            // SAFETY: &[f32] to &[u8] reinterpretation is safe on LE platforms.
            // Rust guarantees f32 is IEEE 754, and we're on a LE machine (x86_64).
            let bytes: &[u8] =
                unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 4) };
            fd.write_all(bytes)?;
        }
        fd.sync_all()?;

        // Re-open as read-only and mmap.
        drop(fd);
        Self::open(path)
    }

    /// Open an existing segment file and memory-map it.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let fd = std::fs::OpenOptions::new().read(true).open(path)?;

        let file_size = fd.metadata()?.len() as usize;
        if file_size < HEADER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mmap vector segment too small for header",
            ));
        }

        // SAFETY: file is open and readable, size is validated.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_size,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd.as_raw_fd(),
                0,
            )
        };

        if base == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        let base = base as *const u8;

        // Read header.
        let dim = unsafe {
            let ptr = base as *const u32;
            u32::from_le(*ptr) as usize
        };
        let count = unsafe {
            let ptr = base.add(4) as *const u32;
            u32::from_le(*ptr) as usize
        };

        // Validate file size.
        let expected = HEADER_SIZE + count * dim * 4;
        if file_size < expected {
            unsafe {
                libc::munmap(base as *mut libc::c_void, file_size);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("mmap segment truncated: expected {expected} bytes, got {file_size}"),
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            _fd: fd,
            base,
            mmap_size: file_size,
            dim,
            count,
            data_offset: HEADER_SIZE,
        })
    }

    /// Get a vector by ID. Returns a slice into the mmap'd region.
    ///
    /// This may trigger a page fault if the page isn't resident — the
    /// kernel fetches it from NVMe transparently. On TPC cores, major
    /// faults block the core thread until the I/O completes.
    #[inline]
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        let idx = id as usize;
        if idx >= self.count {
            return None;
        }
        let offset = self.data_offset + idx * self.dim * 4;
        // SAFETY: bounds checked above, file validated on open.
        unsafe {
            let ptr = self.base.add(offset) as *const f32;
            Some(std::slice::from_raw_parts(ptr, self.dim))
        }
    }

    /// Prefetch a vector's page into memory via `madvise(MADV_WILLNEED)`.
    ///
    /// Called during BFS planning or before a batch rerank to avoid
    /// synchronous page faults on the TPC core.
    pub fn prefetch(&self, id: u32) {
        let idx = id as usize;
        if idx >= self.count {
            return;
        }
        let offset = self.data_offset + idx * self.dim * 4;
        let page_start = offset & !(4095); // Align to page boundary.
        let len = (self.dim * 4 + 4095) & !(4095); // Round up to page.
        unsafe {
            libc::madvise(
                self.base.add(page_start) as *mut libc::c_void,
                len,
                libc::MADV_WILLNEED,
            );
        }
    }

    /// Prefetch a batch of vector IDs.
    pub fn prefetch_batch(&self, ids: &[u32]) {
        for &id in ids {
            self.prefetch(id);
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Size of the mmap'd region in bytes.
    pub fn mmap_bytes(&self) -> usize {
        self.mmap_size
    }
}

impl Drop for MmapVectorSegment {
    fn drop(&mut self) {
        if !self.base.is_null() && self.mmap_size > 0 {
            unsafe {
                libc::munmap(self.base as *mut libc::c_void, self.mmap_size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vseg");

        let v0: Vec<f32> = vec![1.0, 2.0, 3.0];
        let v1: Vec<f32> = vec![4.0, 5.0, 6.0];
        let v2: Vec<f32> = vec![7.0, 8.0, 9.0];

        let seg = MmapVectorSegment::create(&path, 3, &[&v0, &v1, &v2]).unwrap();

        assert_eq!(seg.dim(), 3);
        assert_eq!(seg.count(), 3);

        assert_eq!(seg.get_vector(0).unwrap(), &[1.0, 2.0, 3.0]);
        assert_eq!(seg.get_vector(1).unwrap(), &[4.0, 5.0, 6.0]);
        assert_eq!(seg.get_vector(2).unwrap(), &[7.0, 8.0, 9.0]);
        assert!(seg.get_vector(3).is_none());
    }

    #[test]
    fn reopen_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.vseg");

        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| vec![i as f32, (i as f32).sin(), (i as f32).cos()])
            .collect();
        let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();

        MmapVectorSegment::create(&path, 3, &refs).unwrap();

        // Reopen from disk.
        let seg = MmapVectorSegment::open(&path).unwrap();
        assert_eq!(seg.count(), 100);
        assert_eq!(seg.dim(), 3);

        for (i, v) in vectors.iter().enumerate() {
            let loaded = seg.get_vector(i as u32).unwrap();
            assert_eq!(loaded, v.as_slice(), "mismatch at vector {i}");
        }
    }

    #[test]
    fn prefetch_does_not_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prefetch.vseg");

        let v: Vec<f32> = vec![1.0; 768]; // Typical embedding size.
        let seg = MmapVectorSegment::create(&path, 768, &[&v]).unwrap();

        // Should not crash even for out-of-range IDs.
        seg.prefetch(0);
        seg.prefetch(999);
    }

    #[test]
    fn empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.vseg");

        let seg = MmapVectorSegment::create(&path, 3, &[]).unwrap();
        assert_eq!(seg.count(), 0);
        assert!(seg.get_vector(0).is_none());
    }
}
