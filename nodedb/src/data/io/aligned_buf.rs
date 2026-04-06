//! Page-aligned buffer for O_DIRECT I/O.
//!
//! O_DIRECT requires buffer address, offset, and length to be aligned to
//! the logical block size (typically 4096 bytes). This module provides a
//! reusable aligned buffer that satisfies these constraints.

/// Default alignment for O_DIRECT I/O (4 KiB = NVMe logical block size).
pub const ALIGNMENT: usize = 4096;

/// A page-aligned byte buffer suitable for O_DIRECT reads.
///
/// The buffer address is guaranteed aligned to [`ALIGNMENT`]. Capacity is
/// rounded up to the next alignment boundary. Not `Send` — owned by a
/// single Data Plane core.
pub struct AlignedBuf {
    ptr: *mut u8,
    capacity: usize,
    layout: std::alloc::Layout,
}

impl AlignedBuf {
    /// Allocate a new aligned buffer with at least `min_capacity` bytes.
    pub fn new(min_capacity: usize) -> std::io::Result<Self> {
        let capacity = round_up(min_capacity.max(ALIGNMENT), ALIGNMENT);
        let layout = std::alloc::Layout::from_size_align(capacity, ALIGNMENT)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "aligned buffer allocation failed",
            ));
        }

        Ok(Self {
            ptr,
            capacity,
            layout,
        })
    }

    /// Mutable raw pointer to the buffer start (for io_uring SQE submission).
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Read-only pointer.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// View the first `len` bytes as a slice.
    ///
    /// # Safety
    /// Caller must ensure `len <= capacity` and the bytes have been written.
    #[inline]
    pub unsafe fn as_slice(&self, len: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, len) }
    }

    /// Copy the first `len` bytes into an owned Vec.
    ///
    /// # Safety
    /// Caller must ensure `len <= capacity` and the bytes have been written.
    pub unsafe fn to_vec(&self, len: usize) -> Vec<u8> {
        unsafe { self.as_slice(len).to_vec() }
    }

    /// Total capacity in bytes (always aligned).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { std::alloc::dealloc(self.ptr, self.layout) };
        }
    }
}

/// Round `value` up to the next multiple of `align` (power of two).
#[inline]
pub(super) const fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_is_aligned() {
        let buf = AlignedBuf::new(1).unwrap();
        assert_eq!(buf.as_ptr() as usize % ALIGNMENT, 0);
        assert!(buf.capacity() >= ALIGNMENT);
    }

    #[test]
    fn capacity_rounds_up() {
        let buf = AlignedBuf::new(5000).unwrap();
        assert_eq!(buf.capacity(), 8192); // 5000 rounds up to 8192
    }

    #[test]
    fn write_and_read() {
        let mut buf = AlignedBuf::new(4096).unwrap();
        let data = b"hello io_uring";
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf.as_mut_ptr(), data.len());
            let slice = buf.as_slice(data.len());
            assert_eq!(slice, data);
        }
    }

    #[test]
    fn to_vec_copies() {
        let mut buf = AlignedBuf::new(4096).unwrap();
        let data = b"test data";
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf.as_mut_ptr(), data.len());
            let v = buf.to_vec(data.len());
            assert_eq!(v, data);
        }
    }
}
