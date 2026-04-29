//! Writer for the NDVS v2 on-disk vector segment format.

use std::path::Path;

use super::format::{
    DTYPE_F32, FOOTER_SIZE, FORMAT_VERSION, HEADER_SIZE, MAGIC, VectorSegmentCodec, vec_pad,
};

/// Write a v2 NDVS segment file to `path`.
///
/// `surrogate_ids[i]` is the u64 surrogate for `vectors[i]`. The slice may be
/// empty, in which case all surrogate IDs are written as 0.
///
/// # Errors
///
/// Returns `std::io::Error` on any I/O failure or arithmetic overflow.
pub fn write_segment(
    path: &Path,
    dim: usize,
    vectors: &[&[f32]],
    surrogate_ids: &[u64],
) -> std::io::Result<()> {
    use std::io::Write as _;

    debug_assert!(
        surrogate_ids.is_empty() || surrogate_ids.len() == vectors.len(),
        "surrogate_ids length must match vectors length or be empty"
    );

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let count = vectors.len() as u64;

    let mut fd = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    // Header — 32 bytes.
    fd.write_all(&MAGIC)?;
    fd.write_all(&FORMAT_VERSION.to_le_bytes())?;
    fd.write_all(&0u16.to_le_bytes())?; // flags
    fd.write_all(&(dim as u32).to_le_bytes())?;
    fd.write_all(&count.to_le_bytes())?;
    fd.write_all(&[DTYPE_F32])?;
    fd.write_all(&[VectorSegmentCodec::None as u8])?;
    fd.write_all(&[0u8; 10])?; // reserved (10 bytes → header total 32, 8-byte aligned)

    // Vector data block — D × N × 4 bytes, row-major, no framing.
    let mut written_vec_bytes: usize = 0;
    for v in vectors {
        debug_assert_eq!(v.len(), dim, "vector dimension mismatch during write");
        let bytes: &[u8] =
            unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 4) };
        fd.write_all(bytes)?;
        written_vec_bytes += bytes.len();
    }

    // Pad to 8-byte alignment so the surrogate ID block is naturally aligned.
    let pad = vec_pad(written_vec_bytes);
    if pad > 0 {
        fd.write_all(&[0u8; 8][..pad])?;
    }

    // Surrogate ID block — N × 8 bytes.
    for i in 0..vectors.len() {
        let sid: u64 = surrogate_ids.get(i).copied().unwrap_or(0);
        fd.write_all(&sid.to_le_bytes())?;
    }

    fd.sync_all()?;
    drop(fd);

    // Compute CRC32C over the body (header + vector block + surrogate block).
    let vec_bytes = dim
        .checked_mul(vectors.len())
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "vector data size overflow")
        })?;
    let surrogate_bytes = vectors.len().checked_mul(8).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "surrogate block size overflow",
        )
    })?;
    let pad_bytes = vec_pad(vec_bytes);
    let body_len = HEADER_SIZE + vec_bytes + pad_bytes + surrogate_bytes;

    let data = std::fs::read(path)?;
    if data.len() != body_len {
        return Err(std::io::Error::other(format!(
            "unexpected file size after write: {} vs {body_len}",
            data.len()
        )));
    }
    let checksum = crc32c::crc32c(&data);

    // Append the footer (46 bytes).
    let mut fd = std::fs::OpenOptions::new().append(true).open(path)?;

    let mut created_by = [0u8; 32];
    let ver = env!("CARGO_PKG_VERSION").as_bytes();
    let copy_len = ver.len().min(31);
    created_by[..copy_len].copy_from_slice(&ver[..copy_len]);

    fd.write_all(&FORMAT_VERSION.to_le_bytes())?; // footer format_version [0..2]
    fd.write_all(&created_by)?; // created_by              [2..34]
    fd.write_all(&checksum.to_le_bytes())?; // checksum    [34..38]
    fd.write_all(&(FOOTER_SIZE as u32).to_le_bytes())?; // footer_size [38..42]
    fd.write_all(&MAGIC)?; // trailing magic               [42..46]
    fd.sync_all()?;

    Ok(())
}
