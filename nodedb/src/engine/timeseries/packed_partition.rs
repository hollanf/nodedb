//! Single-file packed partition format with versioned binary footer.
//!
//! Combines all column files, schema, sparse index, and metadata into a
//! single immutable file for cold/S3 storage. The footer at the end of
//! the file contains exact byte ranges per column, enabling:
//!
//! - HTTP range requests for individual columns (no full-file download)
//! - Projection pushdown at the storage layer
//! - Zero-copy mmap of specific columns
//!
//! File layout:
//! ```text
//! ┌───────────────────────────────────────┐
//! │  Column 0 data (compressed bytes)     │
//! │  Column 1 data (compressed bytes)     │
//! │  ...                                  │
//! ├───────────────────────────────────────┤
//! │  Sparse index data                    │
//! ├───────────────────────────────────────┤
//! │  Footer body (msgpack PackedFooter)   │
//! ├───────────────────────────────────────┤
//! │  footer_len   : u32 LE   (4 bytes)    │
//! │  crc32c       : u32 LE   (4 bytes)    │  ← CRC over footer body
//! │  version      : u16 LE=1 (2 bytes)    │
//! │  magic "NDPK" : 4 bytes  (4 bytes)    │  ← always at EOF
//! └───────────────────────────────────────┘
//! ```
//!
//! Tail is 14 bytes total. Reader: read last 4 bytes for magic, version at
//! -6..-4, CRC at -10..-6, footer_len at -14..-10, then read body and
//! verify CRC before deserializing.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use nodedb_codec::ColumnCodec;
use nodedb_types::timeseries::PartitionMeta;

/// Magic bytes at the end of a packed partition file (at EOF).
pub const PACKED_MAGIC: [u8; 4] = *b"NDPK";
/// Format version written to the tail.
pub const PACKED_VERSION: u16 = 1;
/// Fixed byte size of the tail (footer_len + crc + version + magic).
pub const PACKED_TAIL_SIZE: usize = 14;

// ── Cursor helper ─────────────────────────────────────────────────────────────

struct TailCursor<'a> {
    data: &'a [u8],
    /// Current position measured from the *end* of the slice.
    from_end: usize,
}

impl<'a> TailCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, from_end: 0 }
    }

    /// Read `n` bytes backwards, returning them in logical order.
    fn read_back(&mut self, n: usize) -> Option<&'a [u8]> {
        let new_from_end = self.from_end + n;
        if new_from_end > self.data.len() {
            return None;
        }
        let end = self.data.len() - self.from_end;
        let start = end - n;
        self.from_end = new_from_end;
        Some(&self.data[start..end])
    }

    fn read_back_u32_le(&mut self) -> Option<u32> {
        let b = self.read_back(4)?;
        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_back_u16_le(&mut self) -> Option<u16> {
        let b = self.read_back(2)?;
        Some(u16::from_le_bytes([b[0], b[1]]))
    }

    /// Bytes consumed from the end so far (== current tail offset).
    fn consumed(&self) -> usize {
        self.from_end
    }
}

// ── Types ─────────────────────────────────────────────────────────────────────

/// Footer of a packed partition file.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PackedFooter {
    /// Per-column byte ranges: column_name → (offset, length).
    pub column_ranges: HashMap<String, (u64, u64)>,
    /// Sparse index byte range (offset, length).
    pub sparse_index_range: Option<(u64, u64)>,
    /// Schema: column names, types, codecs.
    pub schema: Vec<PackedColumnSchema>,
    /// Partition metadata.
    pub meta: PartitionMeta,
}

/// Schema entry for a column in the packed format.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PackedColumnSchema {
    pub name: String,
    pub col_type: String,
    pub codec: ColumnCodec,
}

/// Error type for packed partition operations.
#[derive(thiserror::Error, Debug)]
pub enum PackedError {
    #[error("packed partition I/O: {0}")]
    Io(String),
    #[error("packed partition corrupt: {0}")]
    Corrupt(String),
    #[error("unsupported packed partition format version: {version}")]
    UnsupportedVersion { version: u16 },
    #[error("packed partition footer CRC mismatch: stored={stored:#010x} calc={calc:#010x}")]
    InvalidFooterCrc { stored: u32, calc: u32 },
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Write a packed partition file from individual column files.
pub fn write_packed(
    output_path: &Path,
    columns: &[(String, Vec<u8>)], // (column_name, compressed_bytes)
    sparse_index: Option<&[u8]>,
    schema: &[PackedColumnSchema],
    meta: &PartitionMeta,
) -> Result<u64, PackedError> {
    let mut buf = Vec::new();
    let mut column_ranges = HashMap::new();

    // Write column data.
    for (name, data) in columns {
        let offset = buf.len() as u64;
        buf.extend_from_slice(data);
        column_ranges.insert(name.clone(), (offset, data.len() as u64));
    }

    // Write sparse index.
    let sparse_index_range = sparse_index.map(|idx_data| {
        let offset = buf.len() as u64;
        buf.extend_from_slice(idx_data);
        (offset, idx_data.len() as u64)
    });

    // Build footer body.
    let footer = PackedFooter {
        column_ranges,
        sparse_index_range,
        schema: schema.to_vec(),
        meta: meta.clone(),
    };
    let footer_body = zerompk::to_msgpack_vec(&footer)
        .map_err(|e| PackedError::Io(format!("footer encode: {e}")))?;

    // Append footer body.
    buf.extend_from_slice(&footer_body);
    let footer_len = footer_body.len() as u32;

    // CRC32C over footer body only.
    let crc = crc32c::crc32c(&footer_body);

    // Tail: footer_len (4B) | crc (4B) | version (2B) | magic (4B)
    buf.extend_from_slice(&footer_len.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&PACKED_VERSION.to_le_bytes());
    buf.extend_from_slice(&PACKED_MAGIC);

    let total_size = buf.len() as u64;
    std::fs::write(output_path, &buf)
        .map_err(|e| PackedError::Io(format!("write {}: {e}", output_path.display())))?;

    Ok(total_size)
}

// ── Reader ────────────────────────────────────────────────────────────────────

/// Read the footer from a packed partition file.
pub fn read_footer(file_path: &Path) -> Result<PackedFooter, PackedError> {
    let data = std::fs::read(file_path)
        .map_err(|e| PackedError::Io(format!("read {}: {e}", file_path.display())))?;
    read_footer_from_bytes(&data)
}

/// Read the footer from in-memory bytes (for testing or mmap'd files).
pub fn read_footer_from_bytes(data: &[u8]) -> Result<PackedFooter, PackedError> {
    if data.len() < PACKED_TAIL_SIZE {
        return Err(PackedError::Corrupt(format!(
            "file too small for packed tail: {} bytes",
            data.len()
        )));
    }

    let mut tail = TailCursor::new(data);

    // Read tail fields in reverse order (right → left).
    let magic = tail.read_back(4).unwrap();
    if magic != PACKED_MAGIC {
        return Err(PackedError::Corrupt(format!(
            "invalid magic: expected NDPK, got {magic:?}"
        )));
    }

    let version = tail.read_back_u16_le().unwrap();
    if version != PACKED_VERSION {
        return Err(PackedError::UnsupportedVersion { version });
    }

    let crc_stored = tail.read_back_u32_le().unwrap();
    let footer_len = tail.read_back_u32_le().unwrap() as usize;

    // tail.consumed() == PACKED_TAIL_SIZE at this point.
    let tail_offset = data.len() - tail.consumed();
    if footer_len > tail_offset {
        return Err(PackedError::Corrupt(format!(
            "footer_len {footer_len} exceeds available bytes {tail_offset}"
        )));
    }

    let body_start = tail_offset - footer_len;
    let footer_body = &data[body_start..body_start + footer_len];

    // Verify CRC over body.
    let crc_calc = crc32c::crc32c(footer_body);
    if crc_stored != crc_calc {
        return Err(PackedError::InvalidFooterCrc {
            stored: crc_stored,
            calc: crc_calc,
        });
    }

    zerompk::from_msgpack(footer_body)
        .map_err(|e| PackedError::Corrupt(format!("footer body decode: {e}")))
}

/// Read a single column's data from a packed file using byte range.
pub fn read_column(
    file_path: &Path,
    footer: &PackedFooter,
    column_name: &str,
) -> Result<Vec<u8>, PackedError> {
    let (offset, length) = footer
        .column_ranges
        .get(column_name)
        .ok_or_else(|| PackedError::Corrupt(format!("column '{column_name}' not in footer")))?;

    let data = std::fs::read(file_path).map_err(|e| PackedError::Io(format!("read: {e}")))?;

    let start = *offset as usize;
    let end = start + *length as usize;
    if end > data.len() {
        return Err(PackedError::Corrupt(
            "column range exceeds file size".into(),
        ));
    }

    Ok(data[start..end].to_vec())
}

/// Generate HTTP Range header values for fetching specific columns.
///
/// Returns `(column_name, "bytes=offset-end")` pairs for use with
/// S3 GetObject or HTTP range requests.
pub fn http_range_headers(footer: &PackedFooter, columns: &[&str]) -> Vec<(String, String)> {
    columns
        .iter()
        .filter_map(|&name| {
            footer.column_ranges.get(name).map(|&(offset, length)| {
                let end = offset + length - 1; // HTTP ranges are inclusive.
                (name.to_string(), format!("bytes={offset}-{end}"))
            })
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::PartitionState;
    use tempfile::TempDir;

    fn test_meta() -> PartitionMeta {
        PartitionMeta {
            min_ts: 1000,
            max_ts: 2000,
            row_count: 100,
            size_bytes: 0,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 86_400_000,
            last_flushed_wal_lsn: 0,
            column_stats: HashMap::new(),
            max_system_ts: 0,
        }
    }

    #[test]
    fn write_and_read_footer() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let columns = vec![
            ("timestamp".to_string(), vec![1u8, 2, 3, 4, 5]),
            ("value".to_string(), vec![10, 20, 30]),
        ];
        let schema = vec![
            PackedColumnSchema {
                name: "timestamp".into(),
                col_type: "timestamp".into(),
                codec: ColumnCodec::DeltaFastLanesLz4,
            },
            PackedColumnSchema {
                name: "value".into(),
                col_type: "float64".into(),
                codec: ColumnCodec::AlpFastLanesLz4,
            },
        ];

        let size = write_packed(&path, &columns, None, &schema, &test_meta()).unwrap();
        assert!(size > 0);

        let footer = read_footer(&path).unwrap();
        assert_eq!(footer.column_ranges.len(), 2);
        assert_eq!(footer.column_ranges["timestamp"], (0, 5));
        assert_eq!(footer.column_ranges["value"], (5, 3));
        assert_eq!(footer.meta.row_count, 100);
    }

    #[test]
    fn read_single_column() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let ts_data = vec![1u8, 2, 3, 4, 5];
        let val_data = vec![10u8, 20, 30];
        let columns = vec![
            ("timestamp".to_string(), ts_data.clone()),
            ("value".to_string(), val_data.clone()),
        ];

        write_packed(&path, &columns, None, &[], &test_meta()).unwrap();

        let footer = read_footer(&path).unwrap();
        let ts_bytes = read_column(&path, &footer, "timestamp").unwrap();
        assert_eq!(ts_bytes, ts_data);

        let val_bytes = read_column(&path, &footer, "value").unwrap();
        assert_eq!(val_bytes, val_data);
    }

    #[test]
    fn http_range_headers_for_projection() {
        let mut column_ranges = HashMap::new();
        column_ranges.insert("timestamp".to_string(), (0u64, 1000));
        column_ranges.insert("cpu".to_string(), (1000, 500));
        column_ranges.insert("host".to_string(), (1500, 200));

        let footer = PackedFooter {
            column_ranges,
            sparse_index_range: None,
            schema: vec![],
            meta: test_meta(),
        };

        let ranges = http_range_headers(&footer, &["cpu"]);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].0, "cpu");
        assert_eq!(ranges[0].1, "bytes=1000-1499");
    }

    #[test]
    fn with_sparse_index() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let sparse_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let columns = vec![("ts".to_string(), vec![1, 2, 3])];

        write_packed(&path, &columns, Some(&sparse_data), &[], &test_meta()).unwrap();

        let footer = read_footer(&path).unwrap();
        assert!(footer.sparse_index_range.is_some());
        let (offset, length) = footer.sparse_index_range.unwrap();
        assert_eq!(offset, 3); // After the 3-byte ts column.
        assert_eq!(length, 4);
    }

    #[test]
    fn invalid_magic() {
        let data = vec![0u8; 100]; // No magic at EOF.
        assert!(read_footer_from_bytes(&data).is_err());
    }

    #[test]
    fn projection_pushdown_skips_columns() {
        let mut ranges = HashMap::new();
        ranges.insert("ts".to_string(), (0u64, 10000));
        ranges.insert("cpu".to_string(), (10000, 5000));
        ranges.insert("mem".to_string(), (15000, 5000));
        ranges.insert("host".to_string(), (20000, 2000));

        let footer = PackedFooter {
            column_ranges: ranges,
            sparse_index_range: None,
            schema: vec![],
            meta: test_meta(),
        };

        let headers = http_range_headers(&footer, &["ts", "cpu"]);
        assert_eq!(headers.len(), 2);

        let fetched: u64 = headers
            .iter()
            .map(|(name, _)| footer.column_ranges[name].1)
            .sum();
        assert_eq!(fetched, 15000);
    }

    // ── G-04: packed-partition golden tests ────────────────────────────────────

    /// Verify tail layout byte by byte.
    #[test]
    fn packed_golden_tail_bytes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("golden.ndpk");
        let columns = vec![("ts".to_string(), vec![1u8, 2, 3])];
        write_packed(&path, &columns, None, &[], &test_meta()).unwrap();

        let data = std::fs::read(&path).unwrap();
        let n = data.len();

        // Last 4 bytes: magic
        assert_eq!(&data[n - 4..], b"NDPK");

        // Bytes -6..-4: version = 1 LE
        let version = u16::from_le_bytes([data[n - 6], data[n - 5]]);
        assert_eq!(version, PACKED_VERSION);

        // Bytes -10..-6: crc32c
        let crc_stored = u32::from_le_bytes([data[n - 10], data[n - 9], data[n - 8], data[n - 7]]);

        // Bytes -14..-10: footer_len
        let footer_len =
            u32::from_le_bytes([data[n - 14], data[n - 13], data[n - 12], data[n - 11]]) as usize;

        // Footer body is immediately before the tail.
        let body_end = n - PACKED_TAIL_SIZE;
        let body_start = body_end - footer_len;
        let footer_body = &data[body_start..body_end];

        // CRC must be internally consistent.
        let crc_calc = crc32c::crc32c(footer_body);
        assert_eq!(
            crc_stored, crc_calc,
            "golden: stored CRC does not match re-computed CRC over body"
        );

        // Body must deserialize correctly.
        let footer: PackedFooter = zerompk::from_msgpack(footer_body).unwrap();
        assert_eq!(footer.column_ranges["ts"], (0, 3));
    }

    /// Version != 1 must be rejected.
    #[test]
    fn packed_rejects_unsupported_version() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("v2.ndpk");
        let columns = vec![("ts".to_string(), vec![1u8])];
        write_packed(&path, &columns, None, &[], &test_meta()).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        let n = data.len();
        // Patch version bytes (-6..-4) to version = 2.
        let v2 = 2u16.to_le_bytes();
        data[n - 6] = v2[0];
        data[n - 5] = v2[1];

        let err = read_footer_from_bytes(&data).unwrap_err();
        assert!(
            matches!(err, PackedError::UnsupportedVersion { version: 2 }),
            "expected UnsupportedVersion {{version: 2}}, got {err:?}"
        );
    }

    /// Corrupt CRC must be rejected.
    #[test]
    fn packed_rejects_bad_crc() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad_crc.ndpk");
        let columns = vec![("ts".to_string(), vec![1u8])];
        write_packed(&path, &columns, None, &[], &test_meta()).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        let n = data.len();
        // Flip a byte in the CRC field at -10..-6.
        data[n - 10] ^= 0xFF;

        let err = read_footer_from_bytes(&data).unwrap_err();
        assert!(
            matches!(err, PackedError::InvalidFooterCrc { .. }),
            "expected InvalidFooterCrc, got {err:?}"
        );
    }
}
