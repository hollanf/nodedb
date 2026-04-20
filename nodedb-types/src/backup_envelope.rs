//! Backup envelope: framing for tenant backup bytes.
//!
//! All multi-byte integers are little-endian.
//!
//! ```text
//!   ┌─ HEADER ────────────────────────────────────────────────────┐
//!   │ magic       : 4 bytes  = b"NDBB"                            │
//!   │ version     : u8       = 1                                  │
//!   │ _reserved   : 3 bytes  = 0                                  │
//!   │ tenant_id   : u32                                           │
//!   │ src_vshards : u16   (source cluster's VSHARD_COUNT)         │
//!   │ _reserved   : 2 bytes  = 0                                  │
//!   │ hash_seed   : u64   (source cluster's hash seed, 0 today)   │
//!   │ watermark   : u64   (snapshot LSN; 0 if not captured)       │
//!   │ section_cnt : u16                                           │
//!   │ _reserved   : 6 bytes  = 0                                  │
//!   │ header_crc  : u32   (crc32c over the preceding 40 bytes)    │
//!   └─────────────────────────────────────────────────────────────┘
//!   ┌─ SECTION × section_cnt ─────────────────────────────────────┐
//!   │ origin_node : u64                                           │
//!   │ body_len    : u32   (≤ MAX_SECTION_BYTES)                   │
//!   │ body        : body_len bytes                                │
//!   │ body_crc    : u32   (crc32c over body)                      │
//!   └─────────────────────────────────────────────────────────────┘
//!   ┌─ TRAILER ───────────────────────────────────────────────────┐
//!   │ trailer_crc : u32   (crc32c over header bytes + every       │
//!   │                      section's framed bytes)                │
//!   └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! Total envelope size is bounded by `MAX_TOTAL_BYTES`. The decoder
//! short-circuits before allocating per body / per envelope, so a
//! caller-supplied byte stream cannot drive unbounded allocation.

use thiserror::Error;

pub const MAGIC: &[u8; 4] = b"NDBB";
pub const VERSION: u8 = 1;

/// Header is fixed-size — 44 bytes (40 framed + 4 crc).
pub const HEADER_LEN: usize = 44;
/// Per-section framing overhead: origin(8) + len(4) + crc(4).
pub const SECTION_OVERHEAD: usize = 16;
/// Trailing crc.
pub const TRAILER_LEN: usize = 4;

/// Default cap on total envelope size: 16 GiB. Tunable per call.
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// Default cap on a single section body: 16 GiB.
pub const DEFAULT_MAX_SECTION_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Sentinel `origin_node_id` values that mark sections carrying
/// metadata rather than per-node engine data. The envelope format is
/// backward-compatible: V1 readers that don't know about these
/// sentinels still decode the envelope (the bytes just aren't applied
/// at restore time), but the section CRCs still validate. Restore
/// handlers recognize the sentinel and route the body to the correct
/// catalog writer.
pub const SECTION_ORIGIN_CATALOG_ROWS: u64 = 0xFFFF_FFFF_FFFF_FFF0;
pub const SECTION_ORIGIN_SOURCE_TOMBSTONES: u64 = 0xFFFF_FFFF_FFFF_FFF1;

/// Single catalog-row entry in a catalog-rows section. The outer
/// container is `Vec<StoredCollectionBlob>` msgpack-encoded into the
/// section body. Bytes are the zerompk-encoded `StoredCollection`
/// from the `nodedb` crate — `nodedb-types` intentionally doesn't
/// depend on the `nodedb` catalog types, so the blob is opaque here.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredCollectionBlob {
    pub name: String,
    /// zerompk-encoded `StoredCollection`.
    pub bytes: Vec<u8>,
}

/// Single source-side tombstone entry. `purge_lsn` is the Origin WAL
/// LSN at which the hard-delete committed — restore uses it as a
/// per-collection replay barrier so rows older than the purge don't
/// resurrect.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct SourceTombstoneEntry {
    pub collection: String,
    pub purge_lsn: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("invalid backup format")]
    BadMagic,
    #[error("unsupported backup version: {0}")]
    UnsupportedVersion(u8),
    #[error("invalid backup format")]
    HeaderCrcMismatch,
    #[error("invalid backup format")]
    BodyCrcMismatch,
    #[error("invalid backup format")]
    TrailerCrcMismatch,
    #[error("backup truncated")]
    Truncated,
    #[error("backup tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch { expected: u32, actual: u32 },
    #[error("backup exceeds size cap of {cap} bytes")]
    OverSizeTotal { cap: u64 },
    #[error("backup section exceeds size cap of {cap} bytes")]
    OverSizeSection { cap: u64 },
    #[error("too many sections: {0}")]
    TooManySections(u16),
}

/// Header metadata captured at backup time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeMeta {
    pub tenant_id: u32,
    pub source_vshard_count: u16,
    pub hash_seed: u64,
    pub snapshot_watermark: u64,
}

/// One contiguous body produced by one origin node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    pub origin_node_id: u64,
    pub body: Vec<u8>,
}

/// Decoded envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub meta: EnvelopeMeta,
    pub sections: Vec<Section>,
}

/// Build an envelope by pushing sections one at a time, then `finalize()`.
pub struct EnvelopeWriter {
    meta: EnvelopeMeta,
    sections: Vec<Section>,
    max_total: u64,
    max_section: u64,
    framed_size: u64,
}

impl EnvelopeWriter {
    pub fn new(meta: EnvelopeMeta) -> Self {
        Self::with_caps(meta, DEFAULT_MAX_TOTAL_BYTES, DEFAULT_MAX_SECTION_BYTES)
    }

    pub fn with_caps(meta: EnvelopeMeta, max_total: u64, max_section: u64) -> Self {
        Self {
            meta,
            sections: Vec::new(),
            max_total,
            max_section,
            framed_size: HEADER_LEN as u64 + TRAILER_LEN as u64,
        }
    }

    pub fn push_section(
        &mut self,
        origin_node_id: u64,
        body: Vec<u8>,
    ) -> Result<(), EnvelopeError> {
        if body.len() as u64 > self.max_section {
            return Err(EnvelopeError::OverSizeSection {
                cap: self.max_section,
            });
        }
        let added = SECTION_OVERHEAD as u64 + body.len() as u64;
        if self.framed_size + added > self.max_total {
            return Err(EnvelopeError::OverSizeTotal {
                cap: self.max_total,
            });
        }
        if self.sections.len() >= u16::MAX as usize {
            return Err(EnvelopeError::TooManySections(u16::MAX));
        }
        self.framed_size += added;
        self.sections.push(Section {
            origin_node_id,
            body,
        });
        Ok(())
    }

    pub fn finalize(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.framed_size as usize);
        write_header(&mut out, &self.meta, self.sections.len() as u16);
        for section in &self.sections {
            write_section(&mut out, section);
        }
        // Trailer crc covers header bytes + every section's framed bytes.
        let trailer_crc = crc32c::crc32c(&out);
        out.extend_from_slice(&trailer_crc.to_le_bytes());
        out
    }
}

fn write_header(out: &mut Vec<u8>, meta: &EnvelopeMeta, section_count: u16) {
    let start = out.len();
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&[0u8; 3]);
    out.extend_from_slice(&meta.tenant_id.to_le_bytes());
    out.extend_from_slice(&meta.source_vshard_count.to_le_bytes());
    out.extend_from_slice(&[0u8; 2]);
    out.extend_from_slice(&meta.hash_seed.to_le_bytes());
    out.extend_from_slice(&meta.snapshot_watermark.to_le_bytes());
    out.extend_from_slice(&section_count.to_le_bytes());
    out.extend_from_slice(&[0u8; 6]);
    let header_crc = crc32c::crc32c(&out[start..]);
    out.extend_from_slice(&header_crc.to_le_bytes());
}

fn write_section(out: &mut Vec<u8>, section: &Section) {
    out.extend_from_slice(&section.origin_node_id.to_le_bytes());
    out.extend_from_slice(&(section.body.len() as u32).to_le_bytes());
    out.extend_from_slice(&section.body);
    let body_crc = crc32c::crc32c(&section.body);
    out.extend_from_slice(&body_crc.to_le_bytes());
}

/// Parse and fully validate an envelope.
pub fn parse(bytes: &[u8], max_total: u64) -> Result<Envelope, EnvelopeError> {
    if bytes.len() as u64 > max_total {
        return Err(EnvelopeError::OverSizeTotal { cap: max_total });
    }
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(EnvelopeError::Truncated);
    }

    // Header.
    let header_bytes = &bytes[..HEADER_LEN];
    if &header_bytes[0..4] != MAGIC {
        return Err(EnvelopeError::BadMagic);
    }
    let version = header_bytes[4];
    if version != VERSION {
        return Err(EnvelopeError::UnsupportedVersion(version));
    }
    let claimed_header_crc = u32::from_le_bytes(read4(&header_bytes[40..44]));
    let actual_header_crc = crc32c::crc32c(&header_bytes[..40]);
    if claimed_header_crc != actual_header_crc {
        return Err(EnvelopeError::HeaderCrcMismatch);
    }

    let meta = EnvelopeMeta {
        tenant_id: u32::from_le_bytes(read4(&header_bytes[8..12])),
        source_vshard_count: u16::from_le_bytes(read2(&header_bytes[12..14])),
        hash_seed: u64::from_le_bytes(read8(&header_bytes[16..24])),
        snapshot_watermark: u64::from_le_bytes(read8(&header_bytes[24..32])),
    };
    let section_count = u16::from_le_bytes(read2(&header_bytes[32..34]));

    // Trailer position: tail 4 bytes.
    let trailer_start = bytes.len() - TRAILER_LEN;
    let claimed_trailer_crc = u32::from_le_bytes(read4(&bytes[trailer_start..]));
    let actual_trailer_crc = crc32c::crc32c(&bytes[..trailer_start]);
    if claimed_trailer_crc != actual_trailer_crc {
        return Err(EnvelopeError::TrailerCrcMismatch);
    }

    // Sections live between header and trailer.
    let mut cursor = HEADER_LEN;
    let mut sections = Vec::with_capacity(section_count as usize);
    for _ in 0..section_count {
        if cursor + SECTION_OVERHEAD > trailer_start {
            return Err(EnvelopeError::Truncated);
        }
        let origin_node_id = u64::from_le_bytes(read8(&bytes[cursor..cursor + 8]));
        let body_len = u32::from_le_bytes(read4(&bytes[cursor + 8..cursor + 12])) as usize;
        let body_start = cursor + 12;
        let body_end = body_start + body_len;
        let crc_end = body_end + 4;
        if crc_end > trailer_start {
            return Err(EnvelopeError::Truncated);
        }
        let body = bytes[body_start..body_end].to_vec();
        let claimed_body_crc = u32::from_le_bytes(read4(&bytes[body_end..crc_end]));
        if crc32c::crc32c(&body) != claimed_body_crc {
            return Err(EnvelopeError::BodyCrcMismatch);
        }
        sections.push(Section {
            origin_node_id,
            body,
        });
        cursor = crc_end;
    }
    if cursor != trailer_start {
        return Err(EnvelopeError::Truncated);
    }

    Ok(Envelope { meta, sections })
}

fn read2(s: &[u8]) -> [u8; 2] {
    [s[0], s[1]]
}
fn read4(s: &[u8]) -> [u8; 4] {
    [s[0], s[1], s[2], s[3]]
}
fn read8(s: &[u8]) -> [u8; 8] {
    [s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> EnvelopeMeta {
        EnvelopeMeta {
            tenant_id: 42,
            source_vshard_count: 1024,
            hash_seed: 0,
            snapshot_watermark: 12345,
        }
    }

    #[test]
    fn empty_envelope_roundtrips() {
        let bytes = EnvelopeWriter::new(meta()).finalize();
        let env = parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap();
        assert_eq!(env.meta, meta());
        assert!(env.sections.is_empty());
    }

    #[test]
    fn multi_section_roundtrips() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"one".to_vec()).unwrap();
        w.push_section(2, b"two-payload".to_vec()).unwrap();
        w.push_section(3, vec![]).unwrap();
        let bytes = w.finalize();

        let env = parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap();
        assert_eq!(env.sections.len(), 3);
        assert_eq!(env.sections[0].origin_node_id, 1);
        assert_eq!(env.sections[0].body, b"one");
        assert_eq!(env.sections[1].origin_node_id, 2);
        assert_eq!(env.sections[1].body, b"two-payload");
        assert_eq!(env.sections[2].body, b"");
    }

    #[test]
    fn rejects_short_input() {
        assert_eq!(
            parse(b"NDBB", DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::Truncated)
        );
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = EnvelopeWriter::new(meta()).finalize();
        bytes[0] = b'X';
        // Header CRC will also fail; ensure magic check trips first.
        match parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap_err() {
            EnvelopeError::BadMagic => {}
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut bytes = EnvelopeWriter::new(meta()).finalize();
        bytes[4] = 99;
        // Header CRC will mismatch, but version is checked first.
        match parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap_err() {
            EnvelopeError::UnsupportedVersion(99) => {}
            other => panic!("expected UnsupportedVersion(99), got {other:?}"),
        }
    }

    #[test]
    fn rejects_header_crc_corruption() {
        let mut bytes = EnvelopeWriter::new(meta()).finalize();
        bytes[8] ^= 0xFF; // mutate tenant_id, leave header crc stale
        assert_eq!(
            parse(&bytes, DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::HeaderCrcMismatch)
        );
    }

    #[test]
    fn rejects_body_crc_corruption() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(7, b"hello".to_vec()).unwrap();
        let mut bytes = w.finalize();
        // Body sits at HEADER_LEN + 8 (origin) + 4 (len) = HEADER_LEN+12.
        let body_off = HEADER_LEN + 12;
        bytes[body_off] ^= 0xFF;
        // Trailer CRC will fail before body CRC is even checked. Recompute
        // trailer to isolate body-CRC enforcement.
        let trailer_off = bytes.len() - TRAILER_LEN;
        let new_trailer = crc32c::crc32c(&bytes[..trailer_off]);
        bytes[trailer_off..].copy_from_slice(&new_trailer.to_le_bytes());
        assert_eq!(
            parse(&bytes, DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::BodyCrcMismatch)
        );
    }

    #[test]
    fn rejects_trailer_crc_corruption() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(7, b"x".to_vec()).unwrap();
        let mut bytes = w.finalize();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert_eq!(
            parse(&bytes, DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::TrailerCrcMismatch)
        );
    }

    #[test]
    fn rejects_oversized_total() {
        let mut w = EnvelopeWriter::with_caps(meta(), 64, DEFAULT_MAX_SECTION_BYTES);
        let err = w.push_section(1, vec![0u8; 1024]).unwrap_err();
        assert!(matches!(err, EnvelopeError::OverSizeTotal { .. }));
    }

    #[test]
    fn rejects_oversized_section_at_write() {
        let mut w = EnvelopeWriter::with_caps(meta(), DEFAULT_MAX_TOTAL_BYTES, 8);
        let err = w.push_section(1, vec![0u8; 9]).unwrap_err();
        assert!(matches!(err, EnvelopeError::OverSizeSection { .. }));
    }

    #[test]
    fn rejects_oversized_total_at_parse() {
        let bytes = EnvelopeWriter::new(meta()).finalize();
        assert!(matches!(
            parse(&bytes, 4),
            Err(EnvelopeError::OverSizeTotal { .. })
        ));
    }

    #[test]
    fn truncated_section_body() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"hello world".to_vec()).unwrap();
        let bytes = w.finalize();
        // Lop the last 8 bytes; trailer crc will fail and parse returns
        // either TrailerCrcMismatch or Truncated. Either is a sound rejection.
        let truncated = &bytes[..bytes.len() - 8];
        assert!(parse(truncated, DEFAULT_MAX_TOTAL_BYTES).is_err());
    }
}
