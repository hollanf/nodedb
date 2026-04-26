//! Immutable segment file format for the FTS LSM engine.
//!
//! Layout:
//! ```text
//! [magic: 4 bytes "FTSS"]
//! [version: u16 LE]
//! [num_terms: u32 LE]
//! [term_dict_offset: u64 LE]  — byte offset to term dictionary
//! [posting_data_offset: u64 LE]
//! [... posting block data ...]
//! [... term dictionary entries ...]
//! [footer_checksum: u32 LE]  — CRC32C of everything before this
//! ```
//!
//! Term dictionary entry:
//! ```text
//! [term_len: u16 LE][term_bytes][posting_offset: u64 LE][posting_len: u32 LE][df: u32 LE]
//! ```

/// Segment file magic bytes.
pub const MAGIC: &[u8; 4] = b"FTSS";

/// Current segment format version.
pub const VERSION: u16 = 2;

/// Size of the fixed header (magic + version + num_terms + offsets).
pub const HEADER_SIZE: usize = 4 + 2 + 4 + 8 + 8; // 26 bytes

/// Segment header parsed from raw bytes.
#[derive(Debug, Clone, Copy)]
pub struct SegmentHeader {
    pub version: u16,
    pub num_terms: u32,
    pub term_dict_offset: u64,
    pub posting_data_offset: u64,
}

/// A term dictionary entry pointing into the posting data.
#[derive(Debug, Clone)]
pub struct TermDictEntry {
    pub term: String,
    /// Byte offset into the posting data section.
    pub posting_offset: u64,
    /// Length of the posting data in bytes.
    pub posting_len: u32,
    /// Document frequency (number of postings for this term).
    pub df: u32,
}

/// Write the segment header.
pub fn write_header(
    buf: &mut Vec<u8>,
    num_terms: u32,
    term_dict_offset: u64,
    posting_data_offset: u64,
) {
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&num_terms.to_le_bytes());
    buf.extend_from_slice(&term_dict_offset.to_le_bytes());
    buf.extend_from_slice(&posting_data_offset.to_le_bytes());
}

/// Parse the segment header. Returns `None` if magic/version mismatch.
pub fn parse_header(buf: &[u8]) -> Option<SegmentHeader> {
    if buf.len() < HEADER_SIZE {
        return None;
    }
    if &buf[0..4] != MAGIC {
        return None;
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != VERSION {
        tracing::warn!(
            segment_version = version,
            current_version = VERSION,
            "FTS segment format v{version} is no longer supported; rebuild the FTS index"
        );
        return None;
    }
    let num_terms = u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]);
    let term_dict_offset = u64::from_le_bytes(buf[10..18].try_into().ok()?);
    let posting_data_offset = u64::from_le_bytes(buf[18..26].try_into().ok()?);

    Some(SegmentHeader {
        version,
        num_terms,
        term_dict_offset,
        posting_data_offset,
    })
}

/// Write a term dictionary entry.
pub fn write_term_entry(buf: &mut Vec<u8>, entry: &TermDictEntry) {
    buf.extend_from_slice(&(entry.term.len() as u16).to_le_bytes());
    buf.extend_from_slice(entry.term.as_bytes());
    buf.extend_from_slice(&entry.posting_offset.to_le_bytes());
    buf.extend_from_slice(&entry.posting_len.to_le_bytes());
    buf.extend_from_slice(&entry.df.to_le_bytes());
}

/// Parse term dictionary entries from the term dict section.
pub fn parse_term_dict(buf: &[u8], num_terms: u32) -> Option<Vec<TermDictEntry>> {
    let mut entries = Vec::with_capacity(num_terms as usize);
    let mut pos = 0;
    for _ in 0..num_terms {
        if pos + 2 > buf.len() {
            return None;
        }
        let term_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if pos + term_len + 16 > buf.len() {
            return None;
        }
        let term = std::str::from_utf8(&buf[pos..pos + term_len])
            .ok()?
            .to_string();
        pos += term_len;
        let posting_offset = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let posting_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let df = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        entries.push(TermDictEntry {
            term,
            posting_offset,
            posting_len,
            df,
        });
    }
    Some(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let mut buf = Vec::new();
        write_header(&mut buf, 42, 100, 26);
        let header = parse_header(&buf).unwrap();
        assert_eq!(header.num_terms, 42);
        assert_eq!(header.term_dict_offset, 100);
        assert_eq!(header.posting_data_offset, 26);
    }

    #[test]
    fn term_dict_roundtrip() {
        let entries = vec![
            TermDictEntry {
                term: "hello".into(),
                posting_offset: 0,
                posting_len: 50,
                df: 10,
            },
            TermDictEntry {
                term: "world".into(),
                posting_offset: 50,
                posting_len: 30,
                df: 5,
            },
        ];
        let mut buf = Vec::new();
        for e in &entries {
            write_term_entry(&mut buf, e);
        }
        let parsed = parse_term_dict(&buf, 2).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].term, "hello");
        assert_eq!(parsed[1].posting_len, 30);
    }

    #[test]
    fn bad_magic() {
        let buf = vec![0u8; HEADER_SIZE];
        assert!(parse_header(&buf).is_none());
    }
}
