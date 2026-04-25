//! Segment writer: flushes a memtable to an immutable on-disk segment.
//!
//! Produces a byte buffer containing: sorted term dictionary,
//! compressed 128-doc posting blocks per term, and a header/footer.

use std::collections::HashMap;

use crate::block::{CompactPosting, PostingBlock, into_blocks};

use super::format::{self, TermDictEntry};

/// Flush a memtable's postings into an immutable segment byte buffer.
///
/// `term_postings` is the drained HashMap from `Memtable::drain()`.
/// Returns the serialized segment bytes.
pub fn flush_to_segment(term_postings: HashMap<String, Vec<CompactPosting>>) -> Vec<u8> {
    // Sort terms for binary-searchable term dictionary.
    let mut sorted_terms: Vec<(String, Vec<CompactPosting>)> = term_postings.into_iter().collect();
    sorted_terms.sort_by(|(a, _), (b, _)| a.cmp(b));

    // Phase 1: Encode posting blocks for each term, collect byte offsets.
    let mut posting_data = Vec::new();
    let mut dict_entries = Vec::with_capacity(sorted_terms.len());

    for (term, postings) in &sorted_terms {
        let offset = posting_data.len() as u64;
        let df = postings.len() as u32;

        // Split into 128-doc blocks and serialize each.
        let blocks = into_blocks(postings.clone());
        let mut term_bytes = Vec::new();

        // Write number of blocks.
        term_bytes.extend_from_slice(&(blocks.len() as u32).to_le_bytes());

        for block in &blocks {
            let block_bytes = block.to_bytes();
            term_bytes.extend_from_slice(&(block_bytes.len() as u32).to_le_bytes());
            term_bytes.extend_from_slice(&block_bytes);
        }

        let posting_len = term_bytes.len() as u32;
        posting_data.extend_from_slice(&term_bytes);

        dict_entries.push(TermDictEntry {
            term: term.clone(),
            posting_offset: offset,
            posting_len,
            df,
        });
    }

    // Phase 2: Build the segment buffer.
    let posting_data_offset = format::HEADER_SIZE as u64;
    let term_dict_offset = posting_data_offset + posting_data.len() as u64;

    let mut buf = Vec::new();

    // Header (will be written with correct offsets).
    format::write_header(
        &mut buf,
        dict_entries.len() as u32,
        term_dict_offset,
        posting_data_offset,
    );

    // Posting data.
    buf.extend_from_slice(&posting_data);

    // Term dictionary.
    for entry in &dict_entries {
        format::write_term_entry(&mut buf, entry);
    }

    buf
}

/// Build a segment from pre-sorted, pre-blocked posting data.
///
/// Used by compaction and parallel build where blocks are already formed.
pub fn build_from_blocks(term_blocks: &[(String, Vec<PostingBlock>)]) -> Vec<u8> {
    let mut posting_data = Vec::new();
    let mut dict_entries = Vec::with_capacity(term_blocks.len());

    for (term, blocks) in term_blocks {
        let offset = posting_data.len() as u64;
        let df: u32 = blocks.iter().map(|b| b.len() as u32).sum();

        let mut term_bytes = Vec::new();
        term_bytes.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        for block in blocks {
            let block_bytes = block.to_bytes();
            term_bytes.extend_from_slice(&(block_bytes.len() as u32).to_le_bytes());
            term_bytes.extend_from_slice(&block_bytes);
        }

        let posting_len = term_bytes.len() as u32;
        posting_data.extend_from_slice(&term_bytes);

        dict_entries.push(TermDictEntry {
            term: term.clone(),
            posting_offset: offset,
            posting_len,
            df,
        });
    }

    let posting_data_offset = format::HEADER_SIZE as u64;
    let term_dict_offset = posting_data_offset + posting_data.len() as u64;

    let mut buf = Vec::new();
    format::write_header(
        &mut buf,
        dict_entries.len() as u32,
        term_dict_offset,
        posting_data_offset,
    );
    buf.extend_from_slice(&posting_data);
    for entry in &dict_entries {
        format::write_term_entry(&mut buf, entry);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::smallfloat;

    fn make_postings() -> HashMap<String, Vec<CompactPosting>> {
        let mut map = HashMap::new();
        map.insert(
            "hello".to_string(),
            vec![
                CompactPosting {
                    doc_id: nodedb_types::Surrogate(0),
                    term_freq: 2,
                    fieldnorm: smallfloat::encode(100),
                    positions: vec![0, 5],
                },
                CompactPosting {
                    doc_id: nodedb_types::Surrogate(1),
                    term_freq: 1,
                    fieldnorm: smallfloat::encode(50),
                    positions: vec![3],
                },
            ],
        );
        map.insert(
            "world".to_string(),
            vec![CompactPosting {
                doc_id: nodedb_types::Surrogate(0),
                term_freq: 1,
                fieldnorm: smallfloat::encode(100),
                positions: vec![1],
            }],
        );
        map
    }

    #[test]
    fn flush_produces_valid_segment() {
        let postings = make_postings();
        let segment = flush_to_segment(postings);

        // Should start with magic.
        assert_eq!(&segment[0..4], b"FTSS");

        // Parse header.
        let header = format::parse_header(&segment).unwrap();
        assert_eq!(header.num_terms, 2);
    }

    #[test]
    fn flush_empty_memtable() {
        let segment = flush_to_segment(HashMap::new());
        let header = format::parse_header(&segment).unwrap();
        assert_eq!(header.num_terms, 0);
    }
}
