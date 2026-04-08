//! Join execution handlers — hash, sort-merge, broadcast, and nested-loop.

pub mod hash;
pub mod nested_loop;
pub mod sort_merge;

use nodedb_query::msgpack_scan;

use crate::data::executor::msgpack_utils::write_str;

/// Merge a left and optional right document into a single msgpack map,
/// prefixing each key with its source collection name.
///
/// Returns raw msgpack bytes — no JSON decode, no serde_json::Value.
/// Uses binary scan to iterate source map entries and writes directly
/// to the output buffer.
pub fn merge_join_docs_binary(
    left_bytes: &[u8],
    right_bytes: Option<&[u8]>,
    left_collection: &str,
    right_collection: &str,
) -> Vec<u8> {
    let left_count = count_map_entries(left_bytes);
    let right_count = right_bytes.map_or(0, count_map_entries);
    let total = left_count + right_count;

    // Estimate capacity: original data + prefixed keys overhead.
    let cap = left_bytes.len()
        + right_bytes.map_or(0, |b| b.len())
        + total * (left_collection.len().max(right_collection.len()) + 8);
    let mut buf = Vec::with_capacity(cap);

    write_map_header(&mut buf, total);
    write_prefixed_entries(&mut buf, left_bytes, left_collection);
    if let Some(rb) = right_bytes {
        write_prefixed_entries(&mut buf, rb, right_collection);
    }
    buf
}

/// Count entries in a msgpack map.
fn count_map_entries(bytes: &[u8]) -> usize {
    msgpack_scan::map_header(bytes, 0).map_or(0, |(count, _)| count)
}

/// Write a msgpack map header.
pub fn write_map_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Iterate msgpack map entries and write each key prefixed with `prefix.`
/// and its value bytes verbatim.
fn write_prefixed_entries(buf: &mut Vec<u8>, bytes: &[u8], prefix: &str) {
    let Some((count, mut pos)) = msgpack_scan::map_header(bytes, 0) else {
        return;
    };
    for _ in 0..count {
        // Read key string.
        let key = msgpack_scan::read_str(bytes, pos);
        pos = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };
        // Copy value bytes verbatim.
        let value_start = pos;
        let value_end = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };

        // Write prefixed key.
        if let Some(k) = key {
            if prefix.is_empty() {
                write_str(buf, k);
            } else {
                // Avoid allocation: write prefix.key directly.
                let prefixed_len = prefix.len() + 1 + k.len();
                if prefixed_len < 32 {
                    buf.push(0xA0 | prefixed_len as u8);
                } else if prefixed_len <= u8::MAX as usize {
                    buf.push(0xD9);
                    buf.push(prefixed_len as u8);
                } else if prefixed_len <= u16::MAX as usize {
                    buf.push(0xDA);
                    buf.extend_from_slice(&(prefixed_len as u16).to_be_bytes());
                } else {
                    buf.push(0xDB);
                    buf.extend_from_slice(&(prefixed_len as u32).to_be_bytes());
                }
                buf.extend_from_slice(prefix.as_bytes());
                buf.push(b'.');
                buf.extend_from_slice(k.as_bytes());
            }
        }
        // Write value bytes verbatim — zero decode.
        buf.extend_from_slice(&bytes[value_start..value_end]);
        pos = value_end;
    }
}

/// Compare two documents using pre-extracted key byte ranges.
/// `a_ranges`/`b_ranges` are `(start, end)` byte slices into the respective docs.
pub(super) fn compare_preextracted(
    a_doc: &[u8],
    a_ranges: &[(usize, usize)],
    b_doc: &[u8],
    b_ranges: &[(usize, usize)],
) -> std::cmp::Ordering {
    use nodedb_query::msgpack_scan::compare_field_bytes;
    for (a_range, b_range) in a_ranges.iter().zip(b_ranges.iter()) {
        let ord = compare_field_bytes(a_doc, *a_range, b_doc, *b_range);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Filter a binary msgpack row against ScanFilter predicates.
///
/// ScanFilter field names may be unqualified ("amount") while the merged
/// join row has qualified keys ("orders.amount"). We try the field name
/// as-is first, then fall back to suffix matching.
pub(super) fn binary_row_matches_filters(
    row: &[u8],
    filters: &[crate::bridge::scan_filter::ScanFilter],
) -> bool {
    filters.iter().all(|f| {
        if f.op == crate::bridge::scan_filter::FilterOp::MatchAll {
            return true;
        }
        // Try exact field name, then "collection.field" pattern.
        if f.matches_binary(row) {
            return true;
        }
        // ScanFilter looked for "amount" but the key is "orders.amount".
        // Extract all keys, find one ending with ".{field}", extract its value,
        // and compare manually.
        let Some((count, mut pos)) = msgpack_scan::map_header(row, 0) else {
            return false;
        };
        let suffix = format!(".{}", f.field);
        for _ in 0..count {
            let key = msgpack_scan::read_str(row, pos);
            let key_end = match msgpack_scan::skip_value(row, pos) {
                Some(p) => p,
                None => return false,
            };
            let val_start = key_end;
            let val_end = match msgpack_scan::skip_value(row, val_start) {
                Some(p) => p,
                None => return false,
            };
            if let Some(k) = key
                && (k.ends_with(&suffix) || k == f.field)
            {
                // Build a single-field map for the filter to match against.
                let mut mini = Vec::with_capacity(f.field.len() + (val_end - val_start) + 8);
                mini.push(0x81); // fixmap with 1 entry
                write_str(&mut mini, &f.field);
                mini.extend_from_slice(&row[val_start..val_end]);
                return f.matches_binary(&mini);
            }
            pos = val_end;
        }
        false
    })
}

/// Apply projection to a binary msgpack row, keeping only requested columns.
///
/// Projection names may be unqualified ("name") while keys are qualified
/// ("users.name"). Returns a new msgpack map with only matching fields,
/// using the unqualified name as the output key.
pub(super) fn binary_row_project(row: &[u8], projection: &[String]) -> Vec<u8> {
    let Some((count, pos)) = msgpack_scan::map_header(row, 0) else {
        return row.to_vec();
    };

    // First pass: find matching entries.
    struct Entry {
        output_key: String,
        val_start: usize,
        val_end: usize,
    }
    let mut entries = Vec::with_capacity(projection.len());
    let mut scan_pos = pos;
    for _ in 0..count {
        let key = msgpack_scan::read_str(row, scan_pos);
        scan_pos = match msgpack_scan::skip_value(row, scan_pos) {
            Some(p) => p,
            None => break,
        };
        let val_start = scan_pos;
        scan_pos = match msgpack_scan::skip_value(row, scan_pos) {
            Some(p) => p,
            None => break,
        };
        if let Some(ref k) = key {
            let short = k.rsplit('.').next().unwrap_or(k);
            if projection.iter().any(|p| p == short || p == k) {
                entries.push(Entry {
                    output_key: short.to_string(),
                    val_start,
                    val_end: scan_pos,
                });
            }
        }
    }

    // Build output map.
    let mut buf = Vec::with_capacity(row.len());
    write_map_header(&mut buf, entries.len());
    for e in &entries {
        write_str(&mut buf, &e.output_key);
        buf.extend_from_slice(&row[e.val_start..e.val_end]);
    }
    buf
}
