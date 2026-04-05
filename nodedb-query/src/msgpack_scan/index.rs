//! Per-document structural index for O(1) field access.
//!
//! When a query accesses multiple fields from the same document (e.g.,
//! GROUP BY + aggregate + filter), building a `FieldIndex` once and
//! reusing it for all field lookups avoids repeated O(N) key scanning.
//!
//! Uses a flat array with linear search for small docs (≤ 16 fields) to
//! avoid HashMap allocation overhead. Falls back to HashMap for large docs.

use crate::msgpack_scan::reader::{map_header, skip_value, str_bounds};

/// Threshold: docs with more fields than this use HashMap, otherwise flat array.
const HASH_THRESHOLD: usize = 16;

/// Pre-computed field offset table for a single MessagePack document.
pub struct FieldIndex {
    inner: IndexInner,
}

enum IndexInner {
    /// Flat array for small documents — linear scan, zero HashMap overhead.
    Flat(Vec<(Box<str>, usize, usize)>),
    /// HashMap for large documents — O(1) lookup.
    Map(std::collections::HashMap<Box<str>, (usize, usize)>),
}

impl FieldIndex {
    /// Build an index for the msgpack map at `offset` in `buf`.
    ///
    /// Scans all map keys once and records value byte ranges.
    /// Returns `None` if `buf` is not a valid map at `offset`.
    pub fn build(buf: &[u8], offset: usize) -> Option<Self> {
        let (count, mut pos) = map_header(buf, offset)?;

        if count <= HASH_THRESHOLD {
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key_str = if let Some((start, len)) = str_bounds(buf, pos) {
                    std::str::from_utf8(buf.get(start..start + len)?).ok()
                } else {
                    None
                };
                pos = skip_value(buf, pos)?;
                let value_start = pos;
                let value_end = skip_value(buf, pos)?;
                if let Some(key) = key_str {
                    entries.push((key.into(), value_start, value_end));
                }
                pos = value_end;
            }
            Some(Self {
                inner: IndexInner::Flat(entries),
            })
        } else {
            let mut offsets = std::collections::HashMap::with_capacity(count);
            for _ in 0..count {
                let key_str = if let Some((start, len)) = str_bounds(buf, pos) {
                    std::str::from_utf8(buf.get(start..start + len)?).ok()
                } else {
                    None
                };
                pos = skip_value(buf, pos)?;
                let value_start = pos;
                let value_end = skip_value(buf, pos)?;
                if let Some(key) = key_str {
                    offsets.insert(key.into(), (value_start, value_end));
                }
                pos = value_end;
            }
            Some(Self {
                inner: IndexInner::Map(offsets),
            })
        }
    }

    /// Create an empty index (no fields).
    pub fn empty() -> Self {
        Self {
            inner: IndexInner::Flat(Vec::new()),
        }
    }

    /// Look up a field's byte range.
    #[inline]
    pub fn get(&self, field: &str) -> Option<(usize, usize)> {
        match &self.inner {
            IndexInner::Flat(entries) => entries
                .iter()
                .find(|(k, _, _)| k.as_ref() == field)
                .map(|(_, s, e)| (*s, *e)),
            IndexInner::Map(map) => map.get(field).copied(),
        }
    }

    /// Number of indexed fields.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            IndexInner::Flat(entries) => entries.len(),
            IndexInner::Map(map) => map.len(),
        }
    }

    /// Whether the index is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_scan::reader::{read_f64, read_i64, read_str};
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn build_and_lookup() {
        let buf = encode(&json!({"name": "alice", "age": 30, "score": 99.5}));
        let idx = FieldIndex::build(&buf, 0).unwrap();

        assert_eq!(idx.len(), 3);

        let (s, _) = idx.get("name").unwrap();
        assert_eq!(read_str(&buf, s), Some("alice"));

        let (s, _) = idx.get("age").unwrap();
        assert_eq!(read_i64(&buf, s), Some(30));

        let (s, _) = idx.get("score").unwrap();
        assert_eq!(read_f64(&buf, s), Some(99.5));
    }

    #[test]
    fn missing_field() {
        let buf = encode(&json!({"x": 1}));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert!(idx.get("y").is_none());
    }

    #[test]
    fn empty_map() {
        let buf = encode(&json!({}));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert!(idx.is_empty());
    }

    #[test]
    fn small_doc_uses_flat() {
        let mut map = serde_json::Map::new();
        for i in 0..10 {
            map.insert(format!("f{i}"), json!(i));
        }
        let buf = encode(&serde_json::Value::Object(map));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert_eq!(idx.len(), 10);
        assert!(matches!(idx.inner, IndexInner::Flat(_)));
    }

    #[test]
    fn large_doc_uses_hashmap() {
        let mut map = serde_json::Map::new();
        for i in 0..20 {
            map.insert(format!("field_{i}"), json!(i));
        }
        let buf = encode(&serde_json::Value::Object(map));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert_eq!(idx.len(), 20);
        assert!(matches!(idx.inner, IndexInner::Map(_)));

        for i in 0..20 {
            let (s, _) = idx.get(&format!("field_{i}")).unwrap();
            assert_eq!(read_i64(&buf, s), Some(i));
        }
    }

    #[test]
    fn not_a_map() {
        let buf = encode(&json!([1, 2, 3]));
        assert!(FieldIndex::build(&buf, 0).is_none());
    }

    #[test]
    fn indexed_vs_sequential_same_result() {
        let buf = encode(&json!({"a": 1, "b": "two", "c": 3.0}));
        let idx = FieldIndex::build(&buf, 0).unwrap();

        for field in &["a", "b", "c"] {
            let indexed = idx.get(field);
            let sequential = crate::msgpack_scan::field::extract_field(&buf, 0, field);
            assert_eq!(indexed, sequential, "mismatch for field {field}");
        }
    }
}
