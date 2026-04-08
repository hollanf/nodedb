//! Universal document scan: routes to the correct engine and normalizes
//! all results to standard msgpack maps.
//!
//! Every query handler (aggregate, join, sort, filter, subquery) should
//! use `scan_collection` instead of calling engine-specific scan methods.
//! This gives a single place to handle format differences:
//! - Schemaless document → msgpack (already standard or legacy JSON)
//! - Strict document → Binary Tuple → decode → msgpack
//! - Key-Value → zerompk Value → transcode → msgpack
//! - Columnar → memtable rows → JSON → msgpack

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::msgpack_utils::write_str;
use nodedb_query::msgpack_scan;

impl CoreLoop {
    /// Universal scan: reads from the correct engine for `collection` and
    /// returns `(doc_id, msgpack_bytes)` pairs in standard msgpack map format.
    ///
    /// Routing order:
    /// 1. KV engine (if collection has KV entries)
    /// 2. Columnar memtable (if collection has columnar data)
    /// 3. Sparse/document engine (default)
    ///
    /// All results are normalized to standard msgpack maps so callers
    /// (aggregate, join, sort, filter) never need engine-specific code.
    pub fn scan_collection(
        &self,
        tid: u32,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        // 1. KV engine
        let kv_docs = self.scan_kv(tid, collection, limit);
        if !kv_docs.is_empty() {
            return Ok(kv_docs);
        }

        // 2. Columnar memtable
        let col_docs = self.scan_columnar(tid, collection, limit);
        if !col_docs.is_empty() {
            return Ok(col_docs);
        }

        // 3. Sparse/document engine (schemaless + strict)
        self.scan_sparse(tid, collection, limit)
    }

    /// Scan KV engine entries → standard msgpack.
    /// Injects the `key` field directly into the msgpack map — no JSON roundtrip.
    fn scan_kv(&self, tid: u32, collection: &str, limit: usize) -> Vec<(String, Vec<u8>)> {
        let now_ms = crate::engine::kv::current_ms();
        let (entries, _next_cursor) =
            self.kv_engine
                .scan(tid, collection, &[], limit, now_ms, None, None, None);
        let mut results = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_str = String::from_utf8_lossy(&key).to_string();
            // Inject "key" field into the msgpack map at binary level.
            let mp = inject_key_into_msgpack(&value, &key_str);
            results.push((key_str, mp));
        }
        results
    }

    /// Scan columnar memtable rows → standard msgpack.
    fn scan_columnar(&self, _tid: u32, collection: &str, limit: usize) -> Vec<(String, Vec<u8>)> {
        let mt = match self.columnar_memtables.get(collection) {
            Some(mt) => mt,
            None => return Vec::new(),
        };

        let schema = mt.schema();
        let row_count = (mt.row_count() as usize).min(limit);
        let col_meta: Vec<_> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| (i, name.clone(), *ty))
            .collect();

        let mut results = Vec::with_capacity(row_count);
        for idx in 0..row_count {
            let mut row = serde_json::Map::new();
            for (col_idx, col_name, col_type) in &col_meta {
                let col_data = mt.column(*col_idx);
                let val = super::handlers::columnar_read::emit_column_value(
                    mt, *col_idx, col_type, col_data, idx,
                );
                row.insert(col_name.to_string(), val);
            }
            let id = row
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mp =
                nodedb_types::json_to_msgpack(&serde_json::Value::Object(row)).unwrap_or_default();
            results.push((id, mp));
        }
        results
    }

    /// Scan sparse/document engine → standard msgpack.
    /// Handles both schemaless (msgpack) and strict (Binary Tuple) formats.
    pub(super) fn scan_sparse(
        &self,
        tid: u32,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let docs = self.sparse.scan_documents(tid, collection, limit)?;

        let config_key = format!("{tid}:{collection}");
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        if let Some(ref schema) = strict_schema {
            let mut normalized = Vec::with_capacity(docs.len());
            for (id, raw) in docs {
                let json =
                    super::strict_format::binary_tuple_to_json(&raw, schema).unwrap_or_else(|| {
                        super::doc_format::decode_document(&raw).unwrap_or(serde_json::Value::Null)
                    });
                let json = if let serde_json::Value::Object(mut map) = json {
                    map.entry("id")
                        .or_insert(serde_json::Value::String(id.clone()));
                    serde_json::Value::Object(map)
                } else {
                    json
                };
                let mp = super::doc_format::encode_to_msgpack(&json);
                normalized.push((id, mp));
            }
            Ok(normalized)
        } else {
            // Schemaless: normalize to standard msgpack and inject `id` field.
            let mut normalized = Vec::with_capacity(docs.len());
            for (id, raw) in docs {
                let json =
                    super::doc_format::decode_document(&raw).unwrap_or(serde_json::Value::Null);
                let json = if let serde_json::Value::Object(mut map) = json {
                    map.entry("id")
                        .or_insert(serde_json::Value::String(id.clone()));
                    serde_json::Value::Object(map)
                } else {
                    json
                };
                let mp = super::doc_format::encode_to_msgpack(&json);
                normalized.push((id, mp));
            }
            Ok(normalized)
        }
    }
}

/// Inject a "key" field into a standard msgpack map without JSON roundtrip.
///
/// If `value` is a valid msgpack map, returns a new map with "key" prepended.
/// If `value` is not a map (e.g. raw string/int), wraps as `{"key": k, "value": v}`.
fn inject_key_into_msgpack(value: &[u8], key: &str) -> Vec<u8> {
    use crate::data::executor::handlers::join::write_map_header;

    if let Some((count, body_start)) = msgpack_scan::map_header(value, 0) {
        // Valid map — prepend "key" entry, copy existing entries.
        let mut buf = Vec::with_capacity(value.len() + key.len() + 16);
        write_map_header(&mut buf, count + 1);
        // Write "key" field.
        write_str(&mut buf, "key");
        write_str(&mut buf, key);
        // Copy existing map body (all key-value pairs after the header).
        buf.extend_from_slice(&value[body_start..]);
        buf
    } else {
        // Not a map — wrap as {"key": k, "value": raw_bytes}.
        let mut buf = Vec::with_capacity(value.len() + key.len() + 16);
        write_map_header(&mut buf, 2);
        write_str(&mut buf, "key");
        write_str(&mut buf, key);
        write_str(&mut buf, "value");
        buf.extend_from_slice(value);
        buf
    }
}
