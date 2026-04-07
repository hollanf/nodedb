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
    pub(super) fn scan_collection(
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
    fn scan_kv(&self, tid: u32, collection: &str, limit: usize) -> Vec<(String, Vec<u8>)> {
        let now_ms = crate::engine::kv::current_ms();
        let (entries, _next_cursor) =
            self.kv_engine
                .scan(tid, collection, &[], limit, now_ms, None, None, None);
        let mut results = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_str = String::from_utf8_lossy(&key).to_string();
            // Decode to JSON, inject `key` field, re-encode as standard msgpack.
            let json: serde_json::Value = nodedb_types::value_from_msgpack(&value)
                .ok()
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null);
            let json = if let serde_json::Value::Object(mut map) = json {
                map.entry("key")
                    .or_insert(serde_json::Value::String(key_str.clone()));
                serde_json::Value::Object(map)
            } else {
                serde_json::json!({"key": key_str, "value": json})
            };
            let mp = super::doc_format::encode_to_msgpack(&json);
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
