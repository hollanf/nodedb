//! Universal document scan: routes to the correct engine and normalizes
//! all results to standard msgpack maps.
//!
//! Every query handler (aggregate, join, sort, filter, subquery) should
//! use `scan_collection` instead of calling engine-specific scan methods.
//! This gives a single place to handle format differences:
//! - Schemaless document → msgpack (already standard or legacy JSON)
//! - Strict document → Binary Tuple → decode → msgpack
//! - Key-Value → zerompk Value → transcode → msgpack
//! - Columnar → memtable/engine rows → JSON → msgpack

use crate::data::executor::core_loop::CoreLoop;
use nodedb_query::msgpack_scan;

impl CoreLoop {
    /// Universal scan: reads from the correct engine for `collection` and
    /// returns `(doc_id, msgpack_bytes)` pairs in standard msgpack map format.
    ///
    /// Routing order:
    /// 1. KV engine (if collection has KV entries)
    /// 2. Columnar storage (timeseries memtable or plain/spatial engine)
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
            let mp = msgpack_scan::inject_str_field(&value, "key", &key_str);
            results.push((key_str, mp));
        }
        results
    }

    /// Scan columnar rows → standard msgpack.
    fn scan_columnar(&self, _tid: u32, collection: &str, limit: usize) -> Vec<(String, Vec<u8>)> {
        if let Some(mt) = self.columnar_memtables.get(collection) {
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
                // Build msgpack map directly — no serde_json intermediary.
                let mut mp = Vec::with_capacity(col_meta.len() * 32);
                msgpack_scan::write_map_header(&mut mp, col_meta.len());
                let mut id = String::new();
                for (col_idx, col_name, col_type) in &col_meta {
                    msgpack_scan::write_str(&mut mp, col_name);
                    let col_data = mt.column(*col_idx);
                    // Check for "id" column to extract the id string.
                    if col_name == "id" {
                        if let crate::engine::timeseries::columnar_memtable::ColumnData::Symbol(
                            ids,
                        ) = col_data
                        {
                            let sym_id = ids[idx];
                            if let Some(s) =
                                mt.symbol_dict(*col_idx).and_then(|dict| dict.get(sym_id))
                            {
                                id = s.to_string();
                            }
                        }
                    }
                    super::handlers::columnar_read::emit_column_value(
                        &mut mp, mt, *col_idx, col_type, col_data, idx,
                    );
                }
                results.push((id, mp));
            }
            return results;
        }

        let Some(engine) = self.columnar_engines.get(collection) else {
            return Vec::new();
        };

        let schema = engine.schema();
        let rows: Vec<_> = engine.scan_memtable_rows().take(limit).collect();
        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            // Build a nodedb_types::Value::Object directly — no JSON intermediary.
            let mut map = std::collections::HashMap::new();
            let mut id = String::new();
            for (i, col_def) in schema.columns.iter().enumerate() {
                if i < row.len() {
                    if col_def.name == "id"
                        && let nodedb_types::value::Value::String(s) = &row[i]
                    {
                        id.clone_from(s);
                    }
                    map.insert(col_def.name.clone(), row[i].clone());
                }
            }
            let ndb_val = nodedb_types::value::Value::Object(map);
            let mp = nodedb_types::value_to_msgpack(&ndb_val).unwrap_or_default();
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
            // Strict: Binary Tuple → msgpack → inject "id".
            let mut normalized = Vec::with_capacity(docs.len());
            for (id, raw) in docs {
                let mp = super::strict_format::binary_tuple_to_msgpack(&raw, schema)
                    .unwrap_or_else(|| super::doc_format::json_to_msgpack(&raw));
                let mp = msgpack_scan::inject_str_field(&mp, "id", &id);
                normalized.push((id, mp));
            }
            Ok(normalized)
        } else {
            // Schemaless: ensure standard msgpack, inject `id` field.
            let mut normalized = Vec::with_capacity(docs.len());
            for (id, raw) in docs {
                let mp = super::doc_format::json_to_msgpack(&raw);
                let mp = msgpack_scan::inject_str_field(&mp, "id", &id);
                normalized.push((id, mp));
            }
            Ok(normalized)
        }
    }
}
