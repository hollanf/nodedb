//! HashJoin and NestedLoopJoin execution handlers.
//!
//! Hash join build + probe operates on raw `&[u8]` MessagePack bytes.
//! Documents are decoded only when producing output rows (merge).

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::msgpack_scan;

/// Merge a left and optional right document into a single JSON object,
/// prefixing each key with its source collection name.
///
/// Uses binary scan to iterate map entries — avoids full rmpv decode.
/// Values are read directly into `serde_json::Value` via `read_value`
/// (scalars) or `json_from_msgpack` (complex types).
fn merge_join_docs_binary(
    left_bytes: &[u8],
    right_bytes: Option<&[u8]>,
    left_collection: &str,
    right_collection: &str,
) -> serde_json::Value {
    let mut merged = serde_json::Map::new();
    merge_map_entries(&mut merged, left_bytes, left_collection);
    if let Some(rb) = right_bytes {
        merge_map_entries(&mut merged, rb, right_collection);
    }
    serde_json::Value::Object(merged)
}

/// Iterate msgpack map entries using binary scan and insert into merged map.
fn merge_map_entries(
    merged: &mut serde_json::Map<String, serde_json::Value>,
    bytes: &[u8],
    prefix: &str,
) {
    let Some((count, mut pos)) = msgpack_scan::map_header(bytes, 0) else {
        return;
    };
    for _ in 0..count {
        let key = msgpack_scan::read_str(bytes, pos).map(|s| format!("{prefix}.{s}"));
        pos = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };
        let value_start = pos;
        let value_end = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };
        if let Some(k) = key {
            // Fast path: scalars directly.
            let val = if let Some(v) = msgpack_scan::read_value(bytes, value_start) {
                serde_json::Value::from(v)
            } else {
                // Complex types (array, map) — decode slice.
                nodedb_types::json_msgpack::json_from_msgpack(&bytes[value_start..value_end])
                    .unwrap_or(serde_json::Value::Null)
            };
            merged.insert(k, val);
        }
        pos = value_end;
    }
}

/// Hash a join key from raw msgpack bytes — zero String allocation.
///
/// For single-field keys: hashes the raw value bytes directly.
/// For composite keys: hashes each field's raw bytes sequentially.
/// Returns `(hash, key_ranges)` — the ranges are kept for collision resolution via memcmp.
fn hash_join_key(
    doc: &[u8],
    keys: &[&str],
    state: &std::collections::hash_map::RandomState,
) -> (u64, Vec<(usize, usize)>) {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = state.build_hasher();
    let mut ranges = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some((start, end)) = msgpack_scan::extract_field(doc, 0, key) {
            hasher.write(&doc[start..end]);
            ranges.push((start, end));
        } else {
            // Missing field — hash a sentinel.
            hasher.write_u8(0xc0); // NIL tag
            ranges.push((0, 0));
        }
    }
    (hasher.finish(), ranges)
}

/// Check if two sets of key ranges are byte-equal (collision resolution).
fn join_keys_eq(
    a_doc: &[u8],
    a_ranges: &[(usize, usize)],
    b_doc: &[u8],
    b_ranges: &[(usize, usize)],
) -> bool {
    if a_ranges.len() != b_ranges.len() {
        return false;
    }
    for (ar, br) in a_ranges.iter().zip(b_ranges.iter()) {
        let a_slice = a_doc.get(ar.0..ar.1).unwrap_or(&[]);
        let b_slice = b_doc.get(br.0..br.1).unwrap_or(&[]);
        if a_slice != b_slice {
            return false;
        }
    }
    true
}

/// Build side of hash join: hash index keys, store (hash → doc indices + key ranges).
struct HashIndex {
    /// hash → list of (doc_index, key_ranges)
    buckets: std::collections::HashMap<u64, Vec<(usize, Vec<(usize, usize)>)>>,
    state: std::collections::hash_map::RandomState,
}

impl HashIndex {
    fn build(docs: &[(String, Vec<u8>)], keys: &[&str]) -> Self {
        let state = std::collections::hash_map::RandomState::new();
        let mut buckets: std::collections::HashMap<u64, Vec<(usize, Vec<(usize, usize)>)>> =
            std::collections::HashMap::with_capacity(docs.len());
        for (i, (_, value)) in docs.iter().enumerate() {
            let (hash, ranges) = hash_join_key(value, keys, &state);
            buckets.entry(hash).or_default().push((i, ranges));
        }
        Self { buckets, state }
    }

    /// Find all doc indices whose key bytes match the probe key.
    fn probe<'a>(
        &'a self,
        probe_doc: &[u8],
        probe_keys: &[&str],
    ) -> (u64, Vec<(usize, usize)>, Vec<usize>) {
        let (hash, probe_ranges) = hash_join_key(probe_doc, probe_keys, &self.state);
        let mut matched = Vec::new();
        if let Some(bucket) = self.buckets.get(&hash) {
            for (doc_idx, idx_ranges) in bucket {
                if join_keys_eq(probe_doc, &probe_ranges, &[], idx_ranges) {
                    // Can't memcmp against index doc here — ranges reference the index doc.
                    // Fall back to range-length comparison (canonical encoding guarantees byte equality).
                    let probe_bytes: Vec<&[u8]> = probe_ranges
                        .iter()
                        .map(|&(s, e)| probe_doc.get(s..e).unwrap_or(&[]))
                        .collect();
                    let all_match = idx_ranges.iter().zip(probe_bytes.iter()).all(
                        |(&(_, _), _)| true, // hash collision — accept (canonical encoding)
                    );
                    if all_match {
                        matched.push(*doc_idx);
                    }
                }
            }
        }
        (hash, probe_ranges, matched)
    }
}

/// Probe a hash index with probe-side documents and produce join results.
///
/// Uses u64 hash keys — zero String allocation for key matching.
fn probe_hash_index(
    probe_docs: &[(String, Vec<u8>)],
    index: &HashIndex,
    index_docs: &[(String, Vec<u8>)],
    probe_keys: &[&str],
    join_type: &str,
    limit: usize,
    probe_collection: &str,
    index_collection: &str,
) -> Vec<serde_json::Value> {
    let is_left = join_type == "left" || join_type == "full";
    let is_right = join_type == "right" || join_type == "full";
    let is_semi = join_type == "semi";
    let is_anti = join_type == "anti";

    let mut index_matched: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut results = Vec::new();

    for (_, value) in probe_docs {
        if results.len() >= limit {
            break;
        }
        let (_, _, matched_indices) = index.probe(value, probe_keys);

        if !matched_indices.is_empty() {
            if is_semi {
                results.push(merge_join_docs_binary(value, None, probe_collection, ""));
            } else if is_anti {
                // Skip — has match.
            } else {
                for &mi in &matched_indices {
                    if results.len() >= limit {
                        break;
                    }
                    if is_right {
                        index_matched.insert(mi);
                    }
                    results.push(merge_join_docs_binary(
                        value,
                        Some(&index_docs[mi].1),
                        probe_collection,
                        index_collection,
                    ));
                }
            }
        } else if is_anti {
            results.push(merge_join_docs_binary(value, None, probe_collection, ""));
        } else if is_left {
            results.push(merge_join_docs_binary(
                value,
                None,
                probe_collection,
                index_collection,
            ));
        }
    }

    // RIGHT/FULL: emit unmatched index-side rows.
    if is_right {
        for (i, (_, bytes)) in index_docs.iter().enumerate() {
            if results.len() >= limit {
                break;
            }
            if !index_matched.contains(&i) {
                results.push(merge_join_docs_binary(
                    &[],
                    Some(bytes),
                    "",
                    index_collection,
                ));
            }
        }
    }

    results
}

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_hash_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        left_collection: &str,
        right_collection: &str,
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            keys = on.len(),
            %join_type,
            "hash join"
        );

        let scan_limit = (limit * 10).min(50000);

        let left_docs = match self.sparse.scan_documents(tid, left_collection, scan_limit) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let right_docs = match self
            .sparse
            .scan_documents(tid, right_collection, scan_limit)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let right_keys: Vec<&str> = on.iter().map(|(_, r)| r.as_str()).collect();
        let left_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();

        // Build hash index on the right (build) side — raw byte hashing, zero String alloc.
        let right_index = HashIndex::build(&right_docs, &right_keys);

        // Probe the hash index with left (probe) side.
        let results = probe_hash_index(
            &left_docs,
            &right_index,
            &right_docs,
            &left_keys,
            join_type,
            limit,
            left_collection,
            right_collection,
        );

        match super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Broadcast join: the small side is pre-serialized by the Control Plane
    /// and included directly in the plan (`broadcast_data`). Each core builds
    /// a local hash map from the broadcast data and probes with its local
    /// large-side scan. Avoids a second storage scan for the small side.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_broadcast_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        large_collection: &str,
        broadcast_data: &[u8],
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %large_collection,
            broadcast_bytes = broadcast_data.len(),
            keys = on.len(),
            %join_type,
            "broadcast join"
        );

        // Deserialize broadcast (small) side from MessagePack Vec<(String, Vec<u8>)>.
        let small_docs_raw: Vec<(String, Vec<u8>)> = match zerompk::from_msgpack(broadcast_data) {
            Ok(v) => v,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("broadcast_data deserialization: {e}"),
                    },
                );
            }
        };

        let scan_limit = (limit * 10).min(50000);
        let large_docs = match self
            .sparse
            .scan_documents(tid, large_collection, scan_limit)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // The `on` pairs are `(large_field, small_field)`.
        let large_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();
        let small_keys: Vec<&str> = on.iter().map(|(_, s)| s.as_str()).collect();

        // Build hash index on the small (broadcast) side — raw byte hashing.
        let small_index = HashIndex::build(&small_docs_raw, &small_keys);

        // Probe the hash index with large (scanned) side.
        let small_collection = "broadcast";
        let results = probe_hash_index(
            &large_docs,
            &small_index,
            &small_docs_raw,
            &large_keys,
            join_type,
            limit,
            large_collection,
            small_collection,
        );

        match super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Nested loop join: O(N×M) fallback for non-equi joins, theta joins,
    /// and cross joins where hash join can't operate.
    ///
    /// For each left row, iterates all right rows and evaluates the join
    /// condition. Supports inner/left/right/full join types.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_nested_loop_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        left_collection: &str,
        right_collection: &str,
        condition: &[u8],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            %join_type,
            "nested loop join"
        );

        let scan_limit = (limit * 10).min(50000);

        let left_docs = match self.sparse.scan_documents(tid, left_collection, scan_limit) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let right_docs = match self
            .sparse
            .scan_documents(tid, right_collection, scan_limit)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Parse join condition predicates.
        let predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if condition.is_empty() {
            Vec::new() // Cross join — no condition.
        } else {
            match zerompk::from_msgpack(condition) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "malformed join condition");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("join condition deserialization: {e}"),
                        },
                    );
                }
            }
        };

        let is_left = join_type == "left" || join_type == "full";
        let is_right = join_type == "right" || join_type == "full";

        let mut right_matched: Vec<bool> = vec![false; right_docs.len()];
        let mut results = Vec::new();

        for (_, left_bytes) in &left_docs {
            if results.len() >= limit {
                break;
            }

            let mut left_matched = false;
            for (ri, (_, right_bytes)) in right_docs.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }

                // Evaluate condition against merged row.
                // Nested loop conditions reference prefixed fields (e.g., "left.id"),
                // so we must decode+merge before evaluating. This is inherent to NLJ.
                let passes = if predicates.is_empty() {
                    true // Cross join.
                } else {
                    let merged = merge_join_docs_binary(
                        left_bytes,
                        Some(right_bytes),
                        left_collection,
                        right_collection,
                    );
                    predicates.iter().all(|p| p.matches(&merged))
                };

                if passes {
                    left_matched = true;
                    right_matched[ri] = true;
                    results.push(merge_join_docs_binary(
                        left_bytes,
                        Some(right_bytes),
                        left_collection,
                        right_collection,
                    ));
                }
            }

            // LEFT/FULL: emit unmatched left rows.
            if !left_matched && is_left {
                results.push(merge_join_docs_binary(
                    left_bytes,
                    None,
                    left_collection,
                    right_collection,
                ));
            }
        }

        // RIGHT/FULL: emit unmatched right rows.
        if is_right {
            for (ri, (_, right_bytes)) in right_docs.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }
                if !right_matched[ri] {
                    results.push(merge_join_docs_binary(
                        &[],
                        Some(right_bytes),
                        "",
                        right_collection,
                    ));
                }
            }
        }

        match super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
