//! Hash join and broadcast join execution.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::msgpack_scan;

use super::merge_join_docs_binary;

/// Hash a join key from raw msgpack bytes — zero String allocation.
///
/// For single-field keys: hashes the raw value bytes directly.
/// For composite keys: hashes each field's raw bytes sequentially.
/// Returns `(hash, key_ranges)` — the ranges are kept for collision resolution via memcmp.
pub(super) fn hash_join_key(
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

/// (doc_index, key_ranges) for a single hash bucket entry.
type BucketEntry = (usize, Vec<(usize, usize)>);

/// Build side of hash join: hash index keys, store (hash → doc indices + key ranges).
pub(super) struct HashIndex {
    pub(super) buckets: std::collections::HashMap<u64, Vec<BucketEntry>>,
    pub(super) state: std::collections::hash_map::RandomState,
}

impl HashIndex {
    pub(super) fn build(docs: &[(String, Vec<u8>)], keys: &[&str]) -> Self {
        let state = std::collections::hash_map::RandomState::new();
        let mut buckets: std::collections::HashMap<u64, Vec<BucketEntry>> =
            std::collections::HashMap::with_capacity(docs.len());
        for (i, (_, value)) in docs.iter().enumerate() {
            let (hash, ranges) = hash_join_key(value, keys, &state);
            buckets.entry(hash).or_default().push((i, ranges));
        }
        Self { buckets, state }
    }

    /// Find all doc indices whose key bytes match the probe key.
    pub(super) fn probe(
        &self,
        probe_doc: &[u8],
        probe_keys: &[&str],
        build_docs: &[(String, Vec<u8>)],
    ) -> (u64, Vec<(usize, usize)>, Vec<usize>) {
        let (hash, probe_ranges) = hash_join_key(probe_doc, probe_keys, &self.state);
        let mut matched = Vec::new();
        if let Some(bucket) = self.buckets.get(&hash) {
            for (doc_idx, idx_ranges) in bucket {
                // Verify actual byte ranges match — hash collisions are possible.
                let mut all_match = !probe_ranges.is_empty();
                for (i, &(ps, pe)) in probe_ranges.iter().enumerate() {
                    if let Some(&(bs, be)) = idx_ranges.get(i) {
                        let build_doc = &build_docs[*doc_idx].1;
                        if pe - ps != be - bs || probe_doc[ps..pe] != build_doc[bs..be] {
                            all_match = false;
                            break;
                        }
                    } else {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    matched.push(*doc_idx);
                }
            }
        }
        (hash, probe_ranges, matched)
    }
}

/// Parameters for probing a hash join index.
pub(super) struct ProbeParams<'a> {
    pub(super) probe_docs: &'a [(String, Vec<u8>)],
    pub(super) index: &'a HashIndex,
    pub(super) index_docs: &'a [(String, Vec<u8>)],
    pub(super) probe_keys: &'a [&'a str],
    pub(super) join_type: &'a str,
    pub(super) limit: usize,
    pub(super) probe_collection: &'a str,
    pub(super) index_collection: &'a str,
    /// For broadcast RIGHT/FULL joins: only the designated core should emit
    /// unmatched right-side rows. Other cores set this to `false` to avoid
    /// N× duplication of unmatched rows across cores.
    pub(super) emit_unmatched_right: bool,
}

/// Probe a hash index with probe-side documents and produce join results.
///
/// Returns binary msgpack rows — no JSON decode.
/// Uses u64 hash keys — zero String allocation for key matching.
pub(super) fn probe_hash_index(p: &ProbeParams<'_>) -> Vec<Vec<u8>> {
    let is_left = p.join_type == "left" || p.join_type == "full";
    let is_right = p.join_type == "right" || p.join_type == "full";
    let is_semi = p.join_type == "semi";
    let is_anti = p.join_type == "anti";
    let is_cross = p.join_type == "cross";

    // Cross join: cartesian product (no hash lookup needed).
    if is_cross {
        let mut results = Vec::new();
        for (_, left_val) in p.probe_docs {
            for (_, right_val) in p.index_docs {
                if results.len() >= p.limit {
                    return results;
                }
                results.push(merge_join_docs_binary(
                    left_val,
                    Some(right_val),
                    p.probe_collection,
                    p.index_collection,
                ));
            }
        }
        return results;
    }

    // For RIGHT/FULL joins, pre-allocate a complete tracking vector so we
    // never miss marking a matched index-side row (even if we hit the limit
    // during the probe loop). This prevents the cartesian product bug where
    // incomplete tracking causes matched rows to be emitted as unmatched.
    let mut index_matched: Vec<bool> = if is_right {
        vec![false; p.index_docs.len()]
    } else {
        Vec::new()
    };
    let mut results = Vec::new();

    for (_, value) in p.probe_docs {
        // For RIGHT/FULL joins, we must complete the full probe to populate
        // index_matched, even after we have enough result rows.
        if !is_right && results.len() >= p.limit {
            break;
        }
        let (_, _, matched_indices) = p.index.probe(value, p.probe_keys, p.index_docs);

        if !matched_indices.is_empty() {
            if is_semi {
                if results.len() < p.limit {
                    results.push(merge_join_docs_binary(value, None, p.probe_collection, ""));
                }
            } else if is_anti {
                // Skip — has match.
            } else {
                for &mi in &matched_indices {
                    if is_right {
                        index_matched[mi] = true;
                    }
                    if results.len() < p.limit {
                        results.push(merge_join_docs_binary(
                            value,
                            Some(&p.index_docs[mi].1),
                            p.probe_collection,
                            p.index_collection,
                        ));
                    }
                }
            }
        } else if is_anti && results.len() < p.limit {
            results.push(merge_join_docs_binary(value, None, p.probe_collection, ""));
        } else if is_left && results.len() < p.limit {
            results.push(merge_join_docs_binary(
                value,
                None,
                p.probe_collection,
                p.index_collection,
            ));
        }
    }

    // RIGHT/FULL: emit unmatched index-side rows.
    // In broadcast mode, only the designated core emits these to avoid duplication.
    if is_right && p.emit_unmatched_right {
        for (i, (_, bytes)) in p.index_docs.iter().enumerate() {
            if results.len() >= p.limit {
                break;
            }
            if !index_matched[i] {
                results.push(merge_join_docs_binary(
                    &[],
                    Some(bytes),
                    "",
                    p.index_collection,
                ));
            }
        }
    }

    results
}

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
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
        projection: &[String],
        post_filter_bytes: &[u8],
        inline_left: Option<&crate::bridge::envelope::PhysicalPlan>,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            keys = on.len(),
            %join_type,
            inline = inline_left.is_some(),
            "hash join"
        );

        let scan_limit = (limit * 10).min(50000);

        // If inline_left is set, execute the sub-plan to get left side docs.
        let left_docs = if let Some(sub_plan) = inline_left {
            let sub_response = self.execute_plan(task, sub_plan);
            match super::super::super::response_codec::decode_response_to_docs(&sub_response) {
                Some(docs) => docs,
                None => {
                    // Sub-plan returned no decodable rows — pass through the response.
                    return sub_response;
                }
            }
        } else {
            match self.scan_collection(tid, left_collection, scan_limit) {
                Ok(d) => d,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };
        let right_docs = match self.scan_collection(tid, right_collection, scan_limit) {
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
        // Single-core hash join: always emit unmatched right rows.
        let mut results = probe_hash_index(&ProbeParams {
            probe_docs: &left_docs,
            index: &right_index,
            index_docs: &right_docs,
            probe_keys: &left_keys,
            join_type,
            limit,
            probe_collection: left_collection,
            index_collection: right_collection,
            emit_unmatched_right: true,
        });

        // Apply post-join WHERE filters (binary — no JSON roundtrip).
        if !post_filter_bytes.is_empty() {
            let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
                zerompk::from_msgpack(post_filter_bytes).unwrap_or_default();
            if !filters.is_empty() {
                results.retain(|row| super::binary_row_matches_filters(row, &filters));
            }
        }

        // Apply post-join projection (binary — no JSON roundtrip).
        if !projection.is_empty() && !projection.iter().any(|p| p == "*") {
            for row in &mut results {
                *row = super::binary_row_project(row, projection);
            }
        }

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(task, payload)
    }

    /// Inline hash join: both sides are pre-gathered as msgpack data.
    /// Used for multi-way joins where the left side is an inner join result.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_inline_hash_join(
        &mut self,
        task: &ExecutionTask,
        left_data: &[u8],
        right_data: &[u8],
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
        projection: &[String],
        post_filter_bytes: &[u8],
    ) -> Response {
        debug!(
            core = self.core_id,
            left_bytes = left_data.len(),
            right_bytes = right_data.len(),
            keys = on.len(),
            %join_type,
            "inline hash join"
        );

        // Decode left side: msgpack array or JSON array (from inner join result).
        let left_docs =
            match super::super::super::response_codec::decode_response_to_docs_from_bytes(left_data)
            {
                Some(d) => d,
                None => {
                    return self.response_with_payload(
                        task,
                        super::super::super::response_codec::encode_binary_rows(&[]),
                    );
                }
            };

        // Decode right side: broadcast_raw format ({id, data} wrapper maps).
        let right_docs = super::super::super::response_codec::decode_raw_scan_to_docs(right_data);

        tracing::warn!(
            core = self.core_id,
            left_count = left_docs.len(),
            right_count = right_docs.len(),
            "inline hash join: decoded both sides"
        );

        // Left docs come from a merged join result where field names are
        // prefixed as "collection.field". The join keys from the planner are
        // unqualified (e.g. "id"). Resolve qualified names by scanning the
        // first left doc for fields ending in ".{key}".
        let mut left_key_strs: Vec<String> = on.iter().map(|(l, _)| l.clone()).collect();
        if let Some((_, first_doc)) = left_docs.first() {
            for key in &mut left_key_strs {
                // If the key doesn't exist directly, find a qualified version.
                if msgpack_scan::extract_field(first_doc, 0, key).is_none() {
                    let suffix = format!(".{key}");
                    // Scan the map for a key ending with ".{key}".
                    if let Some((count, mut pos)) = msgpack_scan::map_header(first_doc, 0) {
                        for _ in 0..count {
                            if let Some(field_name) = msgpack_scan::read_str(first_doc, pos)
                                && field_name.ends_with(&suffix)
                            {
                                *key = field_name.to_string();
                                break;
                            }
                            pos = match msgpack_scan::skip_value(first_doc, pos) {
                                Some(p) => p,
                                None => break,
                            };
                            pos = match msgpack_scan::skip_value(first_doc, pos) {
                                Some(p) => p,
                                None => break,
                            };
                        }
                    }
                }
            }
        }
        let left_keys: Vec<&str> = left_key_strs.iter().map(|s| s.as_str()).collect();
        let right_keys: Vec<&str> = on.iter().map(|(_, r)| r.as_str()).collect();

        let right_index = HashIndex::build(&right_docs, &right_keys);

        let mut results = probe_hash_index(&ProbeParams {
            probe_docs: &left_docs,
            index: &right_index,
            index_docs: &right_docs,
            probe_keys: &left_keys,
            join_type,
            limit,
            probe_collection: "inline_left",
            index_collection: "inline_right",
            emit_unmatched_right: true,
        });

        if !post_filter_bytes.is_empty() {
            let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
                zerompk::from_msgpack(post_filter_bytes).unwrap_or_default();
            if !filters.is_empty() {
                results.retain(|row| super::binary_row_matches_filters(row, &filters));
            }
        }

        if !projection.is_empty() && !projection.iter().any(|p| p == "*") {
            for row in &mut results {
                *row = super::binary_row_project(row, projection);
            }
        }

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(task, payload)
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
        small_collection: &str,
        broadcast_data: &[u8],
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
        projection: &[String],
        post_filter_bytes: &[u8],
    ) -> Response {
        debug!(
            core = self.core_id,
            %large_collection,
            %small_collection,
            broadcast_bytes = broadcast_data.len(),
            keys = on.len(),
            %join_type,
            "broadcast join"
        );

        // Deserialize broadcast (small) side.
        // Format: raw msgpack from broadcast_raw — concatenated arrays of
        // {id, data} maps from each core's document scan. The `data` field
        // contains the actual document bytes.
        let small_docs_raw: Vec<(String, Vec<u8>)> =
            super::super::super::response_codec::decode_raw_scan_to_docs(broadcast_data);

        tracing::warn!(
            core = self.core_id,
            small_count = small_docs_raw.len(),
            broadcast_len = broadcast_data.len(),
            "broadcast join: decoded small side"
        );

        let scan_limit = (limit * 10).min(50000);
        let large_docs = match self.scan_collection(tid, large_collection, scan_limit) {
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
        // Broadcast join: NEVER emit unmatched right-side rows per-core.
        // The Control Plane handles global unmatched emission after merging
        // matched IDs from all cores (see dispatch.rs post-join dedup).
        let mut results = probe_hash_index(&ProbeParams {
            probe_docs: &large_docs,
            index: &small_index,
            index_docs: &small_docs_raw,
            probe_keys: &large_keys,
            join_type,
            limit,
            probe_collection: large_collection,
            index_collection: small_collection,
            emit_unmatched_right: false,
        });

        // Apply post-join WHERE filters (binary).
        if !post_filter_bytes.is_empty() {
            let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
                zerompk::from_msgpack(post_filter_bytes).unwrap_or_default();
            if !filters.is_empty() {
                results.retain(|row| super::binary_row_matches_filters(row, &filters));
            }
        }

        // Apply post-join projection (binary).
        if !projection.is_empty() && !projection.iter().any(|p| p == "*") {
            for row in &mut results {
                *row = super::binary_row_project(row, projection);
            }
        }

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(task, payload)
    }
}
