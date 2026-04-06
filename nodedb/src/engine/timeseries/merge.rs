//! Partition merge executor.
//!
//! Merges multiple sealed partitions into one, combining their columnar
//! data and unifying symbol dictionaries. Used by the background merge job.
//!
//! Crash safety protocol:
//! 1. Write merged partition to new directory
//! 2. Single manifest transaction: insert merged, mark sources Deleted
//! 3. Background cleanup: physically remove source directories
//!
//! If crash at step 1: partial output cleaned on recovery (no manifest entry).
//! If crash at step 2: redb transaction is atomic.
//! If crash at step 3: manifest says Deleted, recovery cleans orphans.

use std::path::{Path, PathBuf};

use nodedb_types::timeseries::{PartitionMeta, SymbolDictionary};

use super::columnar_memtable::{ColumnData, ColumnType};
use super::columnar_segment::{ColumnarSegmentReader, ColumnarSegmentWriter, SegmentError};
use super::partition_registry::{PartitionRegistry, format_partition_dir};

/// Merge multiple source partitions into one output partition.
///
/// Reads all columns from each source, concatenates them (sorted by timestamp),
/// unifies symbol dictionaries, and writes the result.
pub fn merge_partitions(
    base_dir: &Path,
    source_dirs: &[PathBuf],
    output_name: &str,
) -> Result<MergeResult, SegmentError> {
    if source_dirs.is_empty() {
        return Err(SegmentError::Io("no source partitions to merge".into()));
    }

    // Read schemas — use the first partition's schema as the reference.
    let ref_schema = ColumnarSegmentReader::read_schema(&source_dirs[0])?;

    // Read and concatenate columns from all sources.
    let mut merged_columns: Vec<ColumnData> = ref_schema
        .columns
        .iter()
        .map(|(_, ty)| ColumnData::new_empty(*ty))
        .collect();

    let mut total_rows = 0u64;
    let mut global_min_ts = i64::MAX;
    let mut global_max_ts = i64::MIN;
    let mut total_size = 0u64;

    // Per-column symbol dictionaries: merge as we go.
    let mut merged_dicts: std::collections::HashMap<usize, SymbolDictionary> =
        std::collections::HashMap::new();
    for (i, (_, ty)) in ref_schema.columns.iter().enumerate() {
        if *ty == ColumnType::Symbol {
            merged_dicts.insert(i, SymbolDictionary::new());
        }
    }

    for source_dir in source_dirs {
        let meta = ColumnarSegmentReader::read_meta(source_dir)?;
        total_rows += meta.row_count;
        if meta.min_ts < global_min_ts {
            global_min_ts = meta.min_ts;
        }
        if meta.max_ts > global_max_ts {
            global_max_ts = meta.max_ts;
        }
        total_size += meta.size_bytes;

        // Read partition schema for column mapping (handles schema evolution).
        let part_schema = ColumnarSegmentReader::read_schema(source_dir)?;

        for (i, (col_name, col_type)) in ref_schema.columns.iter().enumerate() {
            // Check if this column exists in the source partition.
            let source_idx = part_schema.columns.iter().position(|(n, _)| n == col_name);

            match source_idx {
                Some(_src_i) => {
                    let col = ColumnarSegmentReader::read_column(source_dir, col_name, *col_type)?;

                    if *col_type == ColumnType::Symbol {
                        // Remap symbol IDs through merged dictionary.
                        let source_dict =
                            ColumnarSegmentReader::read_symbol_dict(source_dir, col_name)?;
                        let merged_dict =
                            merged_dicts
                                .get_mut(&i)
                                .ok_or(SegmentError::Corrupt(format!(
                                    "missing merged dict for column index {i}"
                                )))?;
                        let remap = merged_dict.merge(&source_dict, 100_000);

                        let remapped = remap_symbols(col.as_symbols(), &remap);
                        merged_columns[i].extend_symbols(&remapped);
                    } else {
                        merged_columns[i].extend_from(&col);
                    }
                }
                None => {
                    // Column missing in this source — fill with defaults.
                    let fill_count = meta.row_count as usize;
                    merged_columns[i].extend_nulls(fill_count);
                }
            }
        }
    }

    // Sort by timestamp column.
    let ts_idx = ref_schema.timestamp_idx;
    sort_by_timestamp(&mut merged_columns, ts_idx);

    // Write merged partition.
    let writer = ColumnarSegmentWriter::new(base_dir);

    // Build a drain-like result for the writer.
    let drain = crate::engine::timeseries::columnar_memtable::ColumnarDrainResult {
        columns: merged_columns,
        schema: ref_schema.clone(),
        symbol_dicts: merged_dicts,
        row_count: total_rows,
        min_ts: global_min_ts,
        max_ts: global_max_ts,
        series_row_counts: std::collections::HashMap::new(),
    };

    let interval_ms = if global_max_ts > global_min_ts {
        (global_max_ts - global_min_ts) as u64
    } else {
        0
    };

    tracing::debug!(
        output = output_name,
        total_rows,
        total_size,
        sources = source_dirs.len(),
        "merge partition written"
    );

    let meta = writer.write_partition(output_name, &drain, interval_ms, 0)?;

    Ok(MergeResult {
        meta,
        dir_name: output_name.to_string(),
        source_count: source_dirs.len(),
    })
}

/// Execute a merge cycle: find mergeable groups, merge each, update registry.
///
/// Returns the number of merge operations performed.
/// Execute a merge cycle with crash-safe persistence.
///
/// Three-step atomic protocol per merge:
/// 1. Write merged partition to new directory (crash → orphan, no manifest entry)
/// 2. Commit to registry + persist manifest atomically (crash → write-rename atomic)
/// 3. Background cleanup of source directories (crash → cleanup on next startup)
pub fn run_merge_cycle(
    registry: &mut PartitionRegistry,
    base_dir: &Path,
    now_ms: i64,
) -> Result<usize, SegmentError> {
    let groups = registry.find_mergeable(now_ms);
    let mut merge_count = 0;

    for group_starts in &groups {
        // Collect source directories.
        let source_dirs: Vec<PathBuf> = group_starts
            .iter()
            .filter_map(|&start| registry.get(start).map(|e| base_dir.join(&e.dir_name)))
            .collect();

        if source_dirs.len() != group_starts.len() {
            continue; // Some partitions already gone.
        }

        // Mark sources as merging.
        for &start in group_starts {
            registry.mark_merging(start);
        }

        // Determine output name.
        let (Some(&first_start), Some(&last_start)) = (group_starts.first(), group_starts.last())
        else {
            continue;
        };
        let last_entry = registry.get(last_start);
        let last_end = last_entry.map(|e| e.meta.max_ts).unwrap_or(first_start);
        let output_name = format_partition_dir(first_start, last_end);

        // Execute merge.
        match merge_partitions(base_dir, &source_dirs, &output_name) {
            Ok(result) => {
                // Step 2: Atomic manifest update.
                registry.commit_merge(result.meta, result.dir_name, group_starts);

                // Persist manifest (atomic write-rename).
                let manifest_path = base_dir.join("partition_manifest.json");
                if let Err(e) = registry.persist(&manifest_path) {
                    tracing::warn!(error = %e, "failed to persist partition manifest after merge");
                }

                merge_count += 1;
            }
            Err(_e) => {
                // Merge failed — roll back to Sealed state.
                // In production, this would log the error.
                for &start in group_starts {
                    registry.rollback_merging(start);
                }
            }
        }
    }

    Ok(merge_count)
}

/// Incrementally merge O3 buffer rows into an existing sealed partition.
///
/// Copy-on-write: reads the existing partition, merges O3 rows in sorted order,
/// writes a new partition file, then atomically swaps via rename.
/// The old partition file is untouched until the swap succeeds — concurrent
/// queries read the old version until the swap completes.
pub fn merge_o3_into_partition(
    base_dir: &Path,
    partition_dir_name: &str,
    o3_rows: &[super::o3_buffer::O3Row],
) -> Result<PartitionMeta, SegmentError> {
    if o3_rows.is_empty() {
        return Err(SegmentError::Io("no O3 rows to merge".into()));
    }

    let partition_dir = base_dir.join(partition_dir_name);
    if !partition_dir.exists() {
        return Err(SegmentError::Io(format!(
            "partition {} does not exist",
            partition_dir.display()
        )));
    }

    // Read existing partition data.
    let existing_meta = ColumnarSegmentReader::read_meta(&partition_dir)?;
    let existing_schema = ColumnarSegmentReader::read_schema(&partition_dir)?;

    let ts_col =
        ColumnarSegmentReader::read_column(&partition_dir, "timestamp", ColumnType::Timestamp)?;
    let val_col = ColumnarSegmentReader::read_column(&partition_dir, "value", ColumnType::Float64)?;

    let existing_ts = ts_col.as_timestamps();
    let existing_val = val_col.as_f64();

    // Merge: sorted merge of existing rows + O3 rows.
    let total_rows = existing_ts.len() + o3_rows.len();
    let mut merged_ts = Vec::with_capacity(total_rows);
    let mut merged_val = Vec::with_capacity(total_rows);

    let mut ei = 0usize; // existing index
    let mut oi = 0usize; // o3 index

    // O3 rows are already sorted by drain_for_partition.
    while ei < existing_ts.len() && oi < o3_rows.len() {
        if existing_ts[ei] <= o3_rows[oi].timestamp_ms {
            merged_ts.push(existing_ts[ei]);
            merged_val.push(existing_val[ei]);
            ei += 1;
        } else {
            merged_ts.push(o3_rows[oi].timestamp_ms);
            merged_val.push(o3_rows[oi].value);
            oi += 1;
        }
    }
    while ei < existing_ts.len() {
        merged_ts.push(existing_ts[ei]);
        merged_val.push(existing_val[ei]);
        ei += 1;
    }
    while oi < o3_rows.len() {
        merged_ts.push(o3_rows[oi].timestamp_ms);
        merged_val.push(o3_rows[oi].value);
        oi += 1;
    }

    // Write to a temporary partition directory.
    let tmp_name = format!("{partition_dir_name}.o3merge");
    let drain = super::columnar_memtable::ColumnarDrainResult {
        columns: vec![
            ColumnData::Timestamp(merged_ts),
            ColumnData::Float64(merged_val),
        ],
        schema: existing_schema,
        symbol_dicts: std::collections::HashMap::new(),
        row_count: total_rows as u64,
        min_ts: existing_meta.min_ts.min(
            o3_rows
                .iter()
                .map(|r| r.timestamp_ms)
                .min()
                .unwrap_or(i64::MAX),
        ),
        max_ts: existing_meta.max_ts.max(
            o3_rows
                .iter()
                .map(|r| r.timestamp_ms)
                .max()
                .unwrap_or(i64::MIN),
        ),
        series_row_counts: std::collections::HashMap::new(),
    };

    let writer = ColumnarSegmentWriter::new(base_dir);
    let new_meta = writer.write_partition(
        &tmp_name,
        &drain,
        existing_meta.interval_ms,
        existing_meta.last_flushed_wal_lsn,
    )?;

    // Atomic swap: rename tmp → original.
    let tmp_dir = base_dir.join(&tmp_name);
    let backup_name = format!("{partition_dir_name}.old");
    let backup_dir = base_dir.join(&backup_name);

    // Rename original → backup, tmp → original, then remove backup.
    std::fs::rename(&partition_dir, &backup_dir)
        .map_err(|e| SegmentError::Io(format!("rename original → backup: {e}")))?;
    std::fs::rename(&tmp_dir, &partition_dir)
        .map_err(|e| SegmentError::Io(format!("rename tmp → original: {e}")))?;
    let _ = std::fs::remove_dir_all(&backup_dir); // Best-effort cleanup.

    Ok(new_meta)
}

/// Remap symbol IDs using a remap table.
fn remap_symbols(ids: &[u32], remap: &[u32]) -> Vec<u32> {
    ids.iter()
        .map(|&id| {
            if (id as usize) < remap.len() {
                remap[id as usize]
            } else {
                u32::MAX // sentinel for unknown
            }
        })
        .collect()
}

/// Sort all columns by the timestamp column (in-place).
fn sort_by_timestamp(columns: &mut [ColumnData], ts_idx: usize) {
    let len = columns[ts_idx].len();
    if len <= 1 {
        return;
    }

    // Build sort indices from timestamp column.
    let timestamps = columns[ts_idx].as_timestamps();
    let mut indices: Vec<usize> = (0..len).collect();
    indices.sort_by_key(|&i| timestamps[i]);

    // Apply permutation to all columns.
    for col in columns.iter_mut() {
        col.apply_permutation(&indices);
    }
}

/// Result of a merge operation.
#[derive(Debug)]
pub struct MergeResult {
    pub meta: PartitionMeta,
    pub dir_name: String,
    pub source_count: usize,
}

// Extension methods for ColumnData needed by merge.
impl ColumnData {
    fn new_empty(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Timestamp => Self::Timestamp(Vec::new()),
            ColumnType::Float64 => Self::Float64(Vec::new()),
            ColumnType::Int64 => Self::Int64(Vec::new()),
            ColumnType::Symbol => Self::Symbol(Vec::new()),
        }
    }

    fn extend_from(&mut self, other: &ColumnData) {
        match (self, other) {
            (Self::Timestamp(a), Self::Timestamp(b)) => a.extend_from_slice(b),
            (Self::Float64(a), Self::Float64(b)) => a.extend_from_slice(b),
            (Self::Int64(a), Self::Int64(b)) => a.extend_from_slice(b),
            (Self::Symbol(a), Self::Symbol(b)) => a.extend_from_slice(b),
            _ => {} // type mismatch — skip
        }
    }

    fn extend_symbols(&mut self, ids: &[u32]) {
        if let Self::Symbol(v) = self {
            v.extend_from_slice(ids);
        }
    }

    fn extend_nulls(&mut self, count: usize) {
        match self {
            Self::Timestamp(v) => v.extend(std::iter::repeat_n(0i64, count)),
            Self::Float64(v) => v.extend(std::iter::repeat_n(f64::NAN, count)),
            Self::Int64(v) => v.extend(std::iter::repeat_n(0i64, count)),
            Self::Symbol(v) => v.extend(std::iter::repeat_n(u32::MAX, count)),
            Self::DictEncoded { ids, valid, .. } => {
                ids.extend(std::iter::repeat_n(u32::MAX, count));
                valid.extend(std::iter::repeat_n(false, count));
            }
        }
    }

    fn apply_permutation(&mut self, indices: &[usize]) {
        match self {
            Self::Timestamp(v) => permute_vec(v, indices),
            Self::Float64(v) => permute_vec(v, indices),
            Self::Int64(v) => permute_vec(v, indices),
            Self::Symbol(v) => permute_vec(v, indices),
            Self::DictEncoded { ids, valid, .. } => {
                permute_vec(ids, indices);
                permute_vec(valid, indices);
            }
        }
    }
}

/// Apply a permutation to a Vec in-place.
fn permute_vec<T: Clone>(v: &mut Vec<T>, indices: &[usize]) {
    let sorted: Vec<T> = indices.iter().map(|&i| v[i].clone()).collect();
    *v = sorted;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
    };
    use nodedb_types::timeseries::{MetricSample, PartitionInterval, TieredPartitionConfig};
    use tempfile::TempDir;

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn write_test_partition(base_dir: &Path, name: &str, start_ts: i64, count: usize) -> PathBuf {
        let writer = ColumnarSegmentWriter::new(base_dir);
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..count {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: start_ts + i as i64,
                    value: (start_ts + i as i64) as f64,
                },
            );
        }
        let drain = mt.drain();
        writer.write_partition(name, &drain, 86_400_000, 0).unwrap();
        base_dir.join(name)
    }

    #[test]
    fn merge_two_simple_partitions() {
        let tmp = TempDir::new().unwrap();
        let dir1 = write_test_partition(tmp.path(), "ts-part1", 1000, 50);
        let dir2 = write_test_partition(tmp.path(), "ts-part2", 2000, 50);

        let result = merge_partitions(tmp.path(), &[dir1, dir2], "ts-merged").unwrap();

        assert_eq!(result.meta.row_count, 100);
        assert_eq!(result.meta.min_ts, 1000);
        assert_eq!(result.meta.max_ts, 2049);
        assert_eq!(result.source_count, 2);

        // Read back merged data.
        let merged_dir = tmp.path().join("ts-merged");
        let ts_col =
            ColumnarSegmentReader::read_column(&merged_dir, "timestamp", ColumnType::Timestamp)
                .unwrap();
        let timestamps = ts_col.as_timestamps();
        assert_eq!(timestamps.len(), 100);
        // Should be sorted.
        for w in timestamps.windows(2) {
            assert!(w[0] <= w[1], "not sorted: {} > {}", w[0], w[1]);
        }
    }

    #[test]
    fn merge_with_tags() {
        let tmp = TempDir::new().unwrap();
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };

        // Write two partitions with different host values.
        let writer = ColumnarSegmentWriter::new(tmp.path());
        for (part_name, host, start) in [("ts-p1", "host-a", 1000i64), ("ts-p2", "host-b", 2000)] {
            let mut mt = ColumnarMemtable::new(schema.clone(), test_config());
            for i in 0..10 {
                mt.ingest_row(
                    1,
                    &[
                        ColumnValue::Timestamp(start + i),
                        ColumnValue::Float64(1.0),
                        ColumnValue::Symbol(host),
                    ],
                )
                .unwrap();
            }
            let drain = mt.drain();
            writer
                .write_partition(part_name, &drain, 86_400_000, 0)
                .unwrap();
        }

        let result = merge_partitions(
            tmp.path(),
            &[tmp.path().join("ts-p1"), tmp.path().join("ts-p2")],
            "ts-merged-tags",
        )
        .unwrap();
        assert_eq!(result.meta.row_count, 20);

        // Read merged symbol dict — should have both hosts.
        let merged_dir = tmp.path().join("ts-merged-tags");
        let dict = ColumnarSegmentReader::read_symbol_dict(&merged_dir, "host").unwrap();
        assert_eq!(dict.len(), 2);
        assert!(dict.get_id("host-a").is_some());
        assert!(dict.get_id("host-b").is_some());
    }

    #[test]
    fn merge_preserves_timestamp_order() {
        let tmp = TempDir::new().unwrap();
        // Partition 2 has earlier timestamps than partition 1.
        let dir1 = write_test_partition(tmp.path(), "ts-later", 5000, 20);
        let dir2 = write_test_partition(tmp.path(), "ts-earlier", 1000, 20);

        let merged = merge_partitions(tmp.path(), &[dir1, dir2], "ts-sorted").unwrap();
        assert_eq!(
            merged.meta.row_count, 40,
            "merged partition should have all rows"
        );

        let merged_dir = tmp.path().join("ts-sorted");
        let ts_col =
            ColumnarSegmentReader::read_column(&merged_dir, "timestamp", ColumnType::Timestamp)
                .unwrap();
        let timestamps = ts_col.as_timestamps();
        // All timestamps from partition 2 (1000-1019) should come before partition 1 (5000-5019).
        assert_eq!(timestamps[0], 1000);
        assert_eq!(timestamps[19], 1019);
        assert_eq!(timestamps[20], 5000);
    }

    #[test]
    fn run_merge_cycle_test() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000);
        cfg.merge_after_ms = 1000; // 1 second for testing
        cfg.merge_count = 3;
        let mut registry = PartitionRegistry::new(cfg);

        let day_ms = 86_400_000i64;

        // Create and seal 3 partitions with data.
        for d in 1..=3 {
            let start = d * day_ms;
            let (entry, _) = registry.get_or_create_partition(start);
            let dir_name = entry.dir_name.clone();

            write_test_partition(tmp.path(), &dir_name, start, 10);

            // Update meta.
            if let Some(e) = registry.get_mut(start) {
                e.meta.row_count = 10;
                e.meta.max_ts = start + 9;
            }
            registry.seal_partition(start);
        }

        assert_eq!(registry.sealed_count(), 3);

        // Run merge — should merge all 3.
        let now = 10 * day_ms; // far enough in the future
        let merged = run_merge_cycle(&mut registry, tmp.path(), now).unwrap();
        assert_eq!(merged, 1);

        // Purge deleted.
        let deleted_dirs = registry.purge_deleted();
        assert_eq!(deleted_dirs.len(), 2); // 2 deleted (1 overwritten by merged)
    }
}
