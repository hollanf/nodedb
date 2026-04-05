//! Entry points: aggregate_memtable and aggregate_partition.
//!
//! Builds filter bitmask, applies sparse index skip, then dispatches
//! to the tiered grouping strategies.

use std::collections::HashMap;
use std::path::Path;

use nodedb_query::simd_filter;

use super::super::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};
use super::super::columnar_segment::ColumnarSegmentReader;
use super::super::grouped_filter;
use super::strategies::dispatch_grouping;
use super::types::{GroupedAggResult, resolve_schema};
use crate::bridge::scan_filter::ScanFilter;

/// Aggregate from a columnar memtable with GROUP BY + optional time_bucket.
pub fn aggregate_memtable(
    mt: &ColumnarMemtable,
    group_by: &[String],
    aggregates: &[(String, String)],
    filters: &[ScanFilter],
    time_range: (i64, i64),
    bucket_interval_ms: i64,
) -> Option<GroupedAggResult> {
    let schema = mt.schema();
    let num_aggs = aggregates.len();
    let row_count = mt.row_count() as usize;
    if row_count == 0 {
        return Some(GroupedAggResult::new(num_aggs));
    }

    let resolved = resolve_schema(&schema.columns, group_by, aggregates)?;

    let col_refs: Vec<Option<&ColumnData>> = (0..schema.columns.len())
        .map(|i| Some(mt.column(i)))
        .collect();
    let sym_lookup = |col_idx: usize| mt.symbol_dict(col_idx);

    let mut mask = if !filters.is_empty() {
        grouped_filter::eval_filters_to_bitmask(
            filters,
            &schema.columns,
            &col_refs,
            &sym_lookup,
            row_count,
        )?
    } else {
        simd_filter::bitmask_all(row_count)
    };

    let has_time_range = time_range.0 > 0 || time_range.1 < i64::MAX;
    if has_time_range {
        let timestamps = mt.column(resolved.ts_idx).as_timestamps();
        let rt = simd_filter::filter_runtime();
        let ts_mask = (rt.range_i64)(timestamps, time_range.0, time_range.1);
        mask = simd_filter::bitmask_and(&mask, &ts_mask);
    }

    if simd_filter::popcount(&mask) == 0 {
        return Some(GroupedAggResult::new(num_aggs));
    }

    let timestamps = if bucket_interval_ms > 0 {
        Some(mt.column(resolved.ts_idx).as_timestamps())
    } else {
        None
    };

    Some(dispatch_grouping(
        &resolved,
        &col_refs,
        &mask,
        row_count,
        num_aggs,
        group_by,
        &sym_lookup,
        timestamps,
        bucket_interval_ms,
    ))
}

/// Aggregate from a sealed disk partition with GROUP BY + optional time_bucket.
pub fn aggregate_partition(
    partition_dir: &Path,
    group_by: &[String],
    aggregates: &[(String, String)],
    filters: &[ScanFilter],
    time_range: (i64, i64),
    needed_columns: &[String],
    bucket_interval_ms: i64,
) -> Option<GroupedAggResult> {
    let num_aggs = aggregates.len();

    let schema = ColumnarSegmentReader::read_schema(partition_dir).ok()?;
    let meta = ColumnarSegmentReader::read_meta(partition_dir).ok()?;
    let row_count = meta.row_count as usize;
    if row_count == 0 {
        return Some(GroupedAggResult::new(num_aggs));
    }

    let resolved = resolve_schema(&schema.columns, group_by, aggregates)?;

    // Load sparse index for block-level skip.
    let sparse_idx = ColumnarSegmentReader::read_sparse_index(partition_dir)
        .ok()
        .flatten();

    // Determine surviving blocks (if sparse index available).
    let surviving_blocks: Option<Vec<usize>> = sparse_idx
        .as_ref()
        .map(|idx| idx.filter_blocks(time_range.0, time_range.1, &[]));

    // If sparse index skips blocks and we have surviving blocks, use
    // block-level read. Otherwise read full columns.
    let total_blocks = sparse_idx
        .as_ref()
        .map(|idx| idx.block_count())
        .unwrap_or(0);
    let use_block_read = surviving_blocks
        .as_ref()
        .is_some_and(|sb| !sb.is_empty() && sb.len() < total_blocks);

    let col_data: Vec<Option<ColumnData>> = schema
        .columns
        .iter()
        .map(|(name, ty)| {
            if needed_columns.is_empty() || needed_columns.iter().any(|n| n == name) {
                let codec = meta.column_stats.get(name).map(|s| s.codec);
                if use_block_read {
                    // Block-level read: only decode surviving blocks.
                    ColumnarSegmentReader::read_column_blocks(
                        partition_dir,
                        name,
                        *ty,
                        codec,
                        surviving_blocks.as_ref().unwrap(),
                    )
                    .ok()
                    .map(|(data, _ranges)| data)
                } else {
                    ColumnarSegmentReader::read_column_with_codec(partition_dir, name, *ty, codec)
                        .ok()
                }
            } else {
                None
            }
        })
        .collect();

    // When block-level read was used, row_count is the number of
    // decoded rows (only surviving blocks), not the partition total.
    let effective_row_count = if use_block_read {
        col_data
            .iter()
            .find_map(|c| {
                c.as_ref().map(|d| match d {
                    ColumnData::Timestamp(v) => v.len(),
                    ColumnData::Float64(v) => v.len(),
                    ColumnData::Int64(v) => v.len(),
                    ColumnData::Symbol(v) => v.len(),
                    ColumnData::DictEncoded { ids, .. } => ids.len(),
                })
            })
            .unwrap_or(0)
    } else {
        row_count
    };

    let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
        .filter_map(|(i, (name, _))| {
            if needed_columns.is_empty() || needed_columns.iter().any(|n| n == name) {
                ColumnarSegmentReader::read_symbol_dict(partition_dir, name)
                    .ok()
                    .map(|dict| (i, dict))
            } else {
                None
            }
        })
        .collect();

    // Build bitmask over the decoded data.
    // When block-level read was used, data is already filtered to surviving
    // blocks — just need predicate + time range filters on the decoded rows.
    // When full read was used, need time range + sparse skip + predicate.
    let has_time_range = time_range.0 > 0 || time_range.1 < i64::MAX;

    let mut mask = if use_block_read {
        // Block-level read already filtered by sparse index.
        // Only need time range filter within surviving blocks.
        if has_time_range {
            let ts_col = col_data.get(resolved.ts_idx).and_then(|d| d.as_ref())?;
            let timestamps = ts_col.as_timestamps();
            let rt = simd_filter::filter_runtime();
            (rt.range_i64)(timestamps, time_range.0, time_range.1)
        } else {
            simd_filter::bitmask_all(effective_row_count)
        }
    } else {
        let partition_fully_in_range =
            !has_time_range || (meta.min_ts >= time_range.0 && meta.max_ts <= time_range.1);
        let m = if partition_fully_in_range {
            simd_filter::bitmask_all(effective_row_count)
        } else {
            let ts_col = col_data.get(resolved.ts_idx).and_then(|d| d.as_ref())?;
            let timestamps = ts_col.as_timestamps();
            let rt = simd_filter::filter_runtime();
            (rt.range_i64)(timestamps, time_range.0, time_range.1)
        };
        // Apply sparse index block-level skip on full data.
        if let Some(ref idx) = sparse_idx {
            let mut m = m;
            grouped_filter::apply_sparse_skip(&mut m, idx, time_range, effective_row_count);
            m
        } else {
            m
        }
    };

    // Apply predicate filters.
    if !filters.is_empty() {
        let col_refs_tmp: Vec<Option<&ColumnData>> = col_data.iter().map(|c| c.as_ref()).collect();
        let sym_lookup_tmp =
            |col_idx: usize| -> Option<&nodedb_types::timeseries::SymbolDictionary> {
                sym_dicts.get(&col_idx)
            };
        if let Some(filter_mask) = grouped_filter::eval_filters_to_bitmask(
            filters,
            &schema.columns,
            &col_refs_tmp,
            &sym_lookup_tmp,
            effective_row_count,
        ) {
            mask = simd_filter::bitmask_and(&mask, &filter_mask);
        }
    }

    if simd_filter::popcount(&mask) == 0 {
        return Some(GroupedAggResult::new(num_aggs));
    }

    let col_refs: Vec<Option<&ColumnData>> = col_data.iter().map(|c| c.as_ref()).collect();
    let sym_lookup = |col_idx: usize| -> Option<&nodedb_types::timeseries::SymbolDictionary> {
        sym_dicts.get(&col_idx)
    };

    let timestamps = if bucket_interval_ms > 0 {
        col_data
            .get(resolved.ts_idx)
            .and_then(|d| d.as_ref())
            .map(|d| d.as_timestamps())
    } else {
        None
    };

    Some(dispatch_grouping(
        &resolved,
        &col_refs,
        &mask,
        effective_row_count,
        num_aggs,
        group_by,
        &sym_lookup,
        timestamps,
        bucket_interval_ms,
    ))
}
