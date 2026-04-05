//! Columnar predicate evaluation on raw column vectors.
//!
//! Evaluates `ScanFilter` predicates directly on typed columnar data
//! (`Vec<f64>`, `Vec<i64>`, `Vec<u32>` symbol IDs) without constructing
//! JSON rows. Used by timeseries scans (memtable + sealed partitions),
//! columnar aggregation, and any path that filters columnar data.
//!
//! Returns a bitmask of passing rows. Falls back to `None` for filter
//! patterns that can't be evaluated on columnar data (OR clauses, string
//! ordering, unsupported operators).

use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};
use nodedb_types::timeseries::SymbolDictionary;

/// Abstraction over columnar data sources (memtable or sealed partition).
/// Provides column lookup by name and symbol dictionary access.
pub(super) trait ColumnarSource {
    fn resolve_column(&self, name: &str) -> Option<(usize, ColumnType, &ColumnData)>;
    fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary>;
}

/// Adapter for in-memory memtable.
impl ColumnarSource for ColumnarMemtable {
    fn resolve_column(&self, name: &str) -> Option<(usize, ColumnType, &ColumnData)> {
        let schema = self.schema();
        let pos = schema.columns.iter().position(|(n, _)| n == name)?;
        let (_, ty) = &schema.columns[pos];
        Some((pos, *ty, self.column(pos)))
    }

    fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary> {
        ColumnarMemtable::symbol_dict(self, col_idx)
    }
}

/// Adapter for sealed partition data read from disk.
pub(super) struct PartitionColumns<'a> {
    pub schema: &'a [(String, ColumnType)],
    pub columns: &'a [Option<ColumnData>],
    pub sym_dicts: &'a std::collections::HashMap<usize, SymbolDictionary>,
}

impl ColumnarSource for PartitionColumns<'_> {
    fn resolve_column(&self, name: &str) -> Option<(usize, ColumnType, &ColumnData)> {
        let pos = self.schema.iter().position(|(n, _)| n == name)?;
        let (_, ty) = &self.schema[pos];
        let data = self.columns.get(pos)?.as_ref()?;
        Some((pos, *ty, data))
    }

    fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary> {
        self.sym_dicts.get(&col_idx)
    }
}

/// Evaluate filters on a sparse set of row indices (e.g., after time-range pruning).
///
/// Returns a bitmask aligned with `indices` — `mask[i]` is true if
/// `indices[i]` passes all filters. Returns `None` if any filter can't
/// be evaluated on columnar data (caller should fall back to JSON-based
/// evaluation).
pub(super) fn eval_filters_sparse(
    src: &dyn ColumnarSource,
    filters: &[ScanFilter],
    indices: &[u32],
) -> Option<Vec<bool>> {
    let mut mask = vec![true; indices.len()];

    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return None;
        }

        let (col_pos, col_type, col_data) = src.resolve_column(&f.field)?;

        match col_type {
            ColumnType::Float64 => {
                let fv = f.value.as_f64()?;
                if let ColumnData::Float64(vals) = col_data {
                    for (mi, &idx) in indices.iter().enumerate() {
                        if !mask[mi] {
                            continue;
                        }
                        mask[mi] = eval_numeric_f64(vals[idx as usize], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Int64 => {
                let fv = f.value.as_i64()?;
                if let ColumnData::Int64(vals) = col_data {
                    for (mi, &idx) in indices.iter().enumerate() {
                        if !mask[mi] {
                            continue;
                        }
                        mask[mi] = eval_numeric_i64(vals[idx as usize], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Timestamp => {
                let fv = f.value.as_i64()?;
                if let ColumnData::Timestamp(vals) = col_data {
                    for (mi, &idx) in indices.iter().enumerate() {
                        if !mask[mi] {
                            continue;
                        }
                        mask[mi] = eval_numeric_i64(vals[idx as usize], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Symbol => {
                eval_symbol_filter(src, col_pos, col_data, f, indices, &mut mask)?;
            }
        }
    }

    Some(mask)
}

/// Evaluate filters on a contiguous row range `0..row_count`.
pub(super) fn eval_filters_dense(
    src: &dyn ColumnarSource,
    filters: &[ScanFilter],
    row_count: usize,
) -> Option<Vec<bool>> {
    let mut mask = vec![true; row_count];

    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return None;
        }

        let (col_pos, col_type, col_data) = src.resolve_column(&f.field)?;

        match col_type {
            ColumnType::Float64 => {
                let fv = f.value.as_f64()?;
                if let ColumnData::Float64(vals) = col_data {
                    for i in 0..row_count {
                        if !mask[i] {
                            continue;
                        }
                        mask[i] = eval_numeric_f64(vals[i], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Int64 => {
                let fv = f.value.as_i64()?;
                if let ColumnData::Int64(vals) = col_data {
                    for i in 0..row_count {
                        if !mask[i] {
                            continue;
                        }
                        mask[i] = eval_numeric_i64(vals[i], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Timestamp => {
                let fv = f.value.as_i64()?;
                if let ColumnData::Timestamp(vals) = col_data {
                    for i in 0..row_count {
                        if !mask[i] {
                            continue;
                        }
                        mask[i] = eval_numeric_i64(vals[i], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Symbol => {
                let indices: Vec<u32> = (0..row_count as u32).collect();
                eval_symbol_filter(src, col_pos, col_data, f, &indices, &mut mask)?;
            }
        }
    }

    Some(mask)
}

/// Apply passing indices from a bitmask.
pub(super) fn apply_mask(indices: &[u32], mask: &[bool]) -> Vec<u32> {
    indices
        .iter()
        .zip(mask.iter())
        .filter(|&(_, pass)| *pass)
        .map(|(idx, _)| *idx)
        .collect()
}

// ---------------------------------------------------------------------------
// SIMD bitmask filter evaluation (Vec<u64>, 1 bit per row)
// ---------------------------------------------------------------------------

/// Evaluate filters on a contiguous row range `0..row_count`, returning a
/// packed `Vec<u64>` bitmask (bit *i* set = row *i* passes all filters).
///
/// Uses SIMD kernels for numeric and symbol equality comparisons.
/// Returns `None` for filter patterns that can't be evaluated (OR clauses,
/// unsupported operators) — caller should fall back to `eval_filters_dense`.
pub(super) fn eval_filters_bitmask(
    src: &dyn ColumnarSource,
    filters: &[ScanFilter],
    row_count: usize,
) -> Option<Vec<u64>> {
    use nodedb_query::simd_filter::{self, filter_runtime};

    let rt = filter_runtime();
    let mut mask = simd_filter::bitmask_all(row_count);

    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return None; // OR clauses not supported in bitmask path
        }

        let (col_pos, col_type, col_data) = src.resolve_column(&f.field)?;

        let filter_mask = match col_type {
            ColumnType::Float64 => {
                let fv = f.value.as_f64()?;
                let ColumnData::Float64(vals) = col_data else {
                    return None;
                };
                let slice = &vals[..row_count.min(vals.len())];
                match f.op.as_str() {
                    "gt" => (rt.gt_f64)(slice, fv),
                    "gte" => (rt.gte_f64)(slice, fv),
                    "lt" => (rt.lt_f64)(slice, fv),
                    "lte" => (rt.lte_f64)(slice, fv),
                    "eq" => {
                        // f64 equality — use epsilon range.
                        let lo = fv - f64::EPSILON;
                        let hi = fv + f64::EPSILON;
                        let a = (rt.gte_f64)(slice, lo);
                        let b = (rt.lte_f64)(slice, hi);
                        simd_filter::bitmask_and(&a, &b)
                    }
                    "ne" => {
                        let lo = fv - f64::EPSILON;
                        let hi = fv + f64::EPSILON;
                        let a = (rt.gte_f64)(slice, lo);
                        let b = (rt.lte_f64)(slice, hi);
                        let eq = simd_filter::bitmask_and(&a, &b);
                        simd_filter::bitmask_not(&eq, row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Int64 => {
                let fv = f.value.as_i64()?;
                let ColumnData::Int64(vals) = col_data else {
                    return None;
                };
                let slice = &vals[..row_count.min(vals.len())];
                match f.op.as_str() {
                    "gt" => (rt.gt_i64)(slice, fv),
                    "gte" => (rt.gte_i64)(slice, fv),
                    "lt" => (rt.lt_i64)(slice, fv),
                    "lte" => (rt.lte_i64)(slice, fv),
                    "eq" => {
                        let a = (rt.gte_i64)(slice, fv);
                        let b = (rt.lte_i64)(slice, fv);
                        simd_filter::bitmask_and(&a, &b)
                    }
                    "ne" => {
                        let a = (rt.gte_i64)(slice, fv);
                        let b = (rt.lte_i64)(slice, fv);
                        let eq = simd_filter::bitmask_and(&a, &b);
                        simd_filter::bitmask_not(&eq, row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Timestamp => {
                let fv = f.value.as_i64()?;
                let ColumnData::Timestamp(vals) = col_data else {
                    return None;
                };
                let slice = &vals[..row_count.min(vals.len())];
                match f.op.as_str() {
                    "gt" => (rt.gt_i64)(slice, fv),
                    "gte" => (rt.gte_i64)(slice, fv),
                    "lt" => (rt.lt_i64)(slice, fv),
                    "lte" => (rt.lte_i64)(slice, fv),
                    "eq" => {
                        let a = (rt.gte_i64)(slice, fv);
                        let b = (rt.lte_i64)(slice, fv);
                        simd_filter::bitmask_and(&a, &b)
                    }
                    "ne" => {
                        let a = (rt.gte_i64)(slice, fv);
                        let b = (rt.lte_i64)(slice, fv);
                        let eq = simd_filter::bitmask_and(&a, &b);
                        simd_filter::bitmask_not(&eq, row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Symbol => {
                let filter_str = f.value.as_str()?;
                let dict = src.symbol_dict(col_pos)?;
                let ColumnData::Symbol(ids) = col_data else {
                    return None;
                };
                let slice = &ids[..row_count.min(ids.len())];
                match f.op.as_str() {
                    "eq" => {
                        if let Some(target_id) = dict.get_id(filter_str) {
                            (rt.eq_u32)(slice, target_id)
                        } else {
                            vec![0u64; simd_filter::words_for(row_count)]
                        }
                    }
                    "ne" => {
                        if let Some(target_id) = dict.get_id(filter_str) {
                            (rt.ne_u32)(slice, target_id)
                        } else {
                            simd_filter::bitmask_all(row_count)
                        }
                    }
                    _ => return None,
                }
            }
        };

        mask = simd_filter::bitmask_and(&mask, &filter_mask);
    }

    Some(mask)
}

#[inline]
fn eval_numeric_f64(v: f64, op: &str, fv: f64) -> Option<bool> {
    Some(match op {
        "gt" => v > fv,
        "gte" => v >= fv,
        "lt" => v < fv,
        "lte" => v <= fv,
        "eq" => (v - fv).abs() < f64::EPSILON,
        "ne" => (v - fv).abs() >= f64::EPSILON,
        _ => return None,
    })
}

#[inline]
fn eval_numeric_i64(v: i64, op: &str, fv: i64) -> Option<bool> {
    Some(match op {
        "gt" => v > fv,
        "gte" => v >= fv,
        "lt" => v < fv,
        "lte" => v <= fv,
        "eq" => v == fv,
        "ne" => v != fv,
        _ => return None,
    })
}

fn eval_symbol_filter(
    src: &dyn ColumnarSource,
    col_pos: usize,
    col_data: &ColumnData,
    f: &ScanFilter,
    indices: &[u32],
    mask: &mut [bool],
) -> Option<()> {
    let filter_str = f.value.as_str()?;
    let dict = src.symbol_dict(col_pos)?;
    let ColumnData::Symbol(ids) = col_data else {
        return None;
    };

    match f.op.as_str() {
        "eq" => {
            if let Some(target_id) = dict.get_id(filter_str) {
                for (mi, &idx) in indices.iter().enumerate() {
                    if mask[mi] {
                        mask[mi] = ids[idx as usize] == target_id;
                    }
                }
            } else {
                for m in mask.iter_mut() {
                    *m = false;
                }
            }
        }
        "ne" => {
            if let Some(target_id) = dict.get_id(filter_str) {
                for (mi, &idx) in indices.iter().enumerate() {
                    if mask[mi] {
                        mask[mi] = ids[idx as usize] != target_id;
                    }
                }
            }
        }
        "contains" => {
            let matching_ids: std::collections::HashSet<u32> = (0..dict.len() as u32)
                .filter(|&id| dict.get(id).is_some_and(|s| s.contains(filter_str)))
                .collect();
            for (mi, &idx) in indices.iter().enumerate() {
                if mask[mi] {
                    mask[mi] = matching_ids.contains(&ids[idx as usize]);
                }
            }
        }
        _ => return None,
    }

    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
    };
    use nodedb_types::timeseries::SeriesId;

    fn make_test_mt() -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![],
        };
        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        let hosts = ["web-1", "web-2", "db-1"];
        for i in 0..30u64 {
            let sid: SeriesId = i;
            mt.ingest_row(
                sid,
                &[
                    ColumnValue::Timestamp(i as i64 * 1000),
                    ColumnValue::Float64(i as f64 * 10.0),
                    ColumnValue::Symbol(hosts[(i % 3) as usize]),
                ],
            )
            .unwrap();
        }
        mt
    }

    #[test]
    fn dense_float_filter() {
        let mt = make_test_mt();
        let f = ScanFilter {
            field: "value".into(),
            op: "gt".into(),
            value: nodedb_types::Value::Float(200.0),
            clauses: vec![],
        };
        let mask = eval_filters_dense(&mt, &[f], 30).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 9);
    }

    #[test]
    fn sparse_symbol_eq_filter() {
        let mt = make_test_mt();
        let indices: Vec<u32> = (0..30).collect();
        let f = ScanFilter {
            field: "host".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("db-1".into()),
            clauses: vec![],
        };
        let mask = eval_filters_sparse(&mt, &[f], &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 10);
    }

    #[test]
    fn symbol_eq_not_in_dict() {
        let mt = make_test_mt();
        let indices: Vec<u32> = (0..30).collect();
        let f = ScanFilter {
            field: "host".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("nonexistent".into()),
            clauses: vec![],
        };
        let mask = eval_filters_sparse(&mt, &[f], &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 0);
    }

    #[test]
    fn combined_filters() {
        let mt = make_test_mt();
        let indices: Vec<u32> = (0..30).collect();
        let filters = vec![
            ScanFilter {
                field: "value".into(),
                op: "gte".into(),
                value: nodedb_types::Value::Float(100.0),
                clauses: vec![],
            },
            ScanFilter {
                field: "host".into(),
                op: "eq".into(),
                value: nodedb_types::Value::String("web-1".into()),
                clauses: vec![],
            },
        ];
        let mask = eval_filters_sparse(&mt, &filters, &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 6);
    }

    #[test]
    fn or_clause_returns_none() {
        let mt = make_test_mt();
        let f = ScanFilter {
            field: "value".into(),
            op: "or".into(),
            value: nodedb_types::Value::Null,
            clauses: vec![vec![ScanFilter {
                field: "value".into(),
                op: "gt".into(),
                value: nodedb_types::Value::Float(100.0),
                clauses: vec![],
            }]],
        };
        assert!(eval_filters_dense(&mt, &[f], 30).is_none());
    }

    #[test]
    fn partition_columns_adapter() {
        // Simulate sealed partition data.
        let schema = vec![
            ("timestamp".into(), ColumnType::Timestamp),
            ("value".into(), ColumnType::Float64),
            ("host".into(), ColumnType::Symbol),
        ];
        let ts: Vec<i64> = (0..10).map(|i| i * 1000).collect();
        let vals: Vec<f64> = (0..10).map(|i| i as f64 * 100.0).collect();
        let sym_ids: Vec<u32> = (0..10).map(|i| (i % 2) as u32).collect(); // alternating 0, 1

        let columns: Vec<Option<ColumnData>> = vec![
            Some(ColumnData::Timestamp(ts)),
            Some(ColumnData::Float64(vals)),
            Some(ColumnData::Symbol(sym_ids)),
        ];

        let mut sym_dicts = std::collections::HashMap::new();
        let mut dict = SymbolDictionary::new();
        dict.resolve("alpha", u32::MAX); // id 0
        dict.resolve("beta", u32::MAX); // id 1
        sym_dicts.insert(2, dict);

        let src = PartitionColumns {
            schema: &schema,
            columns: &columns,
            sym_dicts: &sym_dicts,
        };

        let indices: Vec<u32> = (0..10).collect();

        // Filter: value > 500
        let f = ScanFilter {
            field: "value".into(),
            op: "gt".into(),
            value: nodedb_types::Value::Float(500.0),
            clauses: vec![],
        };
        let mask = eval_filters_sparse(&src, &[f], &indices).unwrap();
        let passing: usize = mask.iter().filter(|&&b| b).count();
        assert_eq!(passing, 4); // 600, 700, 800, 900

        // Filter: host = 'alpha'
        let f2 = ScanFilter {
            field: "host".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("alpha".into()),
            clauses: vec![],
        };
        let mask2 = eval_filters_sparse(&src, &[f2], &indices).unwrap();
        let passing2: usize = mask2.iter().filter(|&&b| b).count();
        assert_eq!(passing2, 5); // indices 0, 2, 4, 6, 8
    }
}
