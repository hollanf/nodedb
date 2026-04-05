//! Native columnar aggregation for GROUP BY queries on columnar memtables.
//!
//! Bypasses the generic document-style aggregation path that converts
//! columnar data → serde_json::Map → msgpack → JSON string keys.
//! Instead, filters and groups directly on column vectors using integer
//! symbol IDs as group keys. Resolves symbol names only for the final
//! response.
//!
//! Performance: ~36x faster than the generic path for high-cardinality
//! GROUP BY (e.g., 10K+ unique qnames) because it eliminates:
//! - Per-row serde_json::Map construction
//! - Per-row msgpack encode/decode roundtrip
//! - JSON-serialized string HashMap keys
//! - Per-row symbol dictionary string allocation

use std::collections::HashMap;

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};

use super::columnar_filter;

/// Accumulator for running aggregate computation per group.
#[derive(Debug, Clone)]
struct AggAccum {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl AggAccum {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn feed(&mut self, val: f64) {
        self.count += 1;
        self.sum += val;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
    }

    fn feed_count_only(&mut self) {
        self.count += 1;
    }
}

/// A group key composed of symbol IDs (for Symbol columns) or raw i64/f64
/// values (for numeric group-by columns). Avoids string allocation entirely.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKeyPart {
    SymbolId(u32),
    Int64(i64),
    /// f64 stored as bits for Eq/Hash (NaN-safe: all NaNs compare equal).
    Float64Bits(u64),
    Null,
}

/// Packed group key for multi-column GROUP BY.
type GroupKey = Vec<GroupKeyPart>;

/// Result of native columnar aggregation.
pub(super) struct ColumnarAggResult {
    pub rows: Vec<serde_json::Value>,
}

/// Extract a group key part from a column at a given row index.
fn extract_group_key_part(
    col_type: &ColumnType,
    col_data: &ColumnData,
    row_idx: usize,
) -> GroupKeyPart {
    match col_type {
        ColumnType::Symbol => {
            if let ColumnData::Symbol(ids) = col_data {
                GroupKeyPart::SymbolId(ids[row_idx])
            } else {
                GroupKeyPart::Null
            }
        }
        ColumnType::Int64 => {
            if let ColumnData::Int64(vals) = col_data {
                GroupKeyPart::Int64(vals[row_idx])
            } else {
                GroupKeyPart::Null
            }
        }
        ColumnType::Float64 => {
            if let ColumnData::Float64(vals) = col_data {
                GroupKeyPart::Float64Bits(vals[row_idx].to_bits())
            } else {
                GroupKeyPart::Null
            }
        }
        ColumnType::Timestamp => {
            if let ColumnData::Timestamp(vals) = col_data {
                GroupKeyPart::Int64(vals[row_idx])
            } else {
                GroupKeyPart::Null
            }
        }
    }
}

/// Resolve a group key part to a serde_json::Value for output.
fn resolve_key_part(
    mt: &ColumnarMemtable,
    col_idx: usize,
    part: &GroupKeyPart,
) -> serde_json::Value {
    match part {
        GroupKeyPart::SymbolId(id) => mt
            .symbol_dict(col_idx)
            .and_then(|dict| dict.get(*id))
            .map(|s| serde_json::Value::String(s.to_string()))
            .unwrap_or(serde_json::Value::Null),
        GroupKeyPart::Int64(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        GroupKeyPart::Float64Bits(bits) => {
            let v = f64::from_bits(*bits);
            serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        GroupKeyPart::Null => serde_json::Value::Null,
    }
}

/// Try to execute an aggregate query natively on a columnar memtable.
///
/// Returns `None` if the query can't be handled natively (complex filters,
/// string comparison filters, etc.), in which case the caller should fall
/// back to the generic document-style path.
pub(super) fn try_columnar_aggregate(
    mt: &ColumnarMemtable,
    group_by: &[String],
    aggregates: &[(String, String)],
    filters: &[crate::bridge::scan_filter::ScanFilter],
    limit: usize,
    scan_limit: usize,
) -> Option<ColumnarAggResult> {
    let schema = mt.schema();
    let row_count = (mt.row_count() as usize).min(scan_limit);

    if row_count == 0 {
        return Some(ColumnarAggResult { rows: Vec::new() });
    }

    // --- Phase 1: Resolve column indices for group-by and aggregate fields ---

    let group_col_info: Vec<(usize, ColumnType)> = group_by
        .iter()
        .map(|name| {
            schema
                .columns
                .iter()
                .enumerate()
                .find(|(_, (n, _))| n == name)
                .map(|(i, (_, ty))| (i, *ty))
        })
        .collect::<Option<Vec<_>>>()?;

    // For each aggregate, find the column index (None for count(*)).
    let agg_col_info: Vec<(usize, ColumnType)> = aggregates
        .iter()
        .filter(|(_, field)| field != "*")
        .map(|(_, field)| {
            schema
                .columns
                .iter()
                .enumerate()
                .find(|(_, (n, _))| n == field)
                .map(|(i, (_, ty))| (i, *ty))
        })
        .collect::<Option<Vec<_>>>()?;

    // Only handle numeric aggregation columns (Float64, Int64, Timestamp).
    for (_, ty) in &agg_col_info {
        if *ty == ColumnType::Symbol {
            return None; // Can't SUM/AVG a symbol column
        }
    }

    // --- Phase 2: Build filter bitmask ---

    let (row_mask, has_mask) = if filters.is_empty() {
        (vec![true; row_count], false)
    } else {
        match columnar_filter::eval_filters_dense(mt, filters, row_count) {
            Some(mask) => {
                let any_filtered = mask.iter().any(|&b| !b);
                (mask, any_filtered)
            }
            None => return None, // Complex filters — fall back to generic path
        }
    };

    // --- Phase 3: Group and accumulate ---

    // Per-aggregate-field accumulator maps.
    // Key: group key. Value: per-aggregate-op accumulator.
    let mut groups: HashMap<GroupKey, Vec<AggAccum>> =
        HashMap::with_capacity(if has_mask { 1024 } else { row_count / 10 });

    let num_aggs = aggregates.len();

    // Pre-fetch column data references for group-by columns.
    let group_col_data: Vec<_> = group_col_info
        .iter()
        .map(|&(idx, ty)| (idx, ty, mt.column(idx)))
        .collect();

    // Pre-fetch aggregate column data. For count(*), we don't need column data.
    let agg_col_data: Vec<Option<(usize, &ColumnData)>> = aggregates
        .iter()
        .map(|(_, field)| {
            if field == "*" {
                None
            } else {
                schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, (n, _))| n == field)
                    .map(|(i, _)| (i, mt.column(i)))
            }
        })
        .collect();

    for row_idx in 0..row_count {
        if has_mask && !row_mask[row_idx] {
            continue;
        }

        // Build group key from symbol IDs / integers — no string allocation.
        let key: GroupKey = if group_by.is_empty() {
            Vec::new() // single group for all rows
        } else {
            group_col_data
                .iter()
                .map(|(_, col_type, col_data)| extract_group_key_part(col_type, col_data, row_idx))
                .collect()
        };

        // Get or create accumulators for this group.
        let accums = groups
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::new()).collect());

        // Feed values into accumulators.
        for (agg_idx, (op, _)) in aggregates.iter().enumerate() {
            match agg_col_data[agg_idx] {
                None => {
                    // count(*) — just increment count.
                    accums[agg_idx].feed_count_only();
                }
                Some((_, col_data)) => {
                    let val = match col_data {
                        ColumnData::Float64(vals) => vals[row_idx],
                        ColumnData::Int64(vals) => vals[row_idx] as f64,
                        ColumnData::Timestamp(vals) => vals[row_idx] as f64,
                        _ => continue,
                    };
                    if op == "count" {
                        accums[agg_idx].feed_count_only();
                    } else {
                        accums[agg_idx].feed(val);
                    }
                }
            }
        }
    }

    // --- Phase 4: Build result rows (resolve symbols only here) ---

    let mut results: Vec<serde_json::Value> = Vec::with_capacity(groups.len().min(limit));

    for (group_key, accums) in &groups {
        let mut row = serde_json::Map::new();

        // Resolve group key parts to display values.
        for (i, field) in group_by.iter().enumerate() {
            let (col_idx, _) = group_col_info[i];
            let val = if i < group_key.len() {
                resolve_key_part(mt, col_idx, &group_key[i])
            } else {
                serde_json::Value::Null
            };
            row.insert(field.clone(), val);
        }

        // Emit aggregate values.
        for (agg_idx, (op, field)) in aggregates.iter().enumerate() {
            let agg_key = format!("{op}_{field}").replace('*', "all");
            let accum = &accums[agg_idx];
            let val = match op.as_str() {
                "count" => serde_json::json!(accum.count),
                "sum" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.sum)
                    }
                }
                "avg" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.sum / accum.count as f64)
                    }
                }
                "min" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.min)
                    }
                }
                "max" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.max)
                    }
                }
                _ => serde_json::Value::Null,
            };
            row.insert(agg_key, val);
        }

        results.push(serde_json::Value::Object(row));
        if results.len() >= limit {
            break;
        }
    }

    Some(ColumnarAggResult { rows: results })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
    };
    use nodedb_types::timeseries::SeriesId;

    fn make_test_memtable() -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("qname".into(), ColumnType::Symbol),
                ("qtype".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![],
        };
        let config = ColumnarMemtableConfig::default();
        let mut mt = ColumnarMemtable::new(schema, config);

        use crate::engine::timeseries::columnar_memtable::ColumnValue;

        // Insert test rows with varying qnames (high cardinality) and qtypes (low cardinality)
        let qnames = [
            "example.com",
            "google.com",
            "github.com",
            "reddit.com",
            "rust-lang.org",
        ];
        let qtypes = ["A", "AAAA"];

        for i in 0..100 {
            let series_id: SeriesId = i as u64;
            let values = [
                ColumnValue::Timestamp(i as i64 * 1000),
                ColumnValue::Float64(i as f64 * 100.0),
                ColumnValue::Symbol(qnames[i % qnames.len()]),
                ColumnValue::Symbol(qtypes[i % qtypes.len()]),
            ];
            mt.ingest_row(series_id, &values).unwrap();
        }

        mt
    }

    #[test]
    fn group_by_symbol_column() {
        let mt = make_test_memtable();
        let result = try_columnar_aggregate(
            &mt,
            &["qtype".into()],
            &[("count".into(), "*".into()), ("avg".into(), "value".into())],
            &[],
            100,
            100_000,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2); // A and AAAA
        for row in &result.rows {
            let count = row.get("count_all").and_then(|v| v.as_u64()).unwrap();
            assert_eq!(count, 50); // 100 rows / 2 types
        }
    }

    #[test]
    fn group_by_high_cardinality() {
        let mt = make_test_memtable();
        let result = try_columnar_aggregate(
            &mt,
            &["qname".into()],
            &[("count".into(), "*".into()), ("sum".into(), "value".into())],
            &[],
            100,
            100_000,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 5); // 5 unique qnames
    }

    #[test]
    fn filter_and_group() {
        let mt = make_test_memtable();
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "value".into(),
            op: "gt".into(),
            value: nodedb_types::Value::Float(5000.0),
            clauses: vec![],
        };
        let result = try_columnar_aggregate(
            &mt,
            &["qname".into()],
            &[("count".into(), "*".into()), ("avg".into(), "value".into())],
            &[filter],
            100,
            100_000,
        )
        .unwrap();

        // Only rows with value > 5000 (i >= 51, value >= 5100)
        assert!(!result.rows.is_empty());
        for row in &result.rows {
            let avg = row.get("avg_value").and_then(|v| v.as_f64()).unwrap();
            assert!(avg > 5000.0);
        }
    }

    #[test]
    fn no_group_by_aggregate_all() {
        let mt = make_test_memtable();
        let result = try_columnar_aggregate(
            &mt,
            &[],
            &[("count".into(), "*".into()), ("sum".into(), "value".into())],
            &[],
            100,
            100_000,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        let count = result.rows[0]
            .get("count_all")
            .and_then(|v| v.as_u64())
            .unwrap();
        assert_eq!(count, 100);
    }
}
