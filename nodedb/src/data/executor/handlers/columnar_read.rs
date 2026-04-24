//! Columnar base scan handler.
//!
//! Reads rows from the `MutationEngine` memtable, applies projection,
//! WHERE filter predicates, and limit. Used by plain columnar and spatial
//! collections.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

/// Parameters for a columnar base scan. Bundled as a struct because the
/// raw parameter list exceeds the project's too-many-arguments bound.
pub(in crate::data::executor) struct ColumnarScanParams<'a> {
    pub collection: &'a str,
    pub projection: &'a [String],
    pub limit: usize,
    pub filters: &'a [u8],
    /// RLS filter bytes — wiring is the responsibility of a separate
    /// enforcement pass; the base scan handler itself does not consume
    /// them (hence the `_` destructure).
    #[allow(dead_code)]
    pub rls_filters: &'a [u8],
    pub sort_keys: &'a [(String, bool)],
    /// Bitemporal system-time cutoff: drop rows with `_ts_system > cutoff`.
    /// `None` is a current-state read (no filter applied).
    pub system_as_of_ms: Option<i64>,
    /// Bitemporal valid-time point: drop rows whose
    /// `[_ts_valid_from, _ts_valid_until)` interval does not contain this
    /// point. `None` skips valid-time filtering entirely.
    pub valid_at_ms: Option<i64>,
}

impl CoreLoop {
    /// Execute a base columnar scan: read from MutationEngine memtable.
    pub(in crate::data::executor) fn execute_columnar_scan(
        &mut self,
        task: &ExecutionTask,
        params: ColumnarScanParams<'_>,
    ) -> Response {
        let ColumnarScanParams {
            collection,
            projection,
            limit,
            filters,
            rls_filters: _,
            sort_keys,
            system_as_of_ms,
            valid_at_ms,
        } = params;
        let limit = if limit == 0 { 1000 } else { limit };

        // Scan-quiesce gate.
        let _scan_guard =
            match self.acquire_scan_guard(task, task.request.tenant_id.as_u32(), collection) {
                Ok(g) => g,
                Err(resp) => return resp,
            };

        let engine_key = (task.request.tenant_id, collection.to_string());

        let engine = match self.columnar_engines.get(&engine_key) {
            Some(e) => e,
            None => {
                // Empty result for missing collection.
                return match response_codec::encode_json_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        let schema = engine.schema();

        let filter_predicates: Vec<ScanFilter> = if !filters.is_empty() {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Collect matched rows as (row_values, json_object) pairs. We keep
        // the raw `Vec<Value>` for sort-key comparison — the JSON form is
        // emitted only after ORDER BY + limit are applied. When no sort
        // is requested we short-circuit the limit enforcement inside the
        // loop to avoid materialising the entire memtable.
        let mut matched: Vec<(Vec<nodedb_types::value::Value>, serde_json::Value)> = Vec::new();
        let scan_budget = if sort_keys.is_empty() {
            limit.saturating_mul(10).max(limit)
        } else {
            usize::MAX
        };
        // Resolve hidden bitemporal column positions once; `None` means
        // the collection is not bitemporal, so the per-row filter is a
        // no-op regardless of `system_as_of_ms` / `valid_at_ms` values.
        let ts_system_idx = schema.columns.iter().position(|c| c.name == "_ts_system");
        let ts_valid_from_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "_ts_valid_from");
        let ts_valid_until_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "_ts_valid_until");

        for row in engine.scan_memtable_rows().take(scan_budget) {
            if !bitemporal_row_visible(
                &row,
                ts_system_idx,
                ts_valid_from_idx,
                ts_valid_until_idx,
                system_as_of_ms,
                valid_at_ms,
            ) {
                continue;
            }
            if !filter_predicates.is_empty()
                && !row_matches_filters(&row, schema, &filter_predicates)
            {
                continue;
            }
            let mut obj = serde_json::Map::new();
            for (i, col_def) in schema.columns.iter().enumerate() {
                if !projection.is_empty() && !projection.iter().any(|p| p == &col_def.name) {
                    continue;
                }
                if i < row.len() {
                    obj.insert(col_def.name.clone(), value_to_json(&row[i]));
                }
            }
            matched.push((row, serde_json::Value::Object(obj)));
            if sort_keys.is_empty() && matched.len() >= limit {
                break;
            }
        }

        if !sort_keys.is_empty() {
            matched.sort_by(|(a, _), (b, _)| sort_rows_by_keys(a, b, schema, sort_keys));
        }

        let results: Vec<serde_json::Value> =
            matched.into_iter().take(limit).map(|(_, j)| j).collect();

        match response_codec::encode_json_vec(&results) {
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

/// Convert a `nodedb_types::Value` to `serde_json::Value` for response encoding.
pub(in crate::data::executor) fn value_to_json(
    val: &nodedb_types::value::Value,
) -> serde_json::Value {
    use nodedb_types::value::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Uuid(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Geometry(g) => serde_json::to_value(g).unwrap_or(serde_json::Value::Null),
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    }
}

/// Compare two columnar rows by the planner's sort-key list. Missing
/// columns compare as `Null`; unknown column names fall through as
/// `Equal` so a mis-typed ORDER BY at least keeps scan order stable.
pub(in crate::data::executor) fn sort_rows_by_keys(
    a: &[nodedb_types::value::Value],
    b: &[nodedb_types::value::Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    use nodedb_types::value::Value;
    for (field, ascending) in sort_keys {
        let idx = match schema.columns.iter().position(|c| &c.name == field) {
            Some(i) => i,
            None => continue,
        };
        let av = a.get(idx).unwrap_or(&Value::Null);
        let bv = b.get(idx).unwrap_or(&Value::Null);
        let ord = compare_values(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return if *ascending { ord } else { ord.reverse() };
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_values(
    a: &nodedb_types::value::Value,
    b: &nodedb_types::value::Value,
) -> std::cmp::Ordering {
    use nodedb_types::value::Value;
    // Null sorts last in ascending order (Postgres default).
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Integer(x), Value::Float(y)) => (*x as f64)
            .partial_cmp(y)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(x), Value::Integer(y)) => x
            .partial_cmp(&(*y as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.unix_millis().cmp(&y.unix_millis()),
        // Fallback: compare debug-formatted forms so the sort is
        // deterministic for exotic types that happen to coincide in a
        // sort key column.
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

/// Check whether a memtable row satisfies all filter predicates.
///
/// Returns `true` if every filter passes (AND semantics). An unknown field or
/// out-of-bounds column index causes the row to be excluded.
pub(in crate::data::executor) fn row_matches_filters(
    row: &[nodedb_types::value::Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    filters: &[ScanFilter],
) -> bool {
    for filter in filters {
        if filter.op == FilterOp::MatchAll {
            continue;
        }
        let col_idx = match schema.columns.iter().position(|c| c.name == filter.field) {
            Some(i) => i,
            None => continue, // unknown field — skip predicate
        };
        if col_idx >= row.len() {
            return false;
        }
        if !eval_filter(&row[col_idx], filter.op, &filter.value) {
            return false;
        }
    }
    true
}

/// Evaluate a single filter predicate against a row value.
fn eval_filter(
    val: &nodedb_types::value::Value,
    op: FilterOp,
    filter_val: &nodedb_types::value::Value,
) -> bool {
    use nodedb_types::value::Value;

    let val_f64 = match val {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        _ => None,
    };
    let filter_f64 = match filter_val {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        _ => None,
    };

    let val_str = match val {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    };
    let filter_str = match filter_val {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    };

    match op {
        FilterOp::Eq => {
            if let (Some(a), Some(b)) = (val_f64, filter_f64) {
                a == b
            } else if let (Some(a), Some(b)) = (val_str, filter_str) {
                a == b
            } else {
                false
            }
        }
        FilterOp::Ne => {
            if let (Some(a), Some(b)) = (val_f64, filter_f64) {
                a != b
            } else if let (Some(a), Some(b)) = (val_str, filter_str) {
                a != b
            } else {
                true
            }
        }
        FilterOp::Gt => val_f64.zip(filter_f64).is_some_and(|(a, b)| a > b),
        FilterOp::Gte => val_f64.zip(filter_f64).is_some_and(|(a, b)| a >= b),
        FilterOp::Lt => val_f64.zip(filter_f64).is_some_and(|(a, b)| a < b),
        FilterOp::Lte => val_f64.zip(filter_f64).is_some_and(|(a, b)| a <= b),
        FilterOp::IsNull => matches!(val, Value::Null),
        FilterOp::IsNotNull => !matches!(val, Value::Null),
        _ => true, // unknown/unsupported op — pass through
    }
}

/// Write a timeseries columnar memtable cell value directly as msgpack bytes.
///
/// Encodes the column value at the given row index directly into `buf`
/// without intermediate decoding. Used by timeseries raw_scan and aggregate
/// handlers that still use the internal `ColumnarMemtable`.
pub(in crate::data::executor) fn emit_column_value(
    buf: &mut Vec<u8>,
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    col_type: &crate::engine::timeseries::columnar_memtable::ColumnType,
    col_data: &crate::engine::timeseries::columnar_memtable::ColumnData,
    row_idx: usize,
) {
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData as TsColumnData, ColumnType as TsColumnType,
    };
    match col_type {
        TsColumnType::Timestamp => {
            nodedb_query::msgpack_scan::write_i64(buf, col_data.as_timestamps()[row_idx]);
        }
        TsColumnType::Float64 => {
            let v = col_data.as_f64()[row_idx];
            if v.is_finite() {
                nodedb_query::msgpack_scan::write_f64(buf, v);
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
        TsColumnType::Symbol => {
            if let TsColumnData::Symbol(ids) = col_data {
                let sym_id = ids[row_idx];
                if let Some(s) = mt.symbol_dict(col_idx).and_then(|dict| dict.get(sym_id)) {
                    nodedb_query::msgpack_scan::write_str(buf, s);
                } else {
                    nodedb_query::msgpack_scan::write_null(buf);
                }
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
        TsColumnType::Int64 => {
            if let TsColumnData::Int64(vals) = col_data {
                nodedb_query::msgpack_scan::write_i64(buf, vals[row_idx]);
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
    }
}

/// Bitemporal row-level visibility predicate.
///
/// - `system_as_of_ms`: when `Some`, any row whose `_ts_system` value
///   exceeds the cutoff is hidden (write happened after the query's
///   system-time horizon).
/// - `valid_at_ms`: when `Some`, the row's
///   `[_ts_valid_from, _ts_valid_until)` interval must contain this
///   point.
///
/// Rows from non-bitemporal collections have no `_ts_system` column and
/// all three indices are `None`; the function returns `true` unconditionally.
fn bitemporal_row_visible(
    row: &[nodedb_types::value::Value],
    ts_system_idx: Option<usize>,
    ts_valid_from_idx: Option<usize>,
    ts_valid_until_idx: Option<usize>,
    system_as_of_ms: Option<i64>,
    valid_at_ms: Option<i64>,
) -> bool {
    use nodedb_types::value::Value;
    if let Some(cutoff) = system_as_of_ms
        && let Some(idx) = ts_system_idx
        && let Some(v) = row.get(idx)
    {
        let ts = match v {
            Value::Integer(i) => *i,
            Value::DateTime(dt) => dt.micros / 1000,
            _ => return false,
        };
        if ts > cutoff {
            return false;
        }
    }
    if let Some(point) = valid_at_ms {
        let vf = ts_valid_from_idx
            .and_then(|i| row.get(i))
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                Value::DateTime(dt) => Some(dt.micros / 1000),
                _ => None,
            })
            .unwrap_or(i64::MIN);
        let vu = ts_valid_until_idx
            .and_then(|i| row.get(i))
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                Value::DateTime(dt) => Some(dt.micros / 1000),
                _ => None,
            })
            .unwrap_or(i64::MAX);
        if point < vf || point >= vu {
            return false;
        }
    }
    true
}
