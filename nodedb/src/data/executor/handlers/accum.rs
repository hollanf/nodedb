//! Streaming aggregate accumulators for the generic GROUP BY path.
//!
//! Each `AggAccum` variant holds only the derived state needed to compute the
//! final aggregate result — no raw document bytes are retained.  Memory per
//! group is O(num_aggregates × accumulator_size) regardless of how many
//! documents match that group.

use std::collections::HashSet;

use crate::bridge::physical_plan::AggregateSpec;
use nodedb_types::Value;

/// Maximum items collected by materializing aggregates (`array_agg`,
/// `array_agg_distinct`, `percentile_cont`, `string_agg`).
pub(super) const ARRAY_AGG_CAP: usize = 10_000;

/// Per-(group, aggregate-spec) running accumulator.
pub(super) enum AggAccum {
    /// count(*) or count(field).
    Count { n: u64 },
    /// sum / avg: Kahan-compensated running sum + count.
    SumAvg { sum: f64, comp: f64, n: u64 },
    /// min.
    Min { best: Option<Value> },
    /// max.
    Max { best: Option<Value> },
    /// count_distinct: set of raw msgpack bytes.
    CountDistinct { seen: HashSet<Vec<u8>> },
    /// stddev / variance variants: Welford M2 accumulator.
    Welford { n: u64, mean: f64, m2: f64 },
    /// approx_count_distinct: HyperLogLog.
    Hll {
        hll: nodedb_types::approx::HyperLogLog,
    },
    /// approx_percentile: t-digest.
    TDigest {
        digest: nodedb_types::approx::TDigest,
    },
    /// approx_topk: space-saving.
    TopK {
        ss: nodedb_types::approx::SpaceSaving,
        k: usize,
    },
    /// array_agg (capped).
    ArrayAgg { values: Vec<Value> },
    /// array_agg_distinct (capped).
    ArrayAggDistinct {
        seen: HashSet<Vec<u8>>,
        values: Vec<Value>,
    },
    /// percentile_cont (capped).
    PercentileCont { values: Vec<f64>, pct: f64 },
    /// string_agg / group_concat (capped).
    StringAgg { parts: Vec<String> },
}

impl AggAccum {
    pub(super) fn new(agg: &AggregateSpec) -> Self {
        match agg.function.as_str() {
            "count" => AggAccum::Count { n: 0 },
            "sum" | "avg" => AggAccum::SumAvg {
                sum: 0.0,
                comp: 0.0,
                n: 0,
            },
            "min" => AggAccum::Min { best: None },
            "max" => AggAccum::Max { best: None },
            "count_distinct" => AggAccum::CountDistinct {
                seen: HashSet::new(),
            },
            "stddev" | "stddev_pop" | "stddev_samp" | "variance" | "var_pop" | "var_samp" => {
                AggAccum::Welford {
                    n: 0,
                    mean: 0.0,
                    m2: 0.0,
                }
            }
            "approx_count_distinct" => AggAccum::Hll {
                hll: nodedb_types::approx::HyperLogLog::new(),
            },
            "approx_percentile" => AggAccum::TDigest {
                digest: nodedb_types::approx::TDigest::new(),
            },
            "approx_topk" => {
                let k: usize = agg
                    .field
                    .find(':')
                    .and_then(|i| agg.field[..i].parse().ok())
                    .unwrap_or(10);
                AggAccum::TopK {
                    ss: nodedb_types::approx::SpaceSaving::new(k),
                    k,
                }
            }
            "array_agg" => AggAccum::ArrayAgg { values: Vec::new() },
            "array_agg_distinct" => AggAccum::ArrayAggDistinct {
                seen: HashSet::new(),
                values: Vec::new(),
            },
            "percentile_cont" => {
                let pct = agg
                    .field
                    .find(':')
                    .and_then(|i| agg.field[..i].parse().ok())
                    .unwrap_or(0.5);
                AggAccum::PercentileCont {
                    values: Vec::new(),
                    pct,
                }
            }
            "string_agg" | "group_concat" => AggAccum::StringAgg { parts: Vec::new() },
            _ => AggAccum::Count { n: 0 },
        }
    }

    /// Feed one document into this accumulator.
    pub(super) fn feed(&mut self, agg: &AggregateSpec, doc: &[u8]) {
        use nodedb_query::msgpack_scan::aggregate_helpers as ah;
        match self {
            AggAccum::Count { n } => {
                if agg.field == "*" && agg.expr.is_none() {
                    *n += 1;
                } else if ah::extract_non_null(doc, &agg.field, agg.expr.as_ref()).is_some() {
                    *n += 1;
                }
            }
            AggAccum::SumAvg { sum, comp, n } => {
                if let Some(v) = ah::extract_f64(doc, &agg.field, agg.expr.as_ref()) {
                    let y = v - *comp;
                    let t = *sum + y;
                    *comp = (t - *sum) - y;
                    *sum = t;
                    *n += 1;
                }
            }
            AggAccum::Min { best } => {
                if let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref()) {
                    if v.is_null() {
                        return;
                    }
                    let replace = match best {
                        None => true,
                        Some(cur) => {
                            nodedb_query::value_ops::compare_values(&v, cur)
                                == std::cmp::Ordering::Less
                        }
                    };
                    if replace {
                        *best = Some(v);
                    }
                }
            }
            AggAccum::Max { best } => {
                if let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref()) {
                    if v.is_null() {
                        return;
                    }
                    let replace = match best {
                        None => true,
                        Some(cur) => {
                            nodedb_query::value_ops::compare_values(&v, cur)
                                == std::cmp::Ordering::Greater
                        }
                    };
                    if replace {
                        *best = Some(v);
                    }
                }
            }
            AggAccum::CountDistinct { seen } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref()) {
                    if bytes != [0xc0u8] {
                        seen.insert(bytes);
                    }
                }
            }
            AggAccum::Welford { n, mean, m2 } => {
                if let Some(v) = ah::extract_f64(doc, &agg.field, agg.expr.as_ref()) {
                    *n += 1;
                    let delta = v - *mean;
                    *mean += delta / *n as f64;
                    let delta2 = v - *mean;
                    *m2 += delta * delta2;
                }
            }
            AggAccum::Hll { hll } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref()) {
                    if bytes != [0xc0u8] {
                        hll.add(fnv1a(&bytes));
                    }
                }
            }
            AggAccum::TDigest { digest } => {
                let actual = field_after_colon(&agg.field);
                if let Some(v) = ah::extract_f64(doc, actual, agg.expr.as_ref()) {
                    digest.add(v);
                }
            }
            AggAccum::TopK { ss, .. } => {
                let actual = field_after_colon(&agg.field);
                if let Some(bytes) = ah::extract_bytes(doc, actual, agg.expr.as_ref()) {
                    if bytes != [0xc0u8] {
                        ss.add(fnv1a(&bytes));
                    }
                }
            }
            AggAccum::ArrayAgg { values } => {
                if values.len() < ARRAY_AGG_CAP {
                    if let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref()) {
                        if !v.is_null() {
                            values.push(v);
                        }
                    }
                }
            }
            AggAccum::ArrayAggDistinct { seen, values } => {
                if values.len() < ARRAY_AGG_CAP {
                    if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref()) {
                        if bytes != [0xc0u8] && seen.insert(bytes) {
                            if let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref()) {
                                values.push(v);
                            }
                        }
                    }
                }
            }
            AggAccum::PercentileCont { values, .. } => {
                if values.len() < ARRAY_AGG_CAP {
                    let actual = field_after_colon(&agg.field);
                    if let Some(v) = ah::extract_f64(doc, actual, agg.expr.as_ref()) {
                        values.push(v);
                    }
                }
            }
            AggAccum::StringAgg { parts } => {
                if parts.len() < ARRAY_AGG_CAP {
                    if let Some(s) = ah::extract_str(doc, &agg.field, agg.expr.as_ref()) {
                        parts.push(s);
                    }
                }
            }
        }
    }

    /// Consume the accumulator and produce the final `Value`.
    pub(super) fn finalize(self, agg: &AggregateSpec) -> Value {
        match self {
            AggAccum::Count { n } => Value::Integer(n as i64),
            AggAccum::SumAvg { sum, n, .. } => {
                if agg.function == "avg" {
                    if n == 0 {
                        Value::Null
                    } else {
                        Value::Float(sum / n as f64)
                    }
                } else {
                    Value::Float(sum)
                }
            }
            AggAccum::Min { best } => best.unwrap_or(Value::Null),
            AggAccum::Max { best } => best.unwrap_or(Value::Null),
            AggAccum::CountDistinct { seen } => Value::Integer(seen.len() as i64),
            AggAccum::Welford { n, mean: _, m2 } => {
                if n < 2 {
                    return Value::Null;
                }
                let population = matches!(
                    agg.function.as_str(),
                    "stddev" | "stddev_pop" | "variance" | "var_pop"
                );
                let divisor = if population { n as f64 } else { (n - 1) as f64 };
                let variance = m2 / divisor;
                let result = if agg.function.contains("stddev") {
                    variance.sqrt()
                } else {
                    variance
                };
                Value::Float(result)
            }
            AggAccum::Hll { hll } => Value::Integer(hll.estimate().round() as i64),
            AggAccum::TDigest { digest } => {
                let pct = agg
                    .field
                    .find(':')
                    .and_then(|i| agg.field[..i].parse().ok())
                    .unwrap_or(0.5);
                let r = digest.quantile(pct);
                if r.is_nan() {
                    Value::Null
                } else {
                    Value::Float(r)
                }
            }
            AggAccum::TopK { ss, k } => {
                let arr: Vec<Value> = ss
                    .top_k()
                    .into_iter()
                    .take(k)
                    .map(|(item, count, error)| {
                        Value::Object(
                            [
                                ("item".to_string(), Value::Integer(item as i64)),
                                ("count".to_string(), Value::Integer(count as i64)),
                                ("error".to_string(), Value::Integer(error as i64)),
                            ]
                            .into_iter()
                            .collect(),
                        )
                    })
                    .collect();
                Value::Array(arr)
            }
            AggAccum::ArrayAgg { values } => Value::Array(values),
            AggAccum::ArrayAggDistinct { values, .. } => Value::Array(values),
            AggAccum::PercentileCont { mut values, pct } => {
                if values.is_empty() {
                    return Value::Null;
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
                let lo = idx.floor() as usize;
                let hi = idx.ceil() as usize;
                let frac = idx - lo as f64;
                Value::Float(values[lo] * (1.0 - frac) + values[hi] * frac)
            }
            AggAccum::StringAgg { parts } => Value::String(parts.join(",")),
        }
    }
}

/// Per-group running state: one `AggAccum` per aggregate spec.
pub(super) struct GroupState {
    pub(super) accums: Vec<AggAccum>,
}

impl GroupState {
    pub(super) fn new(aggregates: &[AggregateSpec]) -> Self {
        Self {
            accums: aggregates.iter().map(AggAccum::new).collect(),
        }
    }

    pub(super) fn feed(&mut self, aggregates: &[AggregateSpec], doc: &[u8]) {
        for (accum, agg) in self.accums.iter_mut().zip(aggregates) {
            accum.feed(agg, doc);
        }
    }

    pub(super) fn finalize(self, aggregates: &[AggregateSpec]) -> Vec<(String, Value)> {
        self.accums
            .into_iter()
            .zip(aggregates)
            .map(|(accum, agg)| (agg.alias.clone(), accum.finalize(agg)))
            .collect()
    }
}

/// FNV-1a hash (matches the implementation in nodedb-query aggregate.rs).
#[inline]
pub(super) fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Extract the actual field name from "prefix:field" format (e.g. "0.95:latency").
#[inline]
pub(super) fn field_after_colon(field: &str) -> &str {
    field.find(':').map(|i| &field[i + 1..]).unwrap_or(field)
}
