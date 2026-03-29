//! `time_bucket(interval, timestamp)` scalar UDF for DataFusion.
//!
//! Truncates a timestamp (milliseconds) to the start of the interval bucket.
//! Used in `GROUP BY time_bucket('5m', ts)` for dashboard-style aggregation.
//!
//! Signature: `time_bucket(Utf8, Int64) → Int64`
//! - First arg: interval string (`'5m'`, `'1h'`, `'1d'`, `'30s'`)
//! - Second arg: timestamp in epoch milliseconds

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// `time_bucket(interval, timestamp)` — truncate timestamp to bucket start.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimeBucketUdf {
    signature: Signature,
}

impl TimeBucketUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // time_bucket('5m', ts_column)
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                    // time_bucket('5m', ts_column) where ts is float
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for TimeBucketUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for TimeBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "time_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Plan(
                "time_bucket requires 2 arguments: interval and timestamp".into(),
            ));
        }

        // Extract interval from first argument (always a string literal).
        let interval_ms = match &args.args[0] {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(s))) => {
                parse_interval(s)?
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution("time_bucket: interval must be a string".into())
                })?;
                if str_arr.is_empty() {
                    return Err(DataFusionError::Execution(
                        "time_bucket: empty interval array".into(),
                    ));
                }
                let s = str_arr.value(0);
                parse_interval(s)?
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "time_bucket: first argument must be an interval string".into(),
                ));
            }
        };

        // Apply bucketing to second argument (timestamp values).
        match &args.args[1] {
            ColumnarValue::Array(arr) => {
                let timestamps = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "time_bucket: timestamp must be Int64 (epoch ms)".into(),
                    )
                })?;
                let result: Int64Array = timestamps
                    .iter()
                    .map(|ts| ts.map(|t| bucket(interval_ms, t)))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Int64(Some(ts))) => {
                Ok(ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Int64(Some(bucket(interval_ms, *ts))),
                ))
            }
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Float64(Some(ts))) => {
                Ok(ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Int64(Some(bucket(interval_ms, *ts as i64))),
                ))
            }
            _ => Err(DataFusionError::Plan(
                "time_bucket: second argument must be a timestamp (Int64 epoch ms)".into(),
            )),
        }
    }
}

/// Truncate timestamp to bucket start.
fn bucket(interval_ms: i64, timestamp_ms: i64) -> i64 {
    if interval_ms <= 0 {
        return timestamp_ms;
    }
    (timestamp_ms / interval_ms) * interval_ms
}

/// Parse interval string using nodedb-types parser.
fn parse_interval(s: &str) -> DfResult<i64> {
    nodedb_types::kv_parsing::parse_interval_to_ms(s)
        .map(|ms| ms as i64)
        .map_err(|e| DataFusionError::Execution(format!("invalid interval '{s}': {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_5min() {
        let base = 1_704_067_200_000i64;
        assert_eq!(bucket(300_000, base + 120_000), base);
        assert_eq!(bucket(300_000, base + 360_000), base + 300_000);
    }

    #[test]
    fn bucket_hour() {
        let ts = 1_704_070_800_000i64;
        let ts_mid = ts + 30 * 60_000;
        assert_eq!(bucket(3_600_000, ts_mid), ts);
    }

    #[test]
    fn parse_various_intervals() {
        assert_eq!(parse_interval("5m").unwrap(), 300_000);
        assert_eq!(parse_interval("1h").unwrap(), 3_600_000);
        assert_eq!(parse_interval("1d").unwrap(), 86_400_000);
        assert_eq!(parse_interval("30s").unwrap(), 30_000);
    }

    #[test]
    fn udf_registration_and_name() {
        use datafusion::execution::FunctionRegistry;
        use datafusion::execution::context::SessionContext;
        use datafusion::logical_expr::ScalarUDF;

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::new_from_impl(TimeBucketUdf::new()));
        assert!(ctx.udf("time_bucket").is_ok());
    }
}
