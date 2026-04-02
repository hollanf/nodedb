//! Shared types and helpers for DISTRIBUTE and ALLOCATE UDFs.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, ListArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result as DfResult, ScalarValue, exec_err};
use datafusion::logical_expr::ColumnarValue;
use rust_decimal::Decimal;

/// How to distribute leftover sub-units after truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemainderMode {
    LargestRemainder,
    First,
    Last,
    RoundRobin,
}

pub fn parse_remainder_mode(s: &str) -> Result<RemainderMode, String> {
    match s.to_uppercase().as_str() {
        "LARGEST_REMAINDER" => Ok(RemainderMode::LargestRemainder),
        "FIRST" => Ok(RemainderMode::First),
        "LAST" => Ok(RemainderMode::Last),
        "ROUND_ROBIN" => Ok(RemainderMode::RoundRobin),
        other => Err(format!(
            "unknown remainder mode '{other}'. \
             Valid: LARGEST_REMAINDER, FIRST, LAST, ROUND_ROBIN"
        )),
    }
}

/// Convert a `Vec<Decimal>` to a ListArray of Decimal128.
pub fn decimals_to_list_scalar(values: &[Decimal], precision: u32) -> DfResult<ScalarValue> {
    let mantissas: Vec<Option<i128>> = values
        .iter()
        .map(|d| {
            let mut scaled = *d;
            scaled.rescale(precision);
            Some(scaled.mantissa())
        })
        .collect();

    let arr = Decimal128Array::from(mantissas)
        .with_precision_and_scale(38, precision as i8)
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "failed to create Decimal128 array: {e}"
            ))
        })?;

    Ok(ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Decimal128(38, precision as i8),
            true,
        )),
        OffsetBuffer::from_lengths([arr.len()]),
        Arc::new(arr),
        None,
    ))))
}

// ── Argument extraction helpers ──

pub fn extract_decimal_arg(arg: &ColumnarValue, name: &str) -> DfResult<Decimal> {
    match arg {
        ColumnarValue::Scalar(scalar) => scalar_to_decimal(scalar, name),
        ColumnarValue::Array(_) => {
            exec_err!("{name} must be a scalar value")
        }
    }
}

fn scalar_to_decimal(scalar: &ScalarValue, name: &str) -> DfResult<Decimal> {
    match scalar {
        ScalarValue::Decimal128(Some(v), _, scale) => {
            Decimal::try_from_i128_with_scale(*v, *scale as u32).map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{name}: Decimal128 value out of rust_decimal range"
                ))
            })
        }
        ScalarValue::Float64(Some(f)) => Decimal::try_from(*f).map_err(|_| {
            datafusion::error::DataFusionError::Execution(format!(
                "{name}: cannot convert {f} to Decimal"
            ))
        }),
        ScalarValue::Int64(Some(i)) => Ok(Decimal::from(*i)),
        ScalarValue::Int32(Some(i)) => Ok(Decimal::from(*i)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            s.parse::<Decimal>().map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{name}: cannot parse '{s}' as Decimal"
                ))
            })
        }
        _ => {
            let s = scalar.to_string();
            s.trim_matches('\'').parse::<Decimal>().map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{name}: cannot convert {s} to Decimal"
                ))
            })
        }
    }
}

pub fn extract_int_arg(arg: &ColumnarValue, name: &str) -> DfResult<i64> {
    match arg {
        ColumnarValue::Scalar(scalar) => {
            let s = scalar.to_string();
            s.trim().parse::<i64>().map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{name} must be an integer, got '{s}'"
                ))
            })
        }
        ColumnarValue::Array(_) => {
            exec_err!("{name} must be a scalar integer")
        }
    }
}

pub fn extract_string_arg(arg: &ColumnarValue, name: &str) -> DfResult<String> {
    match arg {
        ColumnarValue::Scalar(scalar) => Ok(scalar
            .to_string()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string()),
        ColumnarValue::Array(_) => {
            exec_err!("{name} must be a scalar string")
        }
    }
}

/// Extract a list of Decimals from an ARRAY argument.
pub fn extract_decimal_list_arg(arg: &ColumnarValue, name: &str) -> DfResult<Vec<Decimal>> {
    match arg {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::List(list_arr) => {
                let values = list_arr.values();
                extract_decimals_from_array(values.as_ref(), name)
            }
            _ => {
                exec_err!("{name} must be an ARRAY of numbers")
            }
        },
        ColumnarValue::Array(arr) => {
            if let Some(list_arr) = arr.as_any().downcast_ref::<ListArray>() {
                if list_arr.len() != 1 {
                    return exec_err!("{name}: expected single list, got {}", list_arr.len());
                }
                let values = list_arr.value(0);
                extract_decimals_from_array(values.as_ref(), name)
            } else {
                exec_err!("{name} must be a List array")
            }
        }
    }
}

fn extract_decimals_from_array(
    arr: &dyn datafusion::arrow::array::Array,
    name: &str,
) -> DfResult<Vec<Decimal>> {
    if let Some(dec_arr) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let scale = dec_arr.scale();
        let mut result = Vec::with_capacity(dec_arr.len());
        for i in 0..dec_arr.len() {
            if dec_arr.is_null(i) {
                return exec_err!("{name}: NULL weight not allowed");
            }
            let d = Decimal::try_from_i128_with_scale(dec_arr.value(i), scale as u32).map_err(
                |_| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{name}: Decimal128 value out of range"
                    ))
                },
            )?;
            result.push(d);
        }
        Ok(result)
    } else if let Some(f64_arr) = arr.as_any().downcast_ref::<Float64Array>() {
        let mut result = Vec::with_capacity(f64_arr.len());
        for i in 0..f64_arr.len() {
            if f64_arr.is_null(i) {
                return exec_err!("{name}: NULL weight not allowed");
            }
            let d = Decimal::try_from(f64_arr.value(i)).map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{name}: cannot convert {} to Decimal",
                    f64_arr.value(i)
                ))
            })?;
            result.push(d);
        }
        Ok(result)
    } else if let Some(i64_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        let mut result = Vec::with_capacity(i64_arr.len());
        for i in 0..i64_arr.len() {
            if i64_arr.is_null(i) {
                return exec_err!("{name}: NULL weight not allowed");
            }
            result.push(Decimal::from(i64_arr.value(i)));
        }
        Ok(result)
    } else {
        exec_err!("{name}: unsupported array type {:?}", arr.data_type())
    }
}
