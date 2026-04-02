//! `ROUND(value, decimals, [mode])` — configurable decimal rounding.
//!
//! Six rounding modes via `rust_decimal::RoundingStrategy`:
//! - `HALF_UP`   — round half away from zero (traditional)
//! - `HALF_EVEN` — round half to nearest even (Banker's, default)
//! - `HALF_DOWN` — round half toward zero
//! - `TRUNCATE`  — always toward zero
//! - `CEILING`   — always toward +∞
//! - `FLOOR`     — always toward −∞
//!
//! When `mode` is omitted, uses the session's `rounding_mode` parameter
//! (defaults to `HALF_EVEN`).
//!
//! Input can be Float64 or Decimal128. Both are converted to
//! `rust_decimal::Decimal` for exact rounding, then returned as
//! Decimal128(38, output_scale).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

/// Map a mode string to `rust_decimal::RoundingStrategy`.
fn parse_rounding_mode(mode: &str) -> Result<RoundingStrategy, String> {
    match mode.to_uppercase().as_str() {
        "HALF_UP" => Ok(RoundingStrategy::MidpointAwayFromZero),
        "HALF_EVEN" | "BANKERS" => Ok(RoundingStrategy::MidpointNearestEven),
        "HALF_DOWN" => Ok(RoundingStrategy::MidpointTowardZero),
        "TRUNCATE" | "TRUNC" => Ok(RoundingStrategy::ToZero),
        "CEILING" | "CEIL" => Ok(RoundingStrategy::AwayFromZero),
        "FLOOR" => Ok(RoundingStrategy::ToNegativeInfinity),
        other => Err(format!(
            "unknown rounding mode '{other}'. \
             Valid: HALF_UP, HALF_EVEN, HALF_DOWN, TRUNCATE, CEILING, FLOOR"
        )),
    }
}

/// Perform the rounding: convert to `rust_decimal::Decimal`, round, return as i128 mantissa.
fn round_decimal(value: Decimal, decimals: u32, strategy: RoundingStrategy) -> Decimal {
    value.round_dp_with_strategy(decimals, strategy)
}

/// Convert an f64 to `rust_decimal::Decimal`, round, return the result.
fn round_f64(value: f64, decimals: u32, strategy: RoundingStrategy) -> Option<Decimal> {
    Decimal::try_from(value)
        .ok()
        .map(|d| round_decimal(d, decimals, strategy))
}

/// `ROUND(value, decimals, [mode])` — configurable decimal rounding UDF.
///
/// Registered per-session so the default rounding mode can be read from
/// the session's `rounding_mode` parameter.
pub struct RoundDecimal {
    signature: Signature,
    /// Default rounding mode from session parameter (e.g. "HALF_EVEN").
    default_mode: RoundingStrategy,
}

impl RoundDecimal {
    pub fn new(default_mode_str: &str) -> Self {
        let default_mode =
            parse_rounding_mode(default_mode_str).unwrap_or(RoundingStrategy::MidpointNearestEven);
        Self {
            signature: Signature::one_of(
                vec![
                    // ROUND(float64, int)
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Int32]),
                    // ROUND(float64, int, mode)
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Int32, DataType::Utf8]),
                    // ROUND(decimal128, int)
                    TypeSignature::Exact(vec![DataType::Decimal128(38, 18), DataType::Int32]),
                    // ROUND(decimal128, int, mode)
                    TypeSignature::Exact(vec![
                        DataType::Decimal128(38, 18),
                        DataType::Int32,
                        DataType::Utf8,
                    ]),
                    // Accept any numeric via coercion.
                    TypeSignature::Any(2),
                    TypeSignature::Any(3),
                ],
                Volatility::Immutable,
            ),
            default_mode,
        }
    }
}

impl std::fmt::Debug for RoundDecimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoundDecimal")
            .field("default_mode", &self.default_mode)
            .finish()
    }
}

impl PartialEq for RoundDecimal {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(&self.default_mode) == std::mem::discriminant(&other.default_mode)
    }
}

impl Eq for RoundDecimal {}

impl std::hash::Hash for RoundDecimal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.default_mode).hash(state);
    }
}

impl ScalarUDFImpl for RoundDecimal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        // Always return Decimal128(38, scale) where scale = decimals arg.
        // Since we don't know scale at planning time, use max precision.
        Ok(DataType::Decimal128(38, 18))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arg_values = &args.args;

        if arg_values.len() < 2 || arg_values.len() > 3 {
            return exec_err!("ROUND requires 2 or 3 arguments: (value, decimals, [mode])");
        }

        // Extract decimals (second argument — always scalar in practice).
        let decimals = extract_decimals_arg(&arg_values[1])?;

        // Extract rounding mode (third argument or session default).
        let strategy = if arg_values.len() == 3 {
            extract_mode_arg(&arg_values[2])?
        } else {
            self.default_mode
        };

        // Process the value argument.
        match &arg_values[0] {
            ColumnarValue::Scalar(scalar) => {
                let result = round_scalar(scalar, decimals, strategy)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let result = round_array(arr, decimals, strategy)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }
}

/// Extract the `decimals` integer from the second argument.
fn extract_decimals_arg(arg: &ColumnarValue) -> DfResult<u32> {
    match arg {
        ColumnarValue::Scalar(scalar) => {
            let s = scalar.to_string();
            let n: i32 = s.trim().parse().map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ROUND: decimals must be an integer, got '{s}'"
                ))
            })?;
            if n < 0 {
                return exec_err!("ROUND: decimals must be non-negative, got {n}");
            }
            Ok(n as u32)
        }
        ColumnarValue::Array(_) => {
            exec_err!("ROUND: decimals argument must be a scalar integer")
        }
    }
}

/// Extract the rounding mode from the third argument.
fn extract_mode_arg(arg: &ColumnarValue) -> DfResult<RoundingStrategy> {
    match arg {
        ColumnarValue::Scalar(scalar) => {
            let mode_str = scalar
                .to_string()
                .trim_matches('\'')
                .trim_matches('"')
                .to_uppercase();
            parse_rounding_mode(&mode_str).map_err(datafusion::error::DataFusionError::Execution)
        }
        ColumnarValue::Array(_) => {
            exec_err!("ROUND: mode argument must be a scalar string")
        }
    }
}

/// Round a scalar value.
fn round_scalar(
    scalar: &datafusion::common::ScalarValue,
    decimals: u32,
    strategy: RoundingStrategy,
) -> DfResult<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue;

    let dec = scalar_to_decimal(scalar)?;
    match dec {
        None => Ok(ScalarValue::Decimal128(None, 38, decimals as i8)),
        Some(d) => {
            let rounded = round_decimal(d, decimals, strategy);
            let mantissa = decimal_to_i128(rounded, decimals)?;
            Ok(ScalarValue::Decimal128(Some(mantissa), 38, decimals as i8))
        }
    }
}

/// Round an array of values.
fn round_array(arr: &dyn Array, decimals: u32, strategy: RoundingStrategy) -> DfResult<ArrayRef> {
    // Try Decimal128 first, then Float64.
    if let Some(dec_arr) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let scale = dec_arr.scale();
        let mut builder = Vec::with_capacity(dec_arr.len());
        for i in 0..dec_arr.len() {
            if dec_arr.is_null(i) {
                builder.push(None);
            } else {
                let mantissa = dec_arr.value(i);
                match Decimal::try_from_i128_with_scale(mantissa, scale as u32) {
                    Ok(val) => {
                        let rounded = round_decimal(val, decimals, strategy);
                        let out = decimal_to_i128(rounded, decimals)?;
                        builder.push(Some(out));
                    }
                    Err(_) => builder.push(None),
                }
            }
        }
        let result = Decimal128Array::from(builder)
            .with_precision_and_scale(38, decimals as i8)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ROUND: failed to set precision/scale: {e}"
                ))
            })?;
        Ok(Arc::new(result))
    } else if let Some(f64_arr) = arr.as_any().downcast_ref::<Float64Array>() {
        let mut builder = Vec::with_capacity(f64_arr.len());
        for i in 0..f64_arr.len() {
            if f64_arr.is_null(i) {
                builder.push(None);
            } else {
                match round_f64(f64_arr.value(i), decimals, strategy) {
                    Some(rounded) => {
                        let out = decimal_to_i128(rounded, decimals)?;
                        builder.push(Some(out));
                    }
                    None => builder.push(None),
                }
            }
        }
        let result = Decimal128Array::from(builder)
            .with_precision_and_scale(38, decimals as i8)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ROUND: failed to set precision/scale: {e}"
                ))
            })?;
        Ok(Arc::new(result))
    } else {
        // Try to cast via StringArray → Decimal.
        if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
            let mut builder = Vec::with_capacity(str_arr.len());
            for i in 0..str_arr.len() {
                if str_arr.is_null(i) {
                    builder.push(None);
                } else {
                    match str_arr.value(i).parse::<Decimal>() {
                        Ok(d) => {
                            let rounded = round_decimal(d, decimals, strategy);
                            let out = decimal_to_i128(rounded, decimals)?;
                            builder.push(Some(out));
                        }
                        Err(_) => builder.push(None),
                    }
                }
            }
            let result = Decimal128Array::from(builder)
                .with_precision_and_scale(38, decimals as i8)
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ROUND: failed to set precision/scale: {e}"
                    ))
                })?;
            Ok(Arc::new(result))
        } else {
            exec_err!("ROUND: unsupported input type {:?}", arr.data_type())
        }
    }
}

/// Convert a ScalarValue to `rust_decimal::Decimal`.
fn scalar_to_decimal(scalar: &datafusion::common::ScalarValue) -> DfResult<Option<Decimal>> {
    use datafusion::common::ScalarValue;

    match scalar {
        ScalarValue::Decimal128(None, _, _) => Ok(None),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            Ok(Decimal::try_from_i128_with_scale(*v, *scale as u32).ok())
        }
        ScalarValue::Float64(None) => Ok(None),
        ScalarValue::Float64(Some(f)) => Ok(Decimal::try_from(*f).ok()),
        ScalarValue::Int64(None) => Ok(None),
        ScalarValue::Int64(Some(i)) => Ok(Some(Decimal::from(*i))),
        ScalarValue::Int32(None) => Ok(None),
        ScalarValue::Int32(Some(i)) => Ok(Some(Decimal::from(*i))),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => Ok(None),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Ok(s.parse::<Decimal>().ok())
        }
        other => {
            // Try the string representation as a last resort.
            let s = other.to_string();
            Ok(s.parse::<Decimal>().ok())
        }
    }
}

/// Convert a `rust_decimal::Decimal` to an i128 mantissa for Arrow Decimal128.
fn decimal_to_i128(d: Decimal, target_scale: u32) -> DfResult<i128> {
    // Rescale to target scale, then extract the mantissa.
    let mut scaled = d;
    scaled.rescale(target_scale);
    Ok(scaled.mantissa())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn parse_modes() {
        assert!(parse_rounding_mode("HALF_UP").is_ok());
        assert!(parse_rounding_mode("half_even").is_ok());
        assert!(parse_rounding_mode("BANKERS").is_ok());
        assert!(parse_rounding_mode("TRUNCATE").is_ok());
        assert!(parse_rounding_mode("TRUNC").is_ok());
        assert!(parse_rounding_mode("CEILING").is_ok());
        assert!(parse_rounding_mode("CEIL").is_ok());
        assert!(parse_rounding_mode("FLOOR").is_ok());
        assert!(parse_rounding_mode("HALF_DOWN").is_ok());
        assert!(parse_rounding_mode("INVALID").is_err());
    }

    #[test]
    fn round_half_even() {
        let strategy = RoundingStrategy::MidpointNearestEven;
        // 2.5 → 2 (round to even)
        assert_eq!(round_decimal(d("2.5"), 0, strategy), d("2"));
        // 3.5 → 4 (round to even)
        assert_eq!(round_decimal(d("3.5"), 0, strategy), d("4"));
        // 2.25 → 2.2
        assert_eq!(round_decimal(d("2.25"), 1, strategy), d("2.2"));
        // 2.35 → 2.4
        assert_eq!(round_decimal(d("2.35"), 1, strategy), d("2.4"));
    }

    #[test]
    fn round_half_up() {
        let strategy = RoundingStrategy::MidpointAwayFromZero;
        assert_eq!(round_decimal(d("2.5"), 0, strategy), d("3"));
        assert_eq!(round_decimal(d("3.5"), 0, strategy), d("4"));
        assert_eq!(round_decimal(d("-2.5"), 0, strategy), d("-3"));
    }

    #[test]
    fn round_truncate() {
        let strategy = RoundingStrategy::ToZero;
        assert_eq!(round_decimal(d("2.9"), 0, strategy), d("2"));
        assert_eq!(round_decimal(d("-2.9"), 0, strategy), d("-2"));
        assert_eq!(round_decimal(d("3.456"), 2, strategy), d("3.45"));
    }

    #[test]
    fn round_ceiling() {
        let strategy = RoundingStrategy::AwayFromZero;
        assert_eq!(round_decimal(d("2.1"), 0, strategy), d("3"));
        assert_eq!(round_decimal(d("-2.1"), 0, strategy), d("-3"));
    }

    #[test]
    fn round_floor() {
        let strategy = RoundingStrategy::ToNegativeInfinity;
        assert_eq!(round_decimal(d("2.9"), 0, strategy), d("2"));
        assert_eq!(round_decimal(d("-2.1"), 0, strategy), d("-3"));
    }

    #[test]
    fn round_half_down() {
        let strategy = RoundingStrategy::MidpointTowardZero;
        assert_eq!(round_decimal(d("2.5"), 0, strategy), d("2"));
        assert_eq!(round_decimal(d("3.5"), 0, strategy), d("3"));
        assert_eq!(round_decimal(d("2.6"), 0, strategy), d("3"));
    }

    #[test]
    fn round_f64_to_decimal() {
        let strategy = RoundingStrategy::MidpointNearestEven;
        let result = round_f64(3.14259, 2, strategy).unwrap();
        assert_eq!(result, d("3.14"));
    }

    #[test]
    fn decimal_to_i128_roundtrip() {
        let val = d("123.45");
        let mantissa = decimal_to_i128(val, 2).unwrap();
        assert_eq!(mantissa, 12345);

        let val2 = d("100.00");
        let mantissa2 = decimal_to_i128(val2, 4).unwrap();
        assert_eq!(mantissa2, 1000000);
    }
}
