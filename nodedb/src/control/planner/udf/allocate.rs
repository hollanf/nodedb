//! `ALLOCATE(total, weights, precision, [mode])` — proportional allocation.
//!
//! Exact allocation that guarantees `SUM(output) == total`.
//! Pure `rust_decimal::Decimal` arithmetic — no floats.
//!
//! Remainder modes:
//! - `LARGEST_REMAINDER` (default) — extra sub-units go to lines with largest
//!   fractional remainder after truncation.
//! - `FIRST` — extra sub-units go to the first line.
//! - `LAST` — extra sub-units go to the last line.
//! - `ROUND_ROBIN` — extra sub-units distributed cyclically starting from first.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rust_decimal::Decimal;

use super::alloc_common::{
    RemainderMode, decimals_to_list_scalar, extract_decimal_arg, extract_decimal_list_arg,
    extract_int_arg, extract_string_arg, parse_remainder_mode,
};

/// Allocate `total` proportionally by `weights`, distributing remainder per `mode`.
/// Invariant: `SUM(result) == total`.
pub(crate) fn allocate_impl(
    total: Decimal,
    weights: &[Decimal],
    precision: u32,
    mode: RemainderMode,
) -> Result<Vec<Decimal>, String> {
    if weights.is_empty() {
        return Ok(Vec::new());
    }

    let weight_sum: Decimal = weights.iter().copied().sum();
    if weight_sum == Decimal::ZERO {
        return Err("ALLOCATE: weights must not all be zero".into());
    }

    let sub_unit = Decimal::new(1, precision);

    let mut result = Vec::with_capacity(weights.len());
    let mut remainders = Vec::with_capacity(weights.len());
    let mut distributed = Decimal::ZERO;

    for w in weights {
        let proportion = total * *w / weight_sum;
        let truncated = proportion.trunc_with_scale(precision);
        let frac = proportion - truncated;
        result.push(truncated);
        remainders.push(frac);
        distributed += truncated;
    }

    let leftover = total - distributed;
    let mut units_to_distribute = {
        let mut r = leftover / sub_unit;
        r.rescale(0);
        r.mantissa().unsigned_abs() as usize
    };

    let sign = if leftover < Decimal::ZERO {
        Decimal::NEGATIVE_ONE
    } else {
        Decimal::ONE
    };

    match mode {
        RemainderMode::LargestRemainder => {
            let mut indices: Vec<usize> = (0..weights.len()).collect();
            indices.sort_by(|&a, &b| {
                remainders[b]
                    .abs()
                    .cmp(&remainders[a].abs())
                    .then(a.cmp(&b))
            });
            for idx in indices {
                if units_to_distribute == 0 {
                    break;
                }
                result[idx] += sub_unit * sign;
                units_to_distribute -= 1;
            }
        }
        RemainderMode::First => {
            for item in result.iter_mut() {
                if units_to_distribute == 0 {
                    break;
                }
                *item += sub_unit * sign;
                units_to_distribute -= 1;
            }
        }
        RemainderMode::Last => {
            for item in result.iter_mut().rev() {
                if units_to_distribute == 0 {
                    break;
                }
                *item += sub_unit * sign;
                units_to_distribute -= 1;
            }
        }
        RemainderMode::RoundRobin => {
            let n = result.len();
            let mut i = 0;
            while units_to_distribute > 0 {
                result[i % n] += sub_unit * sign;
                units_to_distribute -= 1;
                i += 1;
            }
        }
    }

    debug_assert_eq!(
        result.iter().copied().sum::<Decimal>(),
        total,
        "ALLOCATE invariant violated"
    );

    Ok(result)
}

/// `ALLOCATE(total, weights, precision, [remainder_mode])` → List<Decimal>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Allocate {
    signature: Signature,
}

impl Allocate {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(3), TypeSignature::Any(4)],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for Allocate {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for Allocate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "allocate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(38, 18),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arg_values = &args.args;
        if arg_values.len() < 3 || arg_values.len() > 4 {
            return exec_err!(
                "ALLOCATE requires 3-4 arguments: (total, weights, precision, [mode])"
            );
        }

        let total = extract_decimal_arg(&arg_values[0], "total")?;
        let weights = extract_decimal_list_arg(&arg_values[1], "weights")?;
        let precision = extract_int_arg(&arg_values[2], "precision")?;

        if precision < 0 {
            return exec_err!("ALLOCATE: precision must be non-negative, got {precision}");
        }

        let mode = if arg_values.len() == 4 {
            let mode_str = extract_string_arg(&arg_values[3], "mode")?;
            parse_remainder_mode(&mode_str)
                .map_err(datafusion::error::DataFusionError::Execution)?
        } else {
            RemainderMode::LargestRemainder
        };

        let result = allocate_impl(total, &weights, precision as u32, mode)
            .map_err(datafusion::error::DataFusionError::Execution)?;
        let scalar = decimals_to_list_scalar(&result, precision as u32)?;
        Ok(ColumnarValue::Scalar(scalar))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn allocate_equal_weights() {
        let result = allocate_impl(
            d("100.00"),
            &[d("1"), d("1"), d("1")],
            2,
            RemainderMode::LargestRemainder,
        )
        .unwrap();
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("100.00"));
    }

    #[test]
    fn allocate_proportional() {
        let result = allocate_impl(
            d("100.00"),
            &[d("50"), d("30"), d("20")],
            2,
            RemainderMode::LargestRemainder,
        )
        .unwrap();
        assert_eq!(result, vec![d("50.00"), d("30.00"), d("20.00")]);
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("100.00"));
    }

    #[test]
    fn allocate_with_remainder_largest() {
        let result = allocate_impl(
            d("2500.00"),
            &[d("800"), d("1000"), d("1200")],
            2,
            RemainderMode::LargestRemainder,
        )
        .unwrap();
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("2500.00"));
        assert_eq!(result[0], d("666.67"));
        assert_eq!(result[1], d("833.33"));
        assert_eq!(result[2], d("1000.00"));
    }

    #[test]
    fn allocate_with_remainder_first() {
        let result = allocate_impl(
            d("100.00"),
            &[d("1"), d("1"), d("1")],
            2,
            RemainderMode::First,
        )
        .unwrap();
        assert_eq!(result, vec![d("33.34"), d("33.33"), d("33.33")]);
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("100.00"));
    }

    #[test]
    fn allocate_with_remainder_last() {
        let result = allocate_impl(
            d("100.00"),
            &[d("1"), d("1"), d("1")],
            2,
            RemainderMode::Last,
        )
        .unwrap();
        assert_eq!(result, vec![d("33.33"), d("33.33"), d("33.34")]);
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("100.00"));
    }

    #[test]
    fn allocate_zero_weights_error() {
        let result = allocate_impl(
            d("100.00"),
            &[d("0"), d("0")],
            2,
            RemainderMode::LargestRemainder,
        );
        assert!(result.is_err());
    }

    #[test]
    fn allocate_single_weight() {
        let result =
            allocate_impl(d("99.99"), &[d("1")], 2, RemainderMode::LargestRemainder).unwrap();
        assert_eq!(result, vec![d("99.99")]);
    }

    #[test]
    fn allocate_high_precision() {
        let result = allocate_impl(
            d("1.000"),
            &[d("1"), d("1"), d("1")],
            4,
            RemainderMode::First,
        )
        .unwrap();
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("1.000"));
        assert_eq!(result[0], d("0.3334"));
        assert_eq!(result[1], d("0.3333"));
        assert_eq!(result[2], d("0.3333"));
    }
}
