//! `DISTRIBUTE(total, n, precision)` — divide total into N equal parts.
//!
//! Exact allocation that guarantees `SUM(output) == total`.
//! Pure `rust_decimal::Decimal` arithmetic — no floats.
//! Remainder sub-units go to the first elements.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rust_decimal::Decimal;

use super::alloc_common::{
    RemainderMode, decimals_to_list_scalar, extract_decimal_arg, extract_int_arg,
};

/// Divide `total` into `n` equal parts, distributing remainder per `mode`.
/// Invariant: `SUM(result) == total`.
pub(crate) fn distribute_impl(
    total: Decimal,
    n: usize,
    precision: u32,
    mode: RemainderMode,
) -> Vec<Decimal> {
    if n == 0 {
        return Vec::new();
    }

    let n_dec = Decimal::from(n as u64);
    let sub_unit = Decimal::new(1, precision);

    let raw_share = total / n_dec;
    let truncated = raw_share.trunc_with_scale(precision);

    let distributed = truncated * n_dec;
    let remainder = total - distributed;

    let mut result = vec![truncated; n];

    let units_to_distribute = {
        let mut r = remainder / sub_unit;
        r.rescale(0);
        r.mantissa().unsigned_abs() as usize
    };

    let sign = if remainder < Decimal::ZERO {
        Decimal::NEGATIVE_ONE
    } else {
        Decimal::ONE
    };

    match mode {
        RemainderMode::LargestRemainder | RemainderMode::First => {
            for item in result.iter_mut().take(units_to_distribute) {
                *item += sub_unit * sign;
            }
        }
        RemainderMode::Last => {
            let len = result.len();
            for i in 0..units_to_distribute {
                result[len - 1 - i] += sub_unit * sign;
            }
        }
        RemainderMode::RoundRobin => {
            for i in 0..units_to_distribute {
                result[i % n] += sub_unit * sign;
            }
        }
    }

    debug_assert_eq!(
        result.iter().copied().sum::<Decimal>(),
        total,
        "DISTRIBUTE invariant violated"
    );

    result
}

/// `DISTRIBUTE(total, n, precision)` → List<Decimal>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Distribute {
    signature: Signature,
}

impl Distribute {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(3)], Volatility::Immutable),
        }
    }
}

impl Default for Distribute {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for Distribute {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "distribute"
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
        if arg_values.len() != 3 {
            return exec_err!("DISTRIBUTE requires 3 arguments: (total, n, precision)");
        }

        let total = extract_decimal_arg(&arg_values[0], "total")?;
        let n = extract_int_arg(&arg_values[1], "n")?;
        let precision = extract_int_arg(&arg_values[2], "precision")?;

        if n < 0 {
            return exec_err!("DISTRIBUTE: n must be non-negative, got {n}");
        }
        if precision < 0 {
            return exec_err!("DISTRIBUTE: precision must be non-negative, got {precision}");
        }

        let result = distribute_impl(total, n as usize, precision as u32, RemainderMode::First);
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
    fn distribute_even_split() {
        let result = distribute_impl(d("100.00"), 4, 2, RemainderMode::First);
        assert_eq!(result, vec![d("25.00"), d("25.00"), d("25.00"), d("25.00")]);
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("100.00"));
    }

    #[test]
    fn distribute_with_remainder() {
        let result = distribute_impl(d("100.00"), 3, 2, RemainderMode::First);
        assert_eq!(result, vec![d("33.34"), d("33.33"), d("33.33")]);
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("100.00"));
    }

    #[test]
    fn distribute_last_mode() {
        let result = distribute_impl(d("100.00"), 3, 2, RemainderMode::Last);
        assert_eq!(result, vec![d("33.33"), d("33.33"), d("33.34")]);
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("100.00"));
    }

    #[test]
    fn distribute_round_robin() {
        let result = distribute_impl(d("10.00"), 3, 2, RemainderMode::RoundRobin);
        assert_eq!(result, vec![d("3.34"), d("3.33"), d("3.33")]);
        assert_eq!(result.iter().copied().sum::<Decimal>(), d("10.00"));
    }

    #[test]
    fn distribute_single() {
        let result = distribute_impl(d("99.99"), 1, 2, RemainderMode::First);
        assert_eq!(result, vec![d("99.99")]);
    }

    #[test]
    fn distribute_zero_total() {
        let result = distribute_impl(d("0.00"), 5, 2, RemainderMode::First);
        assert_eq!(result, vec![d("0.00"); 5]);
    }

    #[test]
    fn distribute_zero_count() {
        let result = distribute_impl(d("100.00"), 0, 2, RemainderMode::First);
        assert!(result.is_empty());
    }
}
