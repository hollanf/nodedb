//! `approx_percentile(column, p)` — TDigest-based quantile estimation.

use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use nodedb_types::approx::TDigest;

fn as_f64(arr: &ArrayRef) -> DfResult<&Float64Array> {
    arr.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Internal("expected Float64 array".into()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApproxPercentileUdaf {
    signature: Signature,
}

impl ApproxPercentileUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ApproxPercentileUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ApproxPercentileUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "approx_percentile"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(TDigestAccum {
            digest: TDigest::new(),
            percentile: None,
        }))
    }
}

#[derive(Debug)]
struct TDigestAccum {
    digest: TDigest,
    percentile: Option<f64>,
}

impl Accumulator for TDigestAccum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let arr = as_f64(&values[0])?;
        if self.percentile.is_none() {
            let p_arr = as_f64(&values[1])?;
            if !p_arr.is_empty() {
                self.percentile = Some(p_arr.value(0));
            }
        }
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.digest.add(arr.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let p = self.percentile.unwrap_or(0.5);
        let result = self.digest.quantile(p);
        Ok(ScalarValue::Float64(if result.is_nan() {
            None
        } else {
            Some(result)
        }))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let centroids = self.digest.centroids();
        let mut flat: Vec<f64> = Vec::with_capacity(centroids.len() * 2 + 1);
        for (mean, count) in centroids {
            flat.push(mean);
            flat.push(count as f64);
        }
        flat.push(self.percentile.unwrap_or(0.5));
        let scalars: Vec<ScalarValue> = flat
            .into_iter()
            .map(|v| ScalarValue::Float64(Some(v)))
            .collect();
        Ok(vec![ScalarValue::List(ScalarValue::new_list(
            &scalars,
            &DataType::Float64,
            true,
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        use datafusion::arrow::array::ListArray;
        let list_arr = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("approx_percentile merge: expected List".into())
            })?;
        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                continue;
            }
            let inner = list_arr.value(i);
            let f64_arr = inner
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "approx_percentile merge: expected Float64 list".into(),
                    )
                })?;
            if f64_arr.len() < 3 {
                continue;
            }
            let p = f64_arr.value(f64_arr.len() - 1);
            if self.percentile.is_none() {
                self.percentile = Some(p);
            }
            let mut other = TDigest::new();
            let data_len = f64_arr.len() - 1;
            let mut j = 0;
            while j + 1 < data_len {
                let mean = f64_arr.value(j);
                let count = f64_arr.value(j + 1) as u64;
                other.add_centroid(mean, count);
                j += 2;
            }
            self.digest.merge(&other);
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.digest.memory_bytes()
    }
}
