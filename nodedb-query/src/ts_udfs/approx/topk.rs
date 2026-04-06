//! `approx_topk(column, k)` — SpaceSaving heavy hitters.
//!
//! Returns a JSON-encoded array of `{"item": ..., "count": ..., "error": ...}`.

use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use nodedb_types::approx::SpaceSaving;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApproxTopkUdaf {
    signature: Signature,
}

impl ApproxTopkUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ApproxTopkUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ApproxTopkUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "approx_topk"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(TopkAccum { ss: None, k: None }))
    }
}

#[derive(Debug)]
struct TopkAccum {
    ss: Option<SpaceSaving>,
    k: Option<usize>,
}

impl TopkAccum {
    fn ensure_init(&mut self, k: usize) {
        if self.ss.is_none() {
            self.k = Some(k);
            self.ss = Some(SpaceSaving::new(k));
        }
    }
}

impl Accumulator for TopkAccum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        if self.k.is_none()
            && let Some(a) = values[1].as_any().downcast_ref::<Int64Array>()
            && !a.is_empty()
        {
            self.ensure_init(a.value(0).max(1) as usize);
        }
        let Some(ss) = &mut self.ss else {
            return Ok(());
        };

        let arr = &values[0];
        if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    ss.add(a.value(i) as u64);
                }
            }
        } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    ss.add(a.value(i));
                }
            }
        } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    ss.add(a.value(i).to_bits());
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let Some(ss) = &self.ss else {
            return Ok(ScalarValue::Utf8(Some("[]".into())));
        };
        let top = ss.top_k();
        let json: Vec<String> = top
            .iter()
            .map(|(item, count, error)| {
                format!(r#"{{"item":{item},"count":{count},"error":{error}}}"#)
            })
            .collect();
        Ok(ScalarValue::Utf8(Some(format!("[{}]", json.join(",")))))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        // Serialization format: [k, item1, count1, error1, item2, count2, error2, ...].
        let k = self.k.unwrap_or(10) as f64;
        let mut flat = vec![k];
        if let Some(ss) = &self.ss {
            for (item, count, error) in ss.top_k() {
                flat.push(item as f64);
                flat.push(count as f64);
                flat.push(error as f64);
            }
        }
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
            .ok_or_else(|| DataFusionError::Internal("approx_topk merge: expected List".into()))?;
        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                continue;
            }
            let inner = list_arr.value(i);
            let f64_arr = inner
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("approx_topk merge: expected Float64 list".into())
                })?;
            if f64_arr.is_empty() {
                continue;
            }
            let k = f64_arr.value(0) as usize;
            self.ensure_init(k);
            let Some(ss) = self.ss.as_mut() else { continue };
            let mut j = 1;
            while j + 2 < f64_arr.len() {
                let item = f64_arr.value(j) as u64;
                let count = f64_arr.value(j + 1) as u64;
                for _ in 0..count {
                    ss.add(item);
                }
                j += 3;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.k.unwrap_or(10) * 24
    }
}
