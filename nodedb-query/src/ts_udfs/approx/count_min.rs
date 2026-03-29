//! `approx_count(column, target)` — CountMinSketch frequency estimation.
//!
//! Returns the estimated count of `target` in `column`.

use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use nodedb_types::approx::CountMinSketch;

fn extract_u64_scalar(arr: &ArrayRef) -> Option<u64> {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>()
        && !a.is_empty()
        && !a.is_null(0)
    {
        return Some(a.value(0) as u64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>()
        && !a.is_empty()
        && !a.is_null(0)
    {
        return Some(a.value(0));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>()
        && !a.is_empty()
        && !a.is_null(0)
    {
        return Some(a.value(0).to_bits());
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApproxCountUdaf {
    signature: Signature,
}

impl ApproxCountUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt64, DataType::UInt64]),
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ApproxCountUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ApproxCountUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "approx_count"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::UInt64)
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(CmsAccum {
            cms: CountMinSketch::new(),
            target: None,
        }))
    }
}

#[derive(Debug)]
struct CmsAccum {
    cms: CountMinSketch,
    target: Option<u64>,
}

impl Accumulator for CmsAccum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        if self.target.is_none() {
            self.target = extract_u64_scalar(&values[1]);
        }
        let arr = &values[0];
        if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    self.cms.add(a.value(i) as u64);
                }
            }
        } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    self.cms.add(a.value(i));
                }
            }
        } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    self.cms.add(a.value(i).to_bits());
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let target = self.target.unwrap_or(0);
        Ok(ScalarValue::UInt64(Some(self.cms.estimate(target))))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let table = self.cms.table_bytes();
        let target = self.target.unwrap_or(0).to_le_bytes();
        let mut bytes = table;
        bytes.extend_from_slice(&target);
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        use datafusion::arrow::array::BinaryArray;
        let bin_arr = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("approx_count merge: expected Binary".into())
            })?;
        for i in 0..bin_arr.len() {
            if !bin_arr.is_null(i) {
                let bytes = bin_arr.value(i);
                if bytes.len() >= 8 {
                    let table_bytes = &bytes[..bytes.len() - 8];
                    let target_bytes = &bytes[bytes.len() - 8..];
                    if self.target.is_none() {
                        self.target = Some(u64::from_le_bytes(
                            target_bytes.try_into().unwrap_or([0; 8]),
                        ));
                    }
                    let other = CountMinSketch::from_table_bytes(table_bytes, 1024, 4);
                    self.cms.merge(&other);
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.cms.memory_bytes() + std::mem::size_of::<Self>()
    }
}
