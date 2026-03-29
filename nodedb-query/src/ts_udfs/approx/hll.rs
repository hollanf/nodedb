//! `approx_count_distinct(column)` — HLL-based cardinality estimation (~0.8% error).

use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};

use nodedb_types::approx::HyperLogLog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApproxCountDistinctUdaf {
    signature: Signature,
}

impl ApproxCountDistinctUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float64]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::UInt64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ApproxCountDistinctUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ApproxCountDistinctUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "approx_count_distinct"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(HllAccum(HyperLogLog::new())))
    }
}

#[derive(Debug)]
struct HllAccum(HyperLogLog);

impl Accumulator for HllAccum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let arr = &values[0];
        if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    self.0.add(a.value(i).to_bits());
                }
            }
        } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    self.0.add(a.value(i) as u64);
                }
            }
        } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    self.0.add(a.value(i));
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.0.estimate())))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let bytes: Vec<u8> = self.0.registers().to_vec();
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        use datafusion::arrow::array::BinaryArray;
        let bin_arr = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("approx_count_distinct merge: expected Binary".into())
            })?;
        for i in 0..bin_arr.len() {
            if !bin_arr.is_null(i) {
                let other = HyperLogLog::from_registers(bin_arr.value(i));
                self.0.merge(&other);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.0.memory_bytes() + std::mem::size_of::<Self>()
    }
}
