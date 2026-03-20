//! `bm25_score(field, query)` — BM25 text search scoring UDF.
//!
//! Returns a relevance score for full-text matching. Used in ORDER BY
//! and WHERE for text search queries:
//!
//! ```sql
//! SELECT * FROM docs
//! ORDER BY bm25_score(body, 'distributed database') DESC
//! LIMIT 10
//! ```
//!
//! This is a **marker UDF**: DataFusion evaluates it as a scalar function
//! that always returns 0.0 at the Control Plane. The real scoring happens
//! when the plan converter detects `ORDER BY bm25_score(...)` and rewrites
//! it to `PhysicalPlan::TextSearch`, which executes on the Data Plane.
//!
//! This approach is the same pattern used by `vector_distance()`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct Bm25Score {
    signature: Signature,
}

impl Default for Bm25Score {
    fn default() -> Self {
        Self::new()
    }
}

impl Bm25Score {
    pub fn new() -> Self {
        Self {
            // bm25_score(field: Utf8, query: Utf8) → Float64
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for Bm25Score {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bm25_score"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        // Marker function: returns 0.0 for all rows.
        // Real scoring happens via TextSearch on the Data Plane.
        let len = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let zeros = Float64Array::from(vec![0.0; len]);
        Ok(ColumnarValue::Array(Arc::new(zeros) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn returns_float64() {
        let udf = Bm25Score::new();
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn invoke_returns_zeros() {
        use datafusion::logical_expr::ScalarFunctionArgs;

        let udf = Bm25Score::new();
        let field =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["body", "body"])) as ArrayRef);
        let query =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["test", "test"])) as ArrayRef);
        let args = ScalarFunctionArgs {
            args: vec![field, query],
            number_rows: 2,
            return_type: &DataType::Float64,
        };
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(f64_arr.len(), 2);
                assert_eq!(f64_arr.value(0), 0.0);
            }
            _ => panic!("expected array"),
        }
    }
}
