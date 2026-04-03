//! `VECTOR_METADATA(collection, column)` — stub UDF for DataFusion type checking.
//!
//! The DDL router intercepts `SELECT VECTOR_METADATA(...)` before it reaches
//! DataFusion execution. This UDF exists solely for plan validation.
//! Actual execution happens in `collection::vector_metadata::handle_vector_metadata_query`.

use std::any::Any;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorMetadata {
    signature: Signature,
}

impl VectorMetadata {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // VECTOR_METADATA('collection', 'column')
                    TypeSignature::Any(2),
                ],
                Volatility::Stable,
            ),
        }
    }
}

impl Default for VectorMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for VectorMetadata {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vector_metadata"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DfResult<ColumnarValue> {
        // Stub: returns empty JSON. Real execution is intercepted by the DDL router.
        let array = StringArray::from(vec!["null"; args.number_rows]);
        Ok(ColumnarValue::Array(std::sync::Arc::new(array)))
    }
}
