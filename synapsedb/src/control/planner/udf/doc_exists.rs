use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{BinaryArray, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::nav::{expand_to_array, navigate_json, navigate_rmpv};

/// `doc_exists(document, path)` — Check if a path exists in a MessagePack document.
///
/// Returns true if the path resolves to a non-nil value in the MessagePack blob.
/// Returns false if the path does not exist, the value is Nil, or the blob is invalid.
///
/// Also accepts JSON UTF-8 strings for backward compatibility.
#[derive(Debug)]
pub struct DocExists {
    signature: Signature,
}

impl DocExists {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for DocExists {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for DocExists {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "doc_exists"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DfResult<ColumnarValue> {
        let docs = expand_to_array(&args[0], num_rows)?;
        let paths = expand_to_array(&args[1], num_rows)?;

        let paths = paths
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "doc_exists: expected Utf8 for path".into(),
                )
            })?;

        if let Some(bin_docs) = docs.as_any().downcast_ref::<BinaryArray>() {
            let result: BooleanArray = bin_docs
                .iter()
                .zip(paths.iter())
                .map(|(doc, path)| Some(exists_msgpack(doc?, path?)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else if let Some(str_docs) = docs.as_any().downcast_ref::<StringArray>() {
            let result: BooleanArray = str_docs
                .iter()
                .zip(paths.iter())
                .map(|(doc, path)| Some(exists_json(doc?, path?)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "doc_exists: expected Binary or Utf8 for document".into(),
            ))
        }
    }
}

fn exists_msgpack(data: &[u8], path: &str) -> bool {
    let value: rmpv::Value = match rmpv::decode::read_value(&mut &data[..]) {
        Ok(v) => v,
        Err(_) => return false,
    };
    match navigate_rmpv(&value, path) {
        Some(v) => !v.is_nil(),
        None => false,
    }
}

fn exists_json(json_str: &str, path: &str) -> bool {
    let parsed: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return false,
    };
    match navigate_json(&parsed, path) {
        Some(v) => !v.is_null(),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::super::nav::test_util::to_msgpack;
    use super::*;

    #[test]
    fn msgpack_exists() {
        let json = serde_json::json!({"name": "alice", "age": 30});
        let data = to_msgpack(&json);
        assert!(exists_msgpack(&data, "$.name"));
        assert!(exists_msgpack(&data, "$.age"));
        assert!(!exists_msgpack(&data, "$.missing"));
    }

    #[test]
    fn msgpack_exists_nested() {
        let json = serde_json::json!({"user": {"email": "a@b.com"}});
        let data = to_msgpack(&json);
        assert!(exists_msgpack(&data, "$.user.email"));
        assert!(!exists_msgpack(&data, "$.user.phone"));
    }

    #[test]
    fn msgpack_nil_is_not_exists() {
        let json = serde_json::json!({"x": null});
        let data = to_msgpack(&json);
        assert!(!exists_msgpack(&data, "$.x"));
    }

    #[test]
    fn msgpack_invalid_data() {
        assert!(!exists_msgpack(&[0xff, 0xfe], "$.foo"));
    }

    #[test]
    fn json_compat() {
        assert!(exists_json(r#"{"name": "alice"}"#, "$.name"));
        assert!(!exists_json(r#"{"name": "alice"}"#, "$.missing"));
        assert!(!exists_json(r#"{"x": null}"#, "$.x"));
    }

    #[test]
    fn udf_batch_binary() {
        let udf = DocExists::new();
        let doc1 = to_msgpack(&serde_json::json!({"a": 1}));
        let doc2 = to_msgpack(&serde_json::json!({"b": 2}));

        let docs = ColumnarValue::Array(Arc::new(BinaryArray::from(vec![
            doc1.as_slice(),
            doc2.as_slice(),
        ])));
        let paths =
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some("$.a".into())));

        let result = udf.invoke_batch(&[docs, paths], 2).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert!(arr.value(0));
                assert!(!arr.value(1));
            }
            _ => panic!("expected array"),
        }
    }
}
