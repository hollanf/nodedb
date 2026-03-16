use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::nav::{expand_to_array, navigate_json, navigate_rmpv, rmpv_to_string};

/// `doc_get(document, path)` — Extract a value from a MessagePack document at a dot-separated path.
///
/// The first argument is a MessagePack-encoded binary blob. The second is a path
/// string like `$.user.email` or `user.email`. Returns the extracted value as a
/// UTF-8 string (numbers stringified, strings returned as-is). Returns NULL if the
/// path does not exist or the blob is not valid MessagePack.
///
/// Also accepts JSON-encoded UTF-8 strings for backward compatibility.
#[derive(Debug)]
pub struct DocGet {
    signature: Signature,
}

impl DocGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // Primary: MessagePack binary blob + path
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                    // Compat: JSON string + path
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for DocGet {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for DocGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "doc_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DfResult<ColumnarValue> {
        let docs = expand_to_array(&args[0], num_rows)?;
        let paths = expand_to_array(&args[1], num_rows)?;

        let paths = paths
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "doc_get: expected Utf8 for path".into(),
                )
            })?;

        // Dispatch based on input type: Binary (MessagePack) or Utf8 (JSON).
        if let Some(bin_docs) = docs.as_any().downcast_ref::<BinaryArray>() {
            let result: StringArray = bin_docs
                .iter()
                .zip(paths.iter())
                .map(|(doc, path)| extract_msgpack(doc?, path?))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else if let Some(str_docs) = docs.as_any().downcast_ref::<StringArray>() {
            let result: StringArray = str_docs
                .iter()
                .zip(paths.iter())
                .map(|(doc, path)| extract_json(doc?, path?))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "doc_get: expected Binary or Utf8 for document".into(),
            ))
        }
    }
}

/// Extract a value from a MessagePack blob at the given path.
fn extract_msgpack(data: &[u8], path: &str) -> Option<String> {
    let value: rmpv::Value = rmpv::decode::read_value(&mut &data[..]).ok()?;
    let result = navigate_rmpv(&value, path)?;
    Some(rmpv_to_string(result))
}

/// Extract a value from a JSON string at the given path.
fn extract_json(json_str: &str, path: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let result = navigate_json(&parsed, path)?;
    Some(json_value_to_string(result))
}

fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::nav::test_util::to_msgpack;
    use super::*;

    #[test]
    fn msgpack_extract_simple_field() {
        let json: serde_json::Value = serde_json::json!({"name": "alice", "age": 30});
        let data = to_msgpack(&json);
        assert_eq!(extract_msgpack(&data, "$.name"), Some("alice".into()));
        assert_eq!(extract_msgpack(&data, "$.age"), Some("30".into()));
    }

    #[test]
    fn msgpack_extract_nested() {
        let json = serde_json::json!({"user": {"email": "a@b.com"}});
        let data = to_msgpack(&json);
        assert_eq!(
            extract_msgpack(&data, "$.user.email"),
            Some("a@b.com".into())
        );
    }

    #[test]
    fn msgpack_extract_array_index() {
        let json = serde_json::json!({"items": [10, 20, 30]});
        let data = to_msgpack(&json);
        assert_eq!(extract_msgpack(&data, "$.items[1]"), Some("20".into()));
    }

    #[test]
    fn msgpack_missing_path() {
        let json = serde_json::json!({"name": "alice"});
        let data = to_msgpack(&json);
        assert_eq!(extract_msgpack(&data, "$.nonexistent"), None);
    }

    #[test]
    fn msgpack_invalid_data() {
        assert_eq!(extract_msgpack(&[0xff, 0xfe], "$.foo"), None);
    }

    #[test]
    fn json_compat_extract() {
        let json_str = r#"{"name": "alice"}"#;
        assert_eq!(extract_json(json_str, "$.name"), Some("alice".into()));
    }

    #[test]
    fn udf_batch_binary() {
        let udf = DocGet::new();
        let doc1 = to_msgpack(&serde_json::json!({"a": 1}));
        let doc2 = to_msgpack(&serde_json::json!({"a": 2}));
        let doc3 = to_msgpack(&serde_json::json!({"b": 3}));

        let docs = ColumnarValue::Array(Arc::new(BinaryArray::from(vec![
            doc1.as_slice(),
            doc2.as_slice(),
            doc3.as_slice(),
        ])));
        let paths =
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some("$.a".into())));

        let result = udf.invoke_batch(&[docs, paths], 3).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(arr.value(0), "1");
                assert_eq!(arr.value(1), "2");
                assert!(arr.is_null(2));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn udf_batch_json_compat() {
        let udf = DocGet::new();
        let docs = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            r#"{"a": 1}"#,
            r#"{"a": 2}"#,
        ])));
        let paths =
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some("$.a".into())));

        let result = udf.invoke_batch(&[docs, paths], 2).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(arr.value(0), "1");
                assert_eq!(arr.value(1), "2");
            }
            _ => panic!("expected array"),
        }
    }
}
