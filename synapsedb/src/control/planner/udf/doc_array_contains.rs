use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{BinaryArray, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::nav::{expand_to_array, navigate_json, navigate_rmpv, rmpv_to_string};

/// `doc_array_contains(document, path, value)` — Check if an array at path contains a value.
///
/// Navigates to the array at `path` in the MessagePack document and checks whether
/// any element matches `value`. Comparison is string-based for strings, numeric for
/// numbers (f64 epsilon). Returns false if the path doesn't exist, the target isn't
/// an array, or the blob is invalid.
///
/// Also accepts JSON UTF-8 strings for backward compatibility.
#[derive(Debug)]
pub struct DocArrayContains {
    signature: Signature,
}

impl DocArrayContains {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for DocArrayContains {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for DocArrayContains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "doc_array_contains"
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
        let values = expand_to_array(&args[2], num_rows)?;

        let paths = paths
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "doc_array_contains: expected Utf8 for path".into(),
                )
            })?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "doc_array_contains: expected Utf8 for value".into(),
                )
            })?;

        if let Some(bin_docs) = docs.as_any().downcast_ref::<BinaryArray>() {
            let result: BooleanArray = bin_docs
                .iter()
                .zip(paths.iter())
                .zip(values.iter())
                .map(|((doc, path), val)| Some(contains_msgpack(doc?, path?, val?)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else if let Some(str_docs) = docs.as_any().downcast_ref::<StringArray>() {
            let result: BooleanArray = str_docs
                .iter()
                .zip(paths.iter())
                .zip(values.iter())
                .map(|((doc, path), val)| Some(contains_json(doc?, path?, val?)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "doc_array_contains: expected Binary or Utf8 for document".into(),
            ))
        }
    }
}

fn contains_msgpack(data: &[u8], path: &str, needle: &str) -> bool {
    let value: rmpv::Value = match rmpv::decode::read_value(&mut &data[..]) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let target = match navigate_rmpv(&value, path) {
        Some(v) => v,
        None => return false,
    };
    let arr = match target.as_array() {
        Some(a) => a,
        None => return false,
    };

    // Numeric comparison.
    if let Ok(needle_num) = needle.parse::<f64>() {
        for item in arr {
            if let Some(n) = item.as_f64() {
                if (n - needle_num).abs() < f64::EPSILON {
                    return true;
                }
            }
            if let Some(i) = item.as_i64() {
                if (i as f64 - needle_num).abs() < f64::EPSILON {
                    return true;
                }
            }
            if let Some(u) = item.as_u64() {
                if (u as f64 - needle_num).abs() < f64::EPSILON {
                    return true;
                }
            }
        }
    }

    // String comparison.
    for item in arr {
        let item_str = rmpv_to_string(item);
        if item_str == needle {
            return true;
        }
    }

    false
}

fn contains_json(json_str: &str, path: &str, needle: &str) -> bool {
    let parsed: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let target = match navigate_json(&parsed, path) {
        Some(v) => v,
        None => return false,
    };
    let arr = match target.as_array() {
        Some(a) => a,
        None => return false,
    };

    if let Ok(needle_num) = needle.parse::<f64>() {
        for item in arr {
            if let Some(n) = item.as_f64() {
                if (n - needle_num).abs() < f64::EPSILON {
                    return true;
                }
            }
        }
    }

    for item in arr {
        match item {
            serde_json::Value::String(s) if s == needle => return true,
            serde_json::Value::Bool(b) => {
                if (needle == "true" && *b) || (needle == "false" && !*b) {
                    return true;
                }
            }
            other => {
                let s = other.to_string();
                if s == needle {
                    return true;
                }
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::super::nav::test_util::to_msgpack;
    use super::*;

    #[test]
    fn msgpack_contains_string() {
        let json = serde_json::json!({"tags": ["admin", "user", "editor"]});
        let data = to_msgpack(&json);
        assert!(contains_msgpack(&data, "$.tags", "admin"));
        assert!(!contains_msgpack(&data, "$.tags", "guest"));
    }

    #[test]
    fn msgpack_contains_number() {
        let json = serde_json::json!({"scores": [10, 20, 30]});
        let data = to_msgpack(&json);
        assert!(contains_msgpack(&data, "$.scores", "20"));
        assert!(!contains_msgpack(&data, "$.scores", "25"));
    }

    #[test]
    fn msgpack_contains_nested() {
        let json = serde_json::json!({"user": {"roles": ["admin"]}});
        let data = to_msgpack(&json);
        assert!(contains_msgpack(&data, "$.user.roles", "admin"));
    }

    #[test]
    fn msgpack_not_array_returns_false() {
        let json = serde_json::json!({"name": "alice"});
        let data = to_msgpack(&json);
        assert!(!contains_msgpack(&data, "$.name", "alice"));
    }

    #[test]
    fn msgpack_missing_path_returns_false() {
        let json = serde_json::json!({"a": 1});
        let data = to_msgpack(&json);
        assert!(!contains_msgpack(&data, "$.missing", "1"));
    }

    #[test]
    fn msgpack_invalid_data() {
        assert!(!contains_msgpack(&[0xff, 0xfe], "$.tags", "x"));
    }

    #[test]
    fn json_compat_contains() {
        assert!(contains_json(
            r#"{"tags": ["admin", "user"]}"#,
            "$.tags",
            "admin"
        ));
        assert!(!contains_json(r#"{"tags": ["admin"]}"#, "$.tags", "guest"));
    }

    #[test]
    fn json_contains_boolean() {
        assert!(contains_json(
            r#"{"flags": [true, false]}"#,
            "$.flags",
            "true"
        ));
    }

    #[test]
    fn udf_batch_binary() {
        let udf = DocArrayContains::new();
        let doc1 = to_msgpack(&serde_json::json!({"tags": ["a", "b"]}));
        let doc2 = to_msgpack(&serde_json::json!({"tags": ["c"]}));

        let docs = ColumnarValue::Array(Arc::new(BinaryArray::from(vec![
            doc1.as_slice(),
            doc2.as_slice(),
        ])));
        let paths =
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some("$.tags".into())));
        let values = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some("a".into())));

        let result = udf.invoke_batch(&[docs, paths, values], 2).unwrap();
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
