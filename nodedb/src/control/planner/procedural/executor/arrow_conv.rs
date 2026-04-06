//! Arrow scalar to `nodedb_types::Value` conversion.
//!
//! Used by the statement executor to convert DataFusion evaluation results
//! into typed values for ASSIGN and OUT parameter handling.
//! Uses stack-friendly `nodedb_types::Value` instead of heap-heavy `serde_json::Value`.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use nodedb_types::Value;

/// Extract a single scalar value from an Arrow array at the given row index.
///
/// Returns `nodedb_types::Value` which uses stack-allocated variants for
/// integers and floats (no heap allocation). Previously used `serde_json::json!()`
/// which allocated via `Number::from()` for every scalar.
pub fn arrow_scalar_to_value(col: &Arc<dyn Array>, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }

    match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::Bool(a.value(row)))
            .unwrap_or(Value::Null),
        DataType::Int8 => col
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| Value::Integer(a.value(row) as i64))
            .unwrap_or(Value::Null),
        DataType::Int16 => col
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| Value::Integer(a.value(row) as i64))
            .unwrap_or(Value::Null),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| Value::Integer(a.value(row) as i64))
            .unwrap_or(Value::Null),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| Value::Integer(a.value(row)))
            .unwrap_or(Value::Null),
        DataType::UInt8 => col
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| Value::Integer(a.value(row) as i64))
            .unwrap_or(Value::Null),
        DataType::UInt16 => col
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| Value::Integer(a.value(row) as i64))
            .unwrap_or(Value::Null),
        DataType::UInt32 => col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| Value::Integer(a.value(row) as i64))
            .unwrap_or(Value::Null),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| Value::Integer(a.value(row) as i64))
            .unwrap_or(Value::Null),
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| Value::Float(a.value(row) as f64))
            .unwrap_or(Value::Null),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| Value::Float(a.value(row)))
            .unwrap_or(Value::Null),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| Value::String(a.value(row).to_string()))
            .unwrap_or(Value::Null),
        DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| Value::String(a.value(row).to_string()))
            .unwrap_or(Value::Null),
        _ => {
            // Fallback: format as string via ScalarValue.
            let scalar = datafusion::common::ScalarValue::try_from_array(col, row);
            match scalar {
                Ok(s) => Value::String(s.to_string()),
                Err(_) => Value::Null,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn null_value() {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![None]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Null);
    }

    #[test]
    fn int32_value() {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![42]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Integer(42));
    }

    #[test]
    fn float64_value() {
        let arr: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.5]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Float(1.5));
    }

    #[test]
    fn string_value() {
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello"]));
        assert_eq!(
            arrow_scalar_to_value(&arr, 0),
            Value::String("hello".into())
        );
    }

    #[test]
    fn boolean_value() {
        let arr: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true]));
        assert_eq!(arrow_scalar_to_value(&arr, 0), Value::Bool(true));
    }
}
