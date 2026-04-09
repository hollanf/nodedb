//! Dynamic value type for document fields and SQL parameters.
//!
//! Covers the value types needed for AI agent workloads: strings, numbers,
//! booleans, binary blobs (embeddings), arrays, and nested objects.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::datetime::{NdbDateTime, NdbDuration};
use crate::geometry::Geometry;

/// A dynamic value that can represent any field type in a document
/// or any parameter in a SQL query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Value {
    #[default]
    /// SQL NULL / missing value.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Integer(i64),
    /// 64-bit floating point.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes (embeddings, serialized blobs).
    Bytes(Vec<u8>),
    /// Ordered array of values.
    Array(Vec<Value>),
    /// Nested key-value object.
    Object(HashMap<String, Value>),
    /// UUID (any version, stored as 36-char hyphenated string).
    Uuid(String),
    /// ULID (26-char Crockford Base32).
    Ulid(String),
    /// UTC timestamp with microsecond precision.
    DateTime(NdbDateTime),
    /// Duration with microsecond precision (signed).
    Duration(NdbDuration),
    /// Arbitrary-precision decimal (financial calculations, exact arithmetic).
    Decimal(rust_decimal::Decimal),
    /// GeoJSON-compatible geometry (Point, LineString, Polygon, etc.).
    Geometry(Geometry),
    /// Ordered set of unique values (auto-deduplicated, maintains insertion order).
    Set(Vec<Value>),
    /// Compiled regex pattern (stored as pattern string).
    Regex(String),
    /// A range of values with optional bounds.
    Range {
        /// Start bound (None = unbounded).
        start: Option<Box<Value>>,
        /// End bound (None = unbounded).
        end: Option<Box<Value>>,
        /// Whether the end bound is inclusive (`..=` vs `..`).
        inclusive: bool,
    },
    /// A typed reference to another record: `table:id`.
    Record {
        /// The table/collection name.
        table: String,
        /// The record's document ID.
        id: String,
    },
}

impl Value {
    /// Returns true if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to extract as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to extract as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to extract as byte slice (for embeddings).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to extract as UUID string.
    pub fn as_uuid(&self) -> Option<&str> {
        match self {
            Value::Uuid(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as ULID string.
    pub fn as_ulid(&self) -> Option<&str> {
        match self {
            Value::Ulid(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as DateTime.
    pub fn as_datetime(&self) -> Option<&NdbDateTime> {
        match self {
            Value::DateTime(dt) => Some(dt),
            _ => None,
        }
    }

    /// Try to extract as Duration.
    pub fn as_duration(&self) -> Option<&NdbDuration> {
        match self {
            Value::Duration(d) => Some(d),
            _ => None,
        }
    }

    /// Try to extract as Decimal.
    pub fn as_decimal(&self) -> Option<&rust_decimal::Decimal> {
        match self {
            Value::Decimal(d) => Some(d),
            _ => None,
        }
    }

    /// Try to extract as Geometry.
    pub fn as_geometry(&self) -> Option<&Geometry> {
        match self {
            Value::Geometry(g) => Some(g),
            _ => None,
        }
    }

    /// Try to extract as a set (deduplicated array).
    pub fn as_set(&self) -> Option<&[Value]> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as regex pattern string.
    pub fn as_regex(&self) -> Option<&str> {
        match self {
            Value::Regex(r) => Some(r),
            _ => None,
        }
    }

    /// Try to extract as a record reference (table, id).
    pub fn as_record(&self) -> Option<(&str, &str)> {
        match self {
            Value::Record { table, id } => Some((table, id)),
            _ => None,
        }
    }

    /// Return the type name of this value as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Integer(_) => "int",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::Uuid(_) => "uuid",
            Value::Ulid(_) => "ulid",
            Value::DateTime(_) => "datetime",
            Value::Duration(_) => "duration",
            Value::Decimal(_) => "decimal",
            Value::Geometry(_) => "geometry",
            Value::Set(_) => "set",
            Value::Regex(_) => "regex",
            Value::Range { .. } => "range",
            Value::Record { .. } => "record",
        }
    }
}

/// Object/array access methods for use as internal document representation.
impl Value {
    /// Look up a field by name. Returns `None` for non-Object variants.
    pub fn get(&self, field: &str) -> Option<&Value> {
        match self {
            Value::Object(map) => map.get(field),
            _ => None,
        }
    }

    /// Mutable field lookup. Returns `None` for non-Object variants.
    pub fn get_mut(&mut self, field: &str) -> Option<&mut Value> {
        match self {
            Value::Object(map) => map.get_mut(field),
            _ => None,
        }
    }

    /// Try to extract as an object (HashMap reference).
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    /// Try to extract as a mutable object.
    pub fn as_object_mut(&mut self) -> Option<&mut HashMap<String, Value>> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    /// Try to extract as an array slice.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(arr) => Some(arr),
            Value::Set(arr) => Some(arr),
            _ => None,
        }
    }
}

impl Value {
    /// Coerced equality: `Value` vs `Value` with numeric/string coercion.
    ///
    /// Single source of truth for type coercion in filter evaluation.
    /// Used by `matches_binary` (msgpack path) and `matches_value` (Value path).
    pub fn eq_coerced(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Integer(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Integer(b)) => *a == *b as f64,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            // Coercion: number vs string
            (Value::Integer(a), Value::String(s)) => {
                s.parse::<i64>().is_ok_and(|n| *a == n)
                    || s.parse::<f64>().is_ok_and(|n| *a as f64 == n)
            }
            (Value::String(s), Value::Integer(b)) => {
                s.parse::<i64>().is_ok_and(|n| n == *b)
                    || s.parse::<f64>().is_ok_and(|n| n == *b as f64)
            }
            (Value::Float(a), Value::String(s)) => s.parse::<f64>().is_ok_and(|n| *a == n),
            (Value::String(s), Value::Float(b)) => s.parse::<f64>().is_ok_and(|n| n == *b),
            _ => false,
        }
    }

    /// Coerced ordering: `Value` vs `Value` with numeric/string coercion.
    ///
    /// Single source of truth for ordering in filter/sort evaluation.
    pub fn cmp_coerced(&self, other: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let self_f64 = match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        };
        let other_f64 = match other {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        };
        if let (Some(a), Some(b)) = (self_f64, other_f64) {
            return a.partial_cmp(&b).unwrap_or(Ordering::Equal);
        }
        let a_str = match self {
            Value::String(s) => s.as_str(),
            _ => return Ordering::Equal,
        };
        let b_str = match other {
            Value::String(s) => s.as_str(),
            _ => return Ordering::Equal,
        };
        a_str.cmp(b_str)
    }

    /// Get array elements (for IN/array operations).
    pub fn as_array_iter(&self) -> Option<impl Iterator<Item = &Value>> {
        match self {
            Value::Array(arr) | Value::Set(arr) => Some(arr.iter()),
            _ => None,
        }
    }
}

/// Convenience conversions.
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_owned())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

impl From<NdbDateTime> for Value {
    fn from(dt: NdbDateTime) -> Self {
        Value::DateTime(dt)
    }
}

impl From<NdbDuration> for Value {
    fn from(d: NdbDuration) -> Self {
        Value::Duration(d)
    }
}

impl From<rust_decimal::Decimal> for Value {
    fn from(d: rust_decimal::Decimal) -> Self {
        Value::Decimal(d)
    }
}

impl From<Geometry> for Value {
    fn from(g: Geometry) -> Self {
        Value::Geometry(g)
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Integer(i) => serde_json::json!(i),
            Value::Float(f) => serde_json::json!(f),
            Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
                serde_json::Value::String(s)
            }
            Value::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                serde_json::Value::String(hex)
            }
            Value::Array(arr) | Value::Set(arr) => {
                serde_json::Value::Array(arr.into_iter().map(serde_json::Value::from).collect())
            }
            Value::Object(map) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            ),
            Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
            Value::Duration(d) => serde_json::Value::String(d.to_string()),
            Value::Decimal(d) => serde_json::Value::String(d.to_string()),
            Value::Geometry(g) => serde_json::to_value(g).unwrap_or(serde_json::Value::Null),
            Value::Range { .. } | Value::Record { .. } => serde_json::Value::Null,
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else if let Some(u) = n.as_u64() {
                    Value::Integer(u as i64)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(map) => {
                Value::Object(map.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl Value {
    /// Convert to a SQL literal string for substitution into SQL text.
    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".into(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
                format!("'{}'", s.replace('\'', "''"))
            }
            Value::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                format!("'\\x{hex}'")
            }
            Value::Array(arr) | Value::Set(arr) => {
                let elements: Vec<String> = arr.iter().map(|v| v.to_sql_literal()).collect();
                format!("ARRAY[{}]", elements.join(", "))
            }
            Value::Object(map) => {
                let json_str = serde_json::to_string(&serde_json::Value::Object(
                    map.iter()
                        .map(|(k, v)| (k.clone(), value_to_json(v)))
                        .collect(),
                ))
                .unwrap_or_default();
                format!("'{}'", json_str.replace('\'', "''"))
            }
            Value::DateTime(dt) => format!("'{dt}'"),
            Value::Duration(d) => format!("'{d}'"),
            Value::Decimal(d) => d.to_string(),
            Value::Geometry(g) => format!("'{}'", serde_json::to_string(g).unwrap_or_default()),
            Value::Range { .. } | Value::Record { .. } => "NULL".into(),
        }
    }
}

/// Convert nodedb_types::Value back to serde_json::Value (for object serialization).
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Array(arr) | Value::Set(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json).collect())
        }
        Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
        other => serde_json::Value::String(other.to_sql_literal()),
    }
}

// ─── Manual zerompk implementation ──────────────────────────────────────────
//
// Cannot use derive because `rust_decimal::Decimal` is an external type.
// Format: [variant_tag: u8, ...payload fields] as a msgpack array.

impl zerompk::ToMessagePack for Value {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            Value::Null => {
                writer.write_array_len(1)?;
                writer.write_u8(0)
            }
            Value::Bool(b) => {
                writer.write_array_len(2)?;
                writer.write_u8(1)?;
                writer.write_boolean(*b)
            }
            Value::Integer(i) => {
                writer.write_array_len(2)?;
                writer.write_u8(2)?;
                writer.write_i64(*i)
            }
            Value::Float(f) => {
                writer.write_array_len(2)?;
                writer.write_u8(3)?;
                writer.write_f64(*f)
            }
            Value::String(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(4)?;
                writer.write_string(s)
            }
            Value::Bytes(b) => {
                writer.write_array_len(2)?;
                writer.write_u8(5)?;
                writer.write_binary(b)
            }
            Value::Array(arr) => {
                writer.write_array_len(2)?;
                writer.write_u8(6)?;
                arr.write(writer)
            }
            Value::Object(map) => {
                writer.write_array_len(2)?;
                writer.write_u8(7)?;
                map.write(writer)
            }
            Value::Uuid(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(8)?;
                writer.write_string(s)
            }
            Value::Ulid(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(9)?;
                writer.write_string(s)
            }
            Value::DateTime(dt) => {
                writer.write_array_len(2)?;
                writer.write_u8(10)?;
                dt.write(writer)
            }
            Value::Duration(d) => {
                writer.write_array_len(2)?;
                writer.write_u8(11)?;
                d.write(writer)
            }
            Value::Decimal(d) => {
                writer.write_array_len(2)?;
                writer.write_u8(12)?;
                writer.write_binary(&d.serialize())
            }
            Value::Geometry(g) => {
                writer.write_array_len(2)?;
                writer.write_u8(13)?;
                g.write(writer)
            }
            Value::Set(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(14)?;
                s.write(writer)
            }
            Value::Regex(r) => {
                writer.write_array_len(2)?;
                writer.write_u8(15)?;
                writer.write_string(r)
            }
            Value::Range {
                start,
                end,
                inclusive,
            } => {
                writer.write_array_len(4)?;
                writer.write_u8(16)?;
                start.write(writer)?;
                end.write(writer)?;
                writer.write_boolean(*inclusive)
            }
            Value::Record { table, id } => {
                writer.write_array_len(3)?;
                writer.write_u8(17)?;
                writer.write_string(table)?;
                writer.write_string(id)
            }
        }
    }
}

impl<'a> zerompk::FromMessagePack<'a> for Value {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len == 0 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 1,
                actual: 0,
            });
        }
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(Value::Null),
            1 => Ok(Value::Bool(reader.read_boolean()?)),
            2 => Ok(Value::Integer(reader.read_i64()?)),
            3 => Ok(Value::Float(reader.read_f64()?)),
            4 => Ok(Value::String(reader.read_string()?.into_owned())),
            5 => Ok(Value::Bytes(reader.read_binary()?.into_owned())),
            6 => Ok(Value::Array(Vec::<Value>::read(reader)?)),
            7 => Ok(Value::Object(HashMap::<String, Value>::read(reader)?)),
            8 => Ok(Value::Uuid(reader.read_string()?.into_owned())),
            9 => Ok(Value::Ulid(reader.read_string()?.into_owned())),
            10 => Ok(Value::DateTime(NdbDateTime::read(reader)?)),
            11 => Ok(Value::Duration(NdbDuration::read(reader)?)),
            12 => {
                let cow = reader.read_binary()?;
                if cow.len() != 16 {
                    return Err(zerompk::Error::BufferTooSmall);
                }
                let mut buf = [0u8; 16];
                buf.copy_from_slice(&cow);
                Ok(Value::Decimal(rust_decimal::Decimal::deserialize(buf)))
            }
            13 => Ok(Value::Geometry(Geometry::read(reader)?)),
            14 => Ok(Value::Set(Vec::<Value>::read(reader)?)),
            15 => Ok(Value::Regex(reader.read_string()?.into_owned())),
            16 => {
                let start = Option::<Box<Value>>::read(reader)?;
                let end = Option::<Box<Value>>::read(reader)?;
                let inclusive = reader.read_boolean()?;
                Ok(Value::Range {
                    start,
                    end,
                    inclusive,
                })
            }
            17 => {
                let table = reader.read_string()?.into_owned();
                let id = reader.read_string()?.into_owned();
                Ok(Value::Record { table, id })
            }
            _ => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_type_checks() {
        assert!(Value::Null.is_null());
        assert!(!Value::Bool(true).is_null());

        assert_eq!(Value::String("hi".into()).as_str(), Some("hi"));
        assert_eq!(Value::Integer(42).as_i64(), Some(42));
        assert_eq!(Value::Float(2.78).as_f64(), Some(2.78));
        assert_eq!(Value::Integer(10).as_f64(), Some(10.0));
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bytes(vec![1, 2]).as_bytes(), Some(&[1, 2][..]));
    }

    #[test]
    fn from_conversions() {
        let s: Value = "hello".into();
        assert_eq!(s.as_str(), Some("hello"));

        let i: Value = 42i64.into();
        assert_eq!(i.as_i64(), Some(42));

        let f: Value = 2.78f64.into();
        assert_eq!(f.as_f64(), Some(2.78));
    }

    #[test]
    fn nested_value() {
        let nested = Value::Object({
            let mut m = HashMap::new();
            m.insert(
                "inner".into(),
                Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            );
            m
        });
        assert!(!nested.is_null());
    }

    // ── Coercion matrix tests ────────────────────────────────────────

    #[test]
    fn eq_coerced_same_type() {
        assert!(Value::Null.eq_coerced(&Value::Null));
        assert!(Value::Bool(true).eq_coerced(&Value::Bool(true)));
        assert!(!Value::Bool(true).eq_coerced(&Value::Bool(false)));
        assert!(Value::Integer(42).eq_coerced(&Value::Integer(42)));
        assert!(Value::Float(3.14).eq_coerced(&Value::Float(3.14)));
        assert!(Value::String("hello".into()).eq_coerced(&Value::String("hello".into())));
    }

    #[test]
    fn eq_coerced_int_float() {
        assert!(Value::Integer(5).eq_coerced(&Value::Float(5.0)));
        assert!(Value::Float(5.0).eq_coerced(&Value::Integer(5)));
        assert!(!Value::Integer(5).eq_coerced(&Value::Float(5.1)));
    }

    #[test]
    fn eq_coerced_string_number() {
        // String "5" equals Integer 5.
        assert!(Value::String("5".into()).eq_coerced(&Value::Integer(5)));
        assert!(Value::Integer(5).eq_coerced(&Value::String("5".into())));
        // String "3.14" equals Float 3.14.
        assert!(Value::String("3.14".into()).eq_coerced(&Value::Float(3.14)));
        assert!(Value::Float(3.14).eq_coerced(&Value::String("3.14".into())));
        // Non-numeric string does not equal number.
        assert!(!Value::String("abc".into()).eq_coerced(&Value::Integer(5)));
        assert!(!Value::Integer(5).eq_coerced(&Value::String("abc".into())));
    }

    #[test]
    fn eq_coerced_cross_type_false() {
        // Bool vs Integer: no coercion.
        assert!(!Value::Bool(true).eq_coerced(&Value::Integer(1)));
        // Null vs anything: only Null == Null.
        assert!(!Value::Null.eq_coerced(&Value::Integer(0)));
        assert!(!Value::Null.eq_coerced(&Value::String("".into())));
    }

    #[test]
    fn cmp_coerced_numeric() {
        use std::cmp::Ordering;
        assert_eq!(
            Value::Integer(5).cmp_coerced(&Value::Integer(10)),
            Ordering::Less
        );
        assert_eq!(
            Value::Integer(10).cmp_coerced(&Value::Float(5.0)),
            Ordering::Greater
        );
        assert_eq!(
            Value::String("90".into()).cmp_coerced(&Value::Integer(80)),
            Ordering::Greater
        );
        assert_eq!(
            Value::Float(3.14).cmp_coerced(&Value::String("3.14".into())),
            Ordering::Equal
        );
    }

    #[test]
    fn cmp_coerced_string_fallback() {
        use std::cmp::Ordering;
        assert_eq!(
            Value::String("abc".into()).cmp_coerced(&Value::String("def".into())),
            Ordering::Less
        );
        assert_eq!(
            Value::String("z".into()).cmp_coerced(&Value::String("a".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn eq_coerced_symmetry() {
        // Verify a == b iff b == a for all cross-type pairs.
        let cases = [
            (Value::Integer(42), Value::String("42".into())),
            (Value::Float(3.14), Value::String("3.14".into())),
            (Value::Integer(5), Value::Float(5.0)),
        ];
        for (a, b) in &cases {
            assert_eq!(
                a.eq_coerced(b),
                b.eq_coerced(a),
                "symmetry violated for {a:?} vs {b:?}"
            );
        }
    }
}
