//! Zero-copy MessagePack serialization for `serde_json::Value`.
//!
//! Provides standalone functions and a newtype wrapper `JsonValue`
//! with `ToMessagePack`/`FromMessagePack` impls, enabling zerompk
//! serialization of JSON values without rmp_serde.

use zerompk::{ToMessagePack, Write};

/// Newtype wrapper around `serde_json::Value` implementing zerompk traits.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonValue(pub serde_json::Value);

impl From<serde_json::Value> for JsonValue {
    #[inline]
    fn from(v: serde_json::Value) -> Self {
        Self(v)
    }
}

impl From<JsonValue> for serde_json::Value {
    #[inline]
    fn from(v: JsonValue) -> Self {
        v.0
    }
}

impl std::ops::Deref for JsonValue {
    type Target = serde_json::Value;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for JsonValue {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// ─── Serialization (via zerompk Write trait) ───────────────────────────────

impl ToMessagePack for JsonValue {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        write_json_value(&self.0, writer)
    }
}

fn write_json_value<W: Write>(val: &serde_json::Value, writer: &mut W) -> zerompk::Result<()> {
    match val {
        serde_json::Value::Null => writer.write_nil(),
        serde_json::Value::Bool(b) => writer.write_boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                writer.write_i64(i)
            } else if let Some(u) = n.as_u64() {
                writer.write_u64(u)
            } else if let Some(f) = n.as_f64() {
                writer.write_f64(f)
            } else {
                writer.write_f64(0.0)
            }
        }
        serde_json::Value::String(s) => writer.write_string(s),
        serde_json::Value::Array(arr) => {
            writer.write_array_len(arr.len())?;
            for item in arr {
                write_json_value(item, writer)?;
            }
            Ok(())
        }
        serde_json::Value::Object(map) => {
            writer.write_map_len(map.len())?;
            for (key, val) in map {
                writer.write_string(key)?;
                write_json_value(val, writer)?;
            }
            Ok(())
        }
    }
}

// ─── Deserialization (raw msgpack byte parsing) ────────────────────────────

impl<'a> zerompk::FromMessagePack<'a> for JsonValue {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        // zerompk's Read trait doesn't expose peek, so we can't detect the
        // type ahead of time. Instead, we use the tag reader which handles
        // the first-byte dispatch internally.
        //
        // Actually, we'll use a different strategy: since zerompk's Read
        // methods fail on wrong marker without advancing (SliceReader peeks
        // first), we try types in priority order. But IOReader advances...
        //
        // Safest: rely on the fact that NodeDB always uses SliceReader
        // (from_msgpack uses SliceReader). We document this constraint.
        //
        // Try each type — SliceReader::peek_byte doesn't advance on the
        // initial check, but the read methods may advance on partial reads.
        //
        // The cleanest solution: delegate to our raw parser.
        //
        // We can't easily do this through the Read trait, so we'll use a
        // workaround: read raw bytes and parse them.

        // Unfortunately we can't get the raw bytes from an arbitrary Read.
        // So we'll serialize/deserialize through our raw functions, accepting
        // that FromMessagePack for JsonValue only works with SliceReader.
        //
        // For now, we'll use rmp_serde for deserialization as a bridge.
        // The write path (which is the hot path for encoding) uses zerompk.

        // Actually — let's try the simplest approach. zerompk Read methods
        // on SliceReader DO peek first (checking the marker byte), and only
        // advance past it if the marker matches. If it doesn't match, they
        // return Err without advancing. This is true for SliceReader but
        // not guaranteed for IOReader. Since all our code uses from_msgpack
        // (which uses SliceReader), this works.

        // Try nil
        if reader.read_nil().is_ok() {
            return Ok(JsonValue(serde_json::Value::Null));
        }
        // Try bool
        if let Ok(b) = reader.read_boolean() {
            return Ok(JsonValue(serde_json::Value::Bool(b)));
        }
        // Try i64 (covers positive fixint, negative fixint, int8-int64, uint8-uint32)
        if let Ok(i) = reader.read_i64() {
            return Ok(JsonValue(serde_json::Value::Number(i.into())));
        }
        // Try u64 (covers uint64 values > i64::MAX)
        if let Ok(u) = reader.read_u64() {
            return Ok(JsonValue(serde_json::Value::Number(u.into())));
        }
        // Try f64
        if let Ok(f) = reader.read_f64() {
            return Ok(JsonValue(serde_json::json!(f)));
        }
        // Try f32
        if let Ok(f) = reader.read_f32() {
            return Ok(JsonValue(serde_json::json!(f as f64)));
        }
        // Try string
        if let Ok(s) = reader.read_string() {
            return Ok(JsonValue(serde_json::Value::String(s.into_owned())));
        }
        // Try array
        if let Ok(len) = reader.read_array_len() {
            reader.increment_depth()?;
            let mut arr = Vec::with_capacity(len.min(4096));
            for _ in 0..len {
                let JsonValue(v) = JsonValue::read(reader)?;
                arr.push(v);
            }
            reader.decrement_depth();
            return Ok(JsonValue(serde_json::Value::Array(arr)));
        }
        // Try map
        if let Ok(len) = reader.read_map_len() {
            reader.increment_depth()?;
            let mut map = serde_json::Map::with_capacity(len.min(4096));
            for _ in 0..len {
                let key = reader.read_string()?;
                let JsonValue(val) = JsonValue::read(reader)?;
                map.insert(key.into_owned(), val);
            }
            reader.decrement_depth();
            return Ok(JsonValue(serde_json::Value::Object(map)));
        }

        Err(zerompk::Error::InvalidMarker(0))
    }
}

/// Serialize a `serde_json::Value` to MessagePack bytes.
#[inline]
pub fn json_to_msgpack(value: &serde_json::Value) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&JsonValue(value.clone()))
}

/// Deserialize a `serde_json::Value` from MessagePack bytes.
#[inline]
pub fn json_from_msgpack(bytes: &[u8]) -> zerompk::Result<serde_json::Value> {
    zerompk::from_msgpack::<JsonValue>(bytes).map(|v| v.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_null() {
        let val = json!(null);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_bool() {
        for val in [json!(true), json!(false)] {
            let bytes = json_to_msgpack(&val).unwrap();
            let restored = json_from_msgpack(&bytes).unwrap();
            assert_eq!(val, restored);
        }
    }

    #[test]
    fn roundtrip_integers() {
        for val in [
            json!(0),
            json!(42),
            json!(-1),
            json!(i64::MAX),
            json!(i64::MIN),
        ] {
            let bytes = json_to_msgpack(&val).unwrap();
            let restored = json_from_msgpack(&bytes).unwrap();
            assert_eq!(val, restored);
        }
    }

    #[test]
    fn roundtrip_float() {
        let val = json!(3.14);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_string() {
        let val = json!("hello world");
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_array() {
        let val = json!([1, "two", null, true, 3.14]);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_object() {
        let val = json!({"name": "Alice", "age": 30, "active": true, "scores": [95, 87]});
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_nested() {
        let val = json!({
            "users": [
                {"id": 1, "name": "Alice", "meta": null},
                {"id": 2, "name": "Bob", "meta": {"role": "admin"}}
            ],
            "count": 2
        });
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_empty_object() {
        let val = json!({});
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_empty_array() {
        let val = json!([]);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_u64_large() {
        // Value larger than i64::MAX
        let val = json!(u64::MAX);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }
}
