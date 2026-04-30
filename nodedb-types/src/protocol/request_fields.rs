//! RequestFields — tagged union of operation-specific field sets.
//!
//! # Wire format (MsgPack)
//!
//! Encoded as a 2-element array: `[discriminant: u8, payload_map]`.
//!
//! ```text
//! [discriminant: u8]  [payload: TextFields map]
//! ```
//!
//! Discriminant table:
//! ```text
//! 0x01 = Text
//! ```
//!
//! Unknown discriminants are rejected with a typed error.

use serde::{Deserialize, Serialize};

use super::text_fields::TextFields;

// ─── Discriminant constants ────────────────────────────────────────

/// Discriminant for `RequestFields::Text`.
const DISC_TEXT: u8 = 0x01;

// ─── RequestFields ─────────────────────────────────────────────────

/// Operation-specific request fields.
///
/// Each variant carries only the fields needed for that operation.
/// Unknown discriminants are rejected with a typed error.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RequestFields {
    /// Auth, SQL, DDL, Explain, Set, Show, Reset, Begin, Commit, Rollback,
    /// CopyFrom, Ping — all use a subset of these text fields.
    Text(TextFields),
}

// ─── ToMessagePack ─────────────────────────────────────────────────

impl zerompk::ToMessagePack for RequestFields {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        match self {
            RequestFields::Text(fields) => {
                writer.write_u8(DISC_TEXT)?;
                fields.write(writer)?;
            }
        }
        Ok(())
    }
}

// ─── FromMessagePack ───────────────────────────────────────────────

impl<'a> zerompk::FromMessagePack<'a> for RequestFields {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 2 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 2,
                actual: len,
            });
        }
        let discriminant = reader.read_u8()?;
        match discriminant {
            DISC_TEXT => {
                let fields = TextFields::read(reader)?;
                Ok(RequestFields::Text(fields))
            }
            other => Err(zerompk::Error::InvalidMarker(other)),
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Text discriminant roundtrip.
    #[test]
    fn requestfields_text_discriminant_roundtrip() {
        let rf = RequestFields::Text(TextFields {
            sql: Some("SELECT 42".into()),
            top_k: Some(5),
            ..Default::default()
        });
        let bytes = zerompk::to_msgpack_vec(&rf).expect("encode");
        // Verify outer array header: fixarray(2) = 0x92
        assert_eq!(bytes[0], 0x92, "expected fixarray(2)");
        // Verify discriminant byte
        assert_eq!(bytes[1], DISC_TEXT, "expected Text discriminant");

        let decoded: RequestFields = zerompk::from_msgpack(&bytes).expect("decode");
        match decoded {
            RequestFields::Text(tf) => {
                assert_eq!(tf.sql.as_deref(), Some("SELECT 42"));
                assert_eq!(tf.top_k, Some(5));
            }
        }
    }

    /// Unknown discriminant is rejected with a typed error.
    #[test]
    fn requestfields_unknown_discriminant_rejected() {
        // Build: fixarray(2), discriminant=0xFF, fixmap(0) (empty TextFields)
        let buf = vec![
            0x92u8, // fixarray(2)
            0xFF,   // unknown discriminant
            0x80,   // fixmap(0) — empty map
        ];
        let result: zerompk::Result<RequestFields> = zerompk::from_msgpack(&buf);
        assert!(result.is_err(), "expected error for unknown discriminant");
        // Must be an UnknownVariant error
        match result.unwrap_err() {
            zerompk::Error::InvalidMarker(v) => assert_eq!(v, 0xFF),
            e => panic!(
                "expected InvalidMarker for unknown discriminant, got: {:?}",
                e
            ),
        }
    }
}
