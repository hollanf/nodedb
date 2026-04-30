//! Batch operation payload types.

use serde::{Deserialize, Serialize};

use crate::json_msgpack::JsonValue;

/// A single vector in a batch insert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchVector {
    pub id: String,
    pub embedding: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl zerompk::ToMessagePack for BatchVector {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        writer.write_string(&self.id)?;
        self.embedding.write(writer)?;
        self.metadata
            .as_ref()
            .map(|v| JsonValue(v.clone()))
            .write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for BatchVector {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 3 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 3,
                actual: len,
            });
        }
        let id = reader.read_string()?.into_owned();
        let embedding = Vec::<f32>::read(reader)?;
        let metadata = Option::<JsonValue>::read(reader)?.map(|v| v.0);
        Ok(Self {
            id,
            embedding,
            metadata,
        })
    }
}

/// A single document in a batch insert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDocument {
    pub id: String,
    pub fields: serde_json::Value,
}

impl zerompk::ToMessagePack for BatchDocument {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        writer.write_string(&self.id)?;
        JsonValue(self.fields.clone()).write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for BatchDocument {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 2 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 2,
                actual: len,
            });
        }
        let id = reader.read_string()?.into_owned();
        let fields = JsonValue::read(reader)?.0;
        Ok(Self { id, fields })
    }
}
