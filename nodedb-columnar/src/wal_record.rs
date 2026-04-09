//! WAL record types for columnar operations.
//!
//! Each mutation (INSERT, DELETE, compaction commit) produces a WAL record
//! that is written before the mutation is applied. On crash recovery, WAL
//! records are replayed to reconstruct the memtable, delete bitmaps, and
//! segment metadata.
//!
//! Records are serialized as MessagePack for compact wire representation.

use serde::{Deserialize, Serialize};
use sonic_rs;
use zerompk::{FromMessagePack, ToMessagePack};

/// A WAL record for a columnar collection operation.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub enum ColumnarWalRecord {
    /// A row was inserted into the memtable.
    ///
    /// Contains the collection name and the row data as packed binary
    /// (the columnar wire format, not MessagePack). On replay, the row
    /// is re-inserted into the memtable.
    InsertRow {
        collection: String,
        /// Row data as packed binary values. Each value is encoded per its
        /// column type: i64 as 8 LE bytes, f64 as 8 LE bytes, strings as
        /// length-prefixed UTF-8, etc.
        row_data: Vec<u8>,
    },

    /// Rows were marked as deleted in a segment's delete bitmap.
    ///
    /// On replay, these row indices are re-applied to the segment's
    /// delete bitmap.
    DeleteRows {
        collection: String,
        segment_id: u32,
        row_indices: Vec<u32>,
    },

    /// A compaction was committed: old segments replaced with new ones.
    ///
    /// This is the atomic commit point of the 3-phase compaction protocol.
    /// On replay:
    /// - If new segments exist on disk: complete the metadata swap.
    /// - If new segments don't exist: the compaction was interrupted before
    ///   writing; discard and treat old segments as authoritative.
    CompactionCommit {
        collection: String,
        old_segment_ids: Vec<u32>,
        new_segment_ids: Vec<u32>,
    },

    /// The memtable was flushed to a new segment.
    ///
    /// On replay, if the segment file exists, update metadata to include it.
    /// If it doesn't exist, the flush was interrupted; rows are already in
    /// the memtable via InsertRow records.
    MemtableFlushed {
        collection: String,
        segment_id: u32,
        row_count: u64,
    },
}

impl ColumnarWalRecord {
    /// Collection name this record belongs to.
    pub fn collection(&self) -> &str {
        match self {
            Self::InsertRow { collection, .. }
            | Self::DeleteRows { collection, .. }
            | Self::CompactionCommit { collection, .. }
            | Self::MemtableFlushed { collection, .. } => collection,
        }
    }

    /// Serialize the record to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::error::ColumnarError> {
        zerompk::to_msgpack_vec(self)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))
    }

    /// Deserialize a record from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::error::ColumnarError> {
        zerompk::from_msgpack(data)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))
    }
}

/// Encode a row of values into the columnar wire format for WAL records.
///
/// Each value is written as: [type_tag: u8][value_bytes].
/// This is more compact than MessagePack for typed columns and enables
/// direct replay into the memtable without schema interpretation overhead.
pub fn encode_row_for_wal(values: &[nodedb_types::value::Value]) -> Vec<u8> {
    use nodedb_types::value::Value;

    let mut buf = Vec::with_capacity(values.len() * 10); // Rough estimate.

    for value in values {
        match value {
            Value::Null => buf.push(0),
            Value::Integer(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Bool(v) => {
                buf.push(3);
                buf.push(*v as u8);
            }
            Value::String(s) => {
                buf.push(4);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Bytes(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::DateTime(dt) => {
                buf.push(6);
                buf.extend_from_slice(&dt.micros.to_le_bytes());
            }
            Value::Decimal(d) => {
                buf.push(7);
                buf.extend_from_slice(&d.serialize());
            }
            Value::Uuid(s) => {
                buf.push(8);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Array(arr) => {
                // Vectors stored as: tag(9) + count(u32) + f32 values.
                buf.push(9);
                buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
                for v in arr {
                    let f = match v {
                        Value::Float(f) => *f as f32,
                        Value::Integer(n) => *n as f32,
                        _ => 0.0,
                    };
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
            _ => {
                // Geometry and other complex types: serialize as JSON bytes.
                buf.push(10);
                let json = sonic_rs::to_vec(value).unwrap_or_default();
                buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
                buf.extend_from_slice(&json);
            }
        }
    }

    buf
}

/// Decode a row from the columnar wire format back into Values.
pub fn decode_row_from_wal(
    data: &[u8],
) -> Result<Vec<nodedb_types::value::Value>, crate::error::ColumnarError> {
    use nodedb_types::value::Value;

    let mut values = Vec::new();
    let mut cursor = 0;

    while cursor < data.len() {
        let tag = data[cursor];
        cursor += 1;

        let value = match tag {
            0 => Value::Null,
            1 => {
                let v = i64::from_le_bytes(data[cursor..cursor + 8].try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated i64".into())
                })?);
                cursor += 8;
                Value::Integer(v)
            }
            2 => {
                let v = f64::from_le_bytes(data[cursor..cursor + 8].try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated f64".into())
                })?);
                cursor += 8;
                Value::Float(v)
            }
            3 => {
                let v = data[cursor] != 0;
                cursor += 1;
                Value::Bool(v)
            }
            4 | 5 | 8 => {
                let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated len".into())
                })?) as usize;
                cursor += 4;
                let bytes = &data[cursor..cursor + len];
                cursor += len;
                match tag {
                    4 => Value::String(String::from_utf8_lossy(bytes).into_owned()),
                    5 => Value::Bytes(bytes.to_vec()),
                    8 => Value::Uuid(String::from_utf8_lossy(bytes).into_owned()),
                    _ => unreachable!(),
                }
            }
            6 => {
                let micros =
                    i64::from_le_bytes(data[cursor..cursor + 8].try_into().map_err(|_| {
                        crate::error::ColumnarError::Serialization("truncated timestamp".into())
                    })?);
                cursor += 8;
                Value::DateTime(nodedb_types::datetime::NdbDateTime::from_micros(micros))
            }
            7 => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&data[cursor..cursor + 16]);
                cursor += 16;
                Value::Decimal(rust_decimal::Decimal::deserialize(bytes))
            }
            9 => {
                let count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().map_err(|_| {
                        crate::error::ColumnarError::Serialization("truncated vector count".into())
                    })?) as usize;
                cursor += 4;
                let mut arr = Vec::with_capacity(count);
                for _ in 0..count {
                    let f =
                        f32::from_le_bytes(data[cursor..cursor + 4].try_into().map_err(|_| {
                            crate::error::ColumnarError::Serialization("truncated f32".into())
                        })?);
                    cursor += 4;
                    arr.push(Value::Float(f as f64));
                }
                Value::Array(arr)
            }
            10 => {
                let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated json len".into())
                })?) as usize;
                cursor += 4;
                let json_bytes = &data[cursor..cursor + len];
                cursor += len;
                sonic_rs::from_slice(json_bytes).unwrap_or(Value::Null)
            }
            _ => {
                return Err(crate::error::ColumnarError::Serialization(format!(
                    "unknown WAL value tag: {tag}"
                )));
            }
        };

        values.push(value);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use nodedb_types::datetime::NdbDateTime;
    use nodedb_types::value::Value;

    use super::*;

    #[test]
    fn wal_record_roundtrip() {
        let records = vec![
            ColumnarWalRecord::InsertRow {
                collection: "test".into(),
                row_data: vec![1, 2, 3],
            },
            ColumnarWalRecord::DeleteRows {
                collection: "test".into(),
                segment_id: 0,
                row_indices: vec![5, 10, 15],
            },
            ColumnarWalRecord::CompactionCommit {
                collection: "test".into(),
                old_segment_ids: vec![0, 1],
                new_segment_ids: vec![2],
            },
            ColumnarWalRecord::MemtableFlushed {
                collection: "test".into(),
                segment_id: 3,
                row_count: 1024,
            },
        ];

        for record in &records {
            let bytes = record.to_bytes().expect("serialize");
            let restored = ColumnarWalRecord::from_bytes(&bytes).expect("deserialize");
            assert_eq!(restored.collection(), record.collection());
        }
    }

    #[test]
    fn row_wire_format_roundtrip() {
        let values = vec![
            Value::Integer(42),
            Value::Float(0.75),
            Value::Bool(true),
            Value::String("hello".into()),
            Value::Bytes(vec![0xDE, 0xAD]),
            Value::DateTime(NdbDateTime::from_micros(1_700_000_000)),
            Value::Decimal(rust_decimal::Decimal::new(314, 2)),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::Null,
            Value::Array(vec![Value::Float(1.0), Value::Float(2.0)]),
        ];

        let encoded = encode_row_for_wal(&values);
        let decoded = decode_row_from_wal(&encoded).expect("decode");

        assert_eq!(decoded.len(), values.len());
        assert_eq!(decoded[0], Value::Integer(42));
        assert_eq!(decoded[1], Value::Float(0.75));
        assert_eq!(decoded[2], Value::Bool(true));
        assert_eq!(decoded[3], Value::String("hello".into()));
        assert_eq!(decoded[4], Value::Bytes(vec![0xDE, 0xAD]));
        assert_eq!(
            decoded[5],
            Value::DateTime(NdbDateTime::from_micros(1_700_000_000))
        );
        assert_eq!(
            decoded[7],
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into())
        );
        assert_eq!(decoded[8], Value::Null);
    }
}
