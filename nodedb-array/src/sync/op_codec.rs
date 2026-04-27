//! MessagePack encode/decode for [`ArrayOp`] and batches thereof.
//!
//! Uses `zerompk` (the workspace's MessagePack codec) so that encoding is
//! consistent with the rest of the internal transport layer.

use crate::error::{ArrayError, ArrayResult};
use crate::sync::op::ArrayOp;

/// Encode a single [`ArrayOp`] to MessagePack bytes.
pub fn encode_op(op: &ArrayOp) -> ArrayResult<Vec<u8>> {
    zerompk::to_msgpack_vec(op).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("encode_op: {e}"),
    })
}

/// Decode a single [`ArrayOp`] from MessagePack bytes.
pub fn decode_op(bytes: &[u8]) -> ArrayResult<ArrayOp> {
    zerompk::from_msgpack(bytes).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("decode_op: {e}"),
    })
}

/// Encode a slice of [`ArrayOp`]s as a single MessagePack value (array).
pub fn encode_op_batch(ops: &[ArrayOp]) -> ArrayResult<Vec<u8>> {
    zerompk::to_msgpack_vec(&ops.to_vec()).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("encode_op_batch: {e}"),
    })
}

/// Decode a `Vec<ArrayOp>` from MessagePack bytes produced by
/// [`encode_op_batch`].
pub fn decode_op_batch(bytes: &[u8]) -> ArrayResult<Vec<ArrayOp>> {
    zerompk::from_msgpack(bytes).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("decode_op_batch: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::op::{ArrayOpHeader, ArrayOpKind};
    use crate::sync::replica_id::ReplicaId;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
    }

    fn header(array: &str, ms: u64) -> ArrayOpHeader {
        ArrayOpHeader {
            array: array.into(),
            hlc: hlc(ms),
            schema_hlc: hlc(ms),
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        }
    }

    fn put_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: header(array, ms),
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(1)],
            attrs: Some(vec![CellValue::Null]),
        }
    }

    fn delete_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: header(array, ms),
            kind: ArrayOpKind::Delete,
            coord: vec![CoordValue::Int64(2)],
            attrs: None,
        }
    }

    fn erase_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: header(array, ms),
            kind: ArrayOpKind::Erase,
            coord: vec![CoordValue::Int64(3)],
            attrs: None,
        }
    }

    #[test]
    fn roundtrip_put() {
        let op = put_op("arr", 100);
        let bytes = encode_op(&op).unwrap();
        let back = decode_op(&bytes).unwrap();
        assert_eq!(op, back);
    }

    #[test]
    fn roundtrip_delete() {
        let op = delete_op("arr", 200);
        let bytes = encode_op(&op).unwrap();
        let back = decode_op(&bytes).unwrap();
        assert_eq!(op, back);
    }

    #[test]
    fn roundtrip_erase() {
        let op = erase_op("arr", 300);
        let bytes = encode_op(&op).unwrap();
        let back = decode_op(&bytes).unwrap();
        assert_eq!(op, back);
    }

    #[test]
    fn roundtrip_batch_of_three() {
        let ops = vec![put_op("a", 1), delete_op("b", 2), erase_op("c", 3)];
        let bytes = encode_op_batch(&ops).unwrap();
        let back = decode_op_batch(&bytes).unwrap();
        assert_eq!(ops, back);
    }

    #[test]
    fn decode_garbage_errors() {
        let garbage = b"this is not valid msgpack \xff\xfe";
        assert!(
            decode_op(garbage).is_err(),
            "decode_op should error on garbage input"
        );
        assert!(
            decode_op_batch(garbage).is_err(),
            "decode_op_batch should error on garbage input"
        );
    }
}
