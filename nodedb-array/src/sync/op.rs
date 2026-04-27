//! Array CRDT operation types.
//!
//! Each mutation to the array engine produces one [`ArrayOp`]. Operations
//! are the atomic unit of replication: they flow from the originating replica
//! to Origin and fan out to all subscribed peers.

use serde::{Deserialize, Serialize};

use crate::error::{ArrayError, ArrayResult};
use crate::sync::hlc::Hlc;
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

/// The kind of mutation an [`ArrayOp`] represents.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ArrayOpKind {
    /// Insert or update a cell. `ArrayOp::attrs` must be `Some`.
    Put,
    /// Logical delete of a cell (soft tombstone). `ArrayOp::attrs` must be `None`.
    Delete,
    /// GDPR-grade erase of a cell (hard tombstone). `ArrayOp::attrs` must be `None`.
    Erase,
}

/// Metadata carried by every array operation.
///
/// `system_from_ms` is redundant with `hlc.physical_ms` and is included
/// separately for fast bitemporal index lookups that do not need to unpack
/// the full HLC.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayOpHeader {
    /// Name of the target array collection.
    pub array: String,
    /// HLC timestamp of this operation at the originating replica.
    pub hlc: Hlc,
    /// HLC of the array schema that was in effect when this op was generated.
    ///
    /// Receivers gate application on `schema_hlc <= local_schema_hlc`.
    pub schema_hlc: Hlc,
    /// Valid-time start (milliseconds since Unix epoch). `-1` means "open".
    pub valid_from_ms: i64,
    /// Valid-time end (milliseconds since Unix epoch). `-1` means "open".
    pub valid_until_ms: i64,
    /// System-time start; mirrors `hlc.physical_ms` for fast bitemporal indexing.
    pub system_from_ms: i64,
}

/// A single array CRDT operation.
///
/// Shape contract:
/// - [`ArrayOpKind::Put`] — `attrs` must be `Some(_)`.
/// - [`ArrayOpKind::Delete`] / [`ArrayOpKind::Erase`] — `attrs` must be `None`.
///
/// Use [`ArrayOp::validate_shape`] to enforce this invariant after construction.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayOp {
    /// Operation metadata (HLC, schema version, bitemporal bounds).
    pub header: ArrayOpHeader,
    /// Kind of mutation.
    pub kind: ArrayOpKind,
    /// N-dimensional coordinate identifying the target cell.
    pub coord: Vec<CoordValue>,
    /// Cell attribute values for a `Put`; `None` for `Delete` and `Erase`.
    pub attrs: Option<Vec<CellValue>>,
}

impl ArrayOp {
    /// Validate that `attrs` presence matches the operation kind.
    ///
    /// Returns [`ArrayError::InvalidOp`] if the shape contract is violated.
    pub fn validate_shape(&self) -> ArrayResult<()> {
        match self.kind {
            ArrayOpKind::Put => {
                if self.attrs.is_none() {
                    return Err(ArrayError::InvalidOp {
                        detail: "Put op must carry attrs".into(),
                    });
                }
            }
            ArrayOpKind::Delete | ArrayOpKind::Erase => {
                if self.attrs.is_some() {
                    return Err(ArrayError::InvalidOp {
                        detail: format!("{:?} op must not carry attrs", self.kind),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::replica_id::ReplicaId;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;

    fn dummy_hlc() -> Hlc {
        Hlc::new(1_000, 0, ReplicaId::new(1)).unwrap()
    }

    fn dummy_header(array: &str) -> ArrayOpHeader {
        ArrayOpHeader {
            array: array.into(),
            hlc: dummy_hlc(),
            schema_hlc: dummy_hlc(),
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: 1_000,
        }
    }

    fn dummy_coord() -> Vec<CoordValue> {
        vec![CoordValue::Int64(0)]
    }

    fn dummy_attrs() -> Option<Vec<CellValue>> {
        Some(vec![CellValue::Null])
    }

    #[test]
    fn put_requires_attrs() {
        let op = ArrayOp {
            header: dummy_header("t"),
            kind: ArrayOpKind::Put,
            coord: dummy_coord(),
            attrs: None,
        };
        assert!(matches!(
            op.validate_shape(),
            Err(ArrayError::InvalidOp { .. })
        ));
    }

    #[test]
    fn delete_rejects_attrs() {
        let op = ArrayOp {
            header: dummy_header("t"),
            kind: ArrayOpKind::Delete,
            coord: dummy_coord(),
            attrs: dummy_attrs(),
        };
        assert!(matches!(
            op.validate_shape(),
            Err(ArrayError::InvalidOp { .. })
        ));
    }

    #[test]
    fn erase_rejects_attrs() {
        let op = ArrayOp {
            header: dummy_header("t"),
            kind: ArrayOpKind::Erase,
            coord: dummy_coord(),
            attrs: dummy_attrs(),
        };
        assert!(matches!(
            op.validate_shape(),
            Err(ArrayError::InvalidOp { .. })
        ));
    }

    #[test]
    fn valid_put_passes() {
        let op = ArrayOp {
            header: dummy_header("t"),
            kind: ArrayOpKind::Put,
            coord: dummy_coord(),
            attrs: dummy_attrs(),
        };
        assert!(op.validate_shape().is_ok());
    }

    #[test]
    fn valid_delete_passes() {
        let op = ArrayOp {
            header: dummy_header("t"),
            kind: ArrayOpKind::Delete,
            coord: dummy_coord(),
            attrs: None,
        };
        assert!(op.validate_shape().is_ok());
    }

    #[test]
    fn serialize_roundtrip() {
        let op = ArrayOp {
            header: dummy_header("test_array"),
            kind: ArrayOpKind::Put,
            coord: dummy_coord(),
            attrs: dummy_attrs(),
        };
        let bytes = zerompk::to_msgpack_vec(&op).expect("serialize");
        let back: ArrayOp = zerompk::from_msgpack(&bytes).expect("deserialize");
        assert_eq!(op, back);
    }
}
