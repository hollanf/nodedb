//! Sparse tile payload — coordinate list + per-attribute columns.
//!
//! Storage layout is column-major so per-attribute codecs
//! (`nodedb-codec`) can be applied without a transpose. Dimension values are
//! dictionary-encoded inline: rare in genomic / single-cell workloads
//! that have many repeats per tile.

use nodedb_types::{OPEN_UPPER, Surrogate};
use serde::{Deserialize, Serialize};

use super::mbr::{MbrBuilder, TileMBR};
use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

/// Per-row lifecycle classification for a [`SparseTile`] row.
///
/// Stored as a parallel `u8` column (`row_kinds`) so the segment can
/// carry tombstone and GDPR-erasure markers that survive flush and are
/// visible to post-flush reads. Compaction's retention policy is the
/// only path allowed to drop these rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Live = 0,
    Tombstone = 1,
    GdprErased = 2,
}

impl RowKind {
    /// Convert a raw byte to a `RowKind`. Fails on unknown values so
    /// corrupt data surfaces as a typed error rather than silent `Live`.
    pub fn from_u8(b: u8) -> ArrayResult<Self> {
        match b {
            0 => Ok(Self::Live),
            1 => Ok(Self::Tombstone),
            2 => Ok(Self::GdprErased),
            other => Err(ArrayError::SegmentCorruption {
                detail: format!("unknown RowKind byte: {other:#04x}"),
            }),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl From<RowKind> for u8 {
    fn from(k: RowKind) -> u8 {
        k.as_u8()
    }
}

/// Per-dim dictionary: distinct values seen, and one index per cell
/// pointing into the dictionary. Index width is selected by callers
/// when picking the codec.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DimDict {
    pub values: Vec<CoordValue>,
    pub indices: Vec<u32>,
}

impl DimDict {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            indices: Vec::new(),
        }
    }

    fn push(&mut self, v: &CoordValue) {
        if let Some(idx) = self.values.iter().position(|x| x == v) {
            self.indices.push(idx as u32);
        } else {
            self.indices.push(self.values.len() as u32);
            self.values.push(v.clone());
        }
    }

    pub fn cardinality(&self) -> usize {
        self.values.len()
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

/// Sparse tile payload.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SparseTile {
    /// One dictionary per schema dim, parallel to `schema.dims`.
    pub dim_dicts: Vec<DimDict>,
    /// One column per schema attr, parallel to `schema.attrs`.
    pub attr_cols: Vec<Vec<CellValue>>,
    /// Per-cell global surrogate (one entry per row, parallel to the
    /// dim-dict index streams and `attr_cols`). Cross-engine bitmap
    /// joins read this column directly without translating coords back
    /// to user-visible primary keys.
    pub surrogates: Vec<Surrogate>,
    /// Per-cell valid-time lower bound in milliseconds (inclusive).
    /// Parallel to `surrogates`.
    pub valid_from_ms: Vec<i64>,
    /// Per-cell valid-time upper bound in milliseconds (exclusive).
    /// [`nodedb_types::OPEN_UPPER`] (`i64::MAX`) means open-ended.
    /// Parallel to `surrogates`.
    pub valid_until_ms: Vec<i64>,
    /// Per-row [`RowKind`] encoded as `u8`. Parallel to `surrogates`.
    /// `0` = Live, `1` = Tombstone, `2` = GdprErased.
    ///
    /// Older segments written before this column existed will deserialise
    /// with an empty `Vec`; readers treat a missing entry as `Live`.
    pub row_kinds: Vec<u8>,
    pub mbr: TileMBR,
}

impl SparseTile {
    /// Empty tile sized for the given schema.
    pub fn empty(schema: &ArraySchema) -> Self {
        Self {
            dim_dicts: (0..schema.arity()).map(|_| DimDict::new()).collect(),
            attr_cols: (0..schema.attrs.len()).map(|_| Vec::new()).collect(),
            surrogates: Vec::new(),
            valid_from_ms: Vec::new(),
            valid_until_ms: Vec::new(),
            row_kinds: Vec::new(),
            mbr: TileMBR::new(schema.arity(), schema.attrs.len()),
        }
    }

    /// Count of live (non-sentinel) rows. Used for MBR statistics and
    /// predicate-pushdown decisions. Sentinel rows are excluded.
    ///
    /// **Do not use this value as a row-iteration bound.** Iteration must
    /// cover sentinel rows so callers can observe Tombstone / GdprErased
    /// markers when applying bitemporal visibility rules — use
    /// [`row_count`](Self::row_count) and filter with
    /// [`row_kind`](Self::row_kind) instead.
    pub fn nnz(&self) -> u32 {
        self.mbr.nnz
    }

    /// Total physical row count including sentinel (Tombstone / GdprErased)
    /// rows. Use this as the upper bound when iterating over rows; check
    /// [`row_kind`](Self::row_kind) inside the loop and skip non-Live rows
    /// when reading attribute columns. The attribute column vectors are
    /// indexed by the **live-row index** (incremented only for Live rows),
    /// not by the iteration index — confusing the two yields incorrect or
    /// out-of-bounds reads.
    pub fn row_count(&self) -> usize {
        self.surrogates.len()
    }

    /// Decode the `RowKind` for the given row index.
    ///
    /// Returns [`RowKind::Live`] when `row_kinds` is empty (forward-compat
    /// with pre-v4 data still in memory during migration) or when the row
    /// index is out of range.
    pub fn row_kind(&self, row: usize) -> ArrayResult<RowKind> {
        match self.row_kinds.get(row) {
            None => Ok(RowKind::Live),
            Some(&b) => RowKind::from_u8(b),
        }
    }
}

/// All row-level data passed to [`SparseTileBuilder::push_row`].
pub struct SparseRow<'a> {
    pub coord: &'a [CoordValue],
    pub attrs: &'a [CellValue],
    pub surrogate: Surrogate,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub kind: RowKind,
}

impl<'a> SparseRow<'a> {
    /// Construct a live row. Convenience wrapper so callers that only write
    /// live data don't have to spell out `kind: RowKind::Live` explicitly.
    pub fn live(
        coord: &'a [CoordValue],
        attrs: &'a [CellValue],
        surrogate: Surrogate,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> Self {
        Self {
            coord,
            attrs,
            surrogate,
            valid_from_ms,
            valid_until_ms,
            kind: RowKind::Live,
        }
    }
}

/// Streaming builder. Folds `(coord, attrs)` pairs into the tile body
/// and the MBR in one pass.
pub struct SparseTileBuilder<'a> {
    schema: &'a ArraySchema,
    dim_dicts: Vec<DimDict>,
    attr_cols: Vec<Vec<CellValue>>,
    surrogates: Vec<Surrogate>,
    valid_from_ms: Vec<i64>,
    valid_until_ms: Vec<i64>,
    row_kinds: Vec<u8>,
    mbr: MbrBuilder,
}

impl<'a> SparseTileBuilder<'a> {
    pub fn new(schema: &'a ArraySchema) -> Self {
        Self {
            schema,
            dim_dicts: (0..schema.arity()).map(|_| DimDict::new()).collect(),
            attr_cols: (0..schema.attrs.len()).map(|_| Vec::new()).collect(),
            surrogates: Vec::new(),
            valid_from_ms: Vec::new(),
            valid_until_ms: Vec::new(),
            row_kinds: Vec::new(),
            mbr: MbrBuilder::new(schema.arity(), schema.attrs.len()),
        }
    }

    /// Push a row with full bitemporal metadata.
    ///
    /// Sentinel rows (`Tombstone`, `GdprErased`) may have an empty `attrs`
    /// slice — the attribute columns receive no entry for these rows, which
    /// is correct because sentinel rows carry no payload.
    pub fn push_row(&mut self, row: SparseRow<'_>) -> ArrayResult<()> {
        let SparseRow {
            coord,
            attrs,
            surrogate,
            valid_from_ms,
            valid_until_ms,
            kind,
        } = row;
        if coord.len() != self.schema.arity() {
            return Err(ArrayError::CoordArityMismatch {
                array: self.schema.name.clone(),
                expected: self.schema.arity(),
                got: coord.len(),
            });
        }
        // Sentinel rows carry no payload — allow empty attrs. Live rows must
        // match the schema attr count exactly.
        if kind == RowKind::Live && attrs.len() != self.schema.attrs.len() {
            return Err(ArrayError::CellTypeMismatch {
                array: self.schema.name.clone(),
                attr: "<all>".to_string(),
                detail: format!(
                    "expected {} attr columns, got {}",
                    self.schema.attrs.len(),
                    attrs.len()
                ),
            });
        }
        for (i, c) in coord.iter().enumerate() {
            self.dim_dicts[i].push(c);
        }
        // For sentinel rows `attrs` is empty — no entries added to attr_cols.
        // For live rows, every attr col gets one entry.
        for (i, a) in attrs.iter().enumerate() {
            self.attr_cols[i].push(a.clone());
        }
        self.surrogates.push(surrogate);
        self.valid_from_ms.push(valid_from_ms);
        self.valid_until_ms.push(valid_until_ms);
        self.row_kinds.push(kind.as_u8());
        // Only fold MBR for live rows; sentinel rows have no meaningful attrs
        // and may have coords outside the declared domain.
        if kind == RowKind::Live {
            self.mbr.fold(coord, attrs);
        }
        Ok(())
    }

    /// Push a live row whose surrogate is unknown to the caller (recovery,
    /// pure-shape ops). The slot is filled with [`Surrogate::ZERO`];
    /// callers that have a real surrogate should use [`Self::push_row`].
    pub fn push(&mut self, coord: &[CoordValue], attrs: &[CellValue]) -> ArrayResult<()> {
        self.push_row(SparseRow {
            coord,
            attrs,
            surrogate: Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
    }

    pub fn build(self) -> SparseTile {
        SparseTile {
            dim_dicts: self.dim_dicts,
            attr_cols: self.attr_cols,
            surrogates: self.surrogates,
            valid_from_ms: self.valid_from_ms,
            valid_until_ms: self.valid_until_ms,
            row_kinds: self.row_kinds,
            mbr: self.mbr.build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "chrom",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(24)),
            ))
            .dim(DimSpec::new(
                "pos",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000)),
            ))
            .attr(AttrSpec::new("variant", AttrType::String, false))
            .attr(AttrSpec::new("qual", AttrType::Float64, true))
            .tile_extents(vec![1, 1_000_000])
            .build()
            .unwrap()
    }

    #[test]
    fn sparse_tile_collects_columns_in_order() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(100)],
            &[CellValue::String("ALT".into()), CellValue::Float64(40.0)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(200)],
            &[CellValue::String("REF".into()), CellValue::Float64(50.0)],
        )
        .unwrap();
        let t = b.build();
        assert_eq!(t.nnz(), 2);
        assert_eq!(t.attr_cols.len(), 2);
        assert_eq!(t.attr_cols[0].len(), 2);
        assert_eq!(t.attr_cols[1].len(), 2);
    }

    #[test]
    fn dim_dict_dedups_repeated_values() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(100)],
            &[CellValue::String("ALT".into()), CellValue::Float64(40.0)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(200)],
            &[CellValue::String("REF".into()), CellValue::Float64(50.0)],
        )
        .unwrap();
        let t = b.build();
        // chrom dictionary should have one entry (1 repeated).
        assert_eq!(t.dim_dicts[0].cardinality(), 1);
        assert_eq!(t.dim_dicts[0].len(), 2);
    }

    #[test]
    fn sparse_tile_rejects_bad_arity() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let r = b.push(
            &[CoordValue::Int64(1)],
            &[CellValue::String("ALT".into()), CellValue::Float64(0.0)],
        );
        assert!(r.is_err());
    }

    #[test]
    fn sparse_tile_rejects_bad_attr_count() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let r = b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(0)],
            &[CellValue::String("ALT".into())],
        );
        assert!(r.is_err());
    }

    #[test]
    fn empty_sparse_tile_has_zero_nnz() {
        let s = schema();
        let t = SparseTile::empty(&s);
        assert_eq!(t.nnz(), 0);
        assert_eq!(t.dim_dicts.len(), 2);
        assert_eq!(t.attr_cols.len(), 2);
    }

    #[test]
    fn sparse_tile_msgpack_roundtrip_carries_valid_time_bounds() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(1), CoordValue::Int64(100)],
            attrs: &[CellValue::String("A".into()), CellValue::Float64(1.0)],
            surrogate: nodedb_types::Surrogate::ZERO,
            valid_from_ms: 50,
            valid_until_ms: 150,
            kind: RowKind::Live,
        })
        .unwrap();
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(2), CoordValue::Int64(200)],
            attrs: &[CellValue::String("B".into()), CellValue::Float64(2.0)],
            surrogate: nodedb_types::Surrogate::ZERO,
            valid_from_ms: 300,
            valid_until_ms: 900,
            kind: RowKind::Live,
        })
        .unwrap();
        let tile = b.build();

        let bytes = zerompk::to_msgpack_vec(&tile).unwrap();
        let decoded: SparseTile = zerompk::from_msgpack(&bytes).unwrap();

        assert_eq!(decoded.valid_from_ms, vec![50, 300]);
        assert_eq!(decoded.valid_until_ms, vec![150, 900]);
        assert_eq!(decoded.nnz(), 2);
    }

    #[test]
    fn sparse_tile_msgpack_roundtrip_carries_row_kinds() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        // Live row
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(1), CoordValue::Int64(10)],
            attrs: &[CellValue::String("X".into()), CellValue::Float64(1.0)],
            surrogate: nodedb_types::Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
        // Tombstone row (no attrs)
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(2), CoordValue::Int64(20)],
            attrs: &[],
            surrogate: nodedb_types::Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Tombstone,
        })
        .unwrap();
        // GdprErased row (no attrs)
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(3), CoordValue::Int64(30)],
            attrs: &[],
            surrogate: nodedb_types::Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::GdprErased,
        })
        .unwrap();
        let tile = b.build();

        let bytes = zerompk::to_msgpack_vec(&tile).unwrap();
        let decoded: SparseTile = zerompk::from_msgpack(&bytes).unwrap();

        assert_eq!(decoded.row_kinds.len(), 3);
        assert_eq!(
            RowKind::from_u8(decoded.row_kinds[0]).unwrap(),
            RowKind::Live
        );
        assert_eq!(
            RowKind::from_u8(decoded.row_kinds[1]).unwrap(),
            RowKind::Tombstone
        );
        assert_eq!(
            RowKind::from_u8(decoded.row_kinds[2]).unwrap(),
            RowKind::GdprErased
        );
    }
}
