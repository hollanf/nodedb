//! Vector-primary collection configuration types.
//!
//! `PrimaryEngine` is a parallel attribute to `CollectionType` that tells the
//! planner which engine is the primary access path for a collection.
//! Vectors remain an index — not a collection type — but a `primary = 'vector'`
//! attribute means the vector index is the hot path and the document store is
//! a metadata sidecar.

use crate::collection::CollectionType;
use crate::columnar::{ColumnarProfile, DocumentMode};
use crate::vector_ann::VectorQuantization;
use crate::vector_distance::DistanceMetric;

/// Which engine serves as the primary access path for a collection.
///
/// This is independent of `CollectionType` — it is an optimizer hint that
/// instructs the planner and executor to use the named engine as the hot path.
/// The default is inferred from `CollectionType` so existing collections need
/// no migration.
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[non_exhaustive]
pub enum PrimaryEngine {
    /// Schemaless document (MessagePack). The historic default.
    #[default]
    Document = 0,
    /// Strict document (Binary Tuples).
    Strict = 1,
    /// Key-Value hash store.
    KeyValue = 2,
    /// Columnar / plain-analytics.
    Columnar = 3,
    /// Columnar with spatial profile.
    Spatial = 4,
    /// Vector-primary: HNSW is the hot path; document store is a metadata sidecar.
    Vector = 10,
}

impl PrimaryEngine {
    /// Infer the primary engine from a `CollectionType`.
    ///
    /// Used when reading catalog entries that predate the `primary` field —
    /// guarantees that existing collections behave as before.
    pub fn infer_from_collection_type(ct: &CollectionType) -> Self {
        match ct {
            CollectionType::Document(DocumentMode::Schemaless) => Self::Document,
            CollectionType::Document(DocumentMode::Strict(_)) => Self::Strict,
            CollectionType::Columnar(ColumnarProfile::Plain) => Self::Columnar,
            CollectionType::Columnar(ColumnarProfile::Timeseries { .. }) => Self::Columnar,
            CollectionType::Columnar(ColumnarProfile::Spatial { .. }) => Self::Spatial,
            CollectionType::KeyValue(_) => Self::KeyValue,
        }
    }
}

/// Configuration for a vector-primary collection.
///
/// Stored in `StoredCollection::vector_primary` when `primary == PrimaryEngine::Vector`.
/// All options correspond to HNSW construction parameters and codec selection for the
/// primary vector index.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VectorPrimaryConfig {
    /// The name of the column that holds vector data (must be of type VECTOR(n)).
    pub vector_field: String,
    /// Vector dimensionality.
    pub dim: u32,
    /// Quantization codec for the primary HNSW index.
    pub quantization: VectorQuantization,
    /// HNSW `M` parameter (number of connections per node).
    pub m: u8,
    /// HNSW `ef_construction` parameter (beam width during index construction).
    pub ef_construction: u16,
    /// Distance metric used for similarity search.
    pub metric: DistanceMetric,
    /// Payload field names that receive in-memory bitmap indexes for fast
    /// pre-filtering, paired with the storage kind (Equality / Range /
    /// Boolean). The DDL handler infers the kind from the column type:
    /// numeric / timestamp / decimal → Range; bool → Boolean; everything
    /// else → Equality.
    pub payload_indexes: Vec<(String, PayloadIndexKind)>,
}

impl Default for VectorPrimaryConfig {
    fn default() -> Self {
        Self {
            vector_field: String::new(),
            dim: 0,
            quantization: VectorQuantization::default(),
            m: 16,
            ef_construction: 200,
            metric: DistanceMetric::Cosine,
            payload_indexes: Vec::new(),
        }
    }
}

/// Storage kind for a payload bitmap index. Equality fields use a
/// `HashMap<key, bitmap>` (O(1) lookup); Range fields use a `BTreeMap`
/// for sorted range scans; Boolean is a low-cardinality equality variant.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[non_exhaustive]
pub enum PayloadIndexKind {
    #[default]
    Equality,
    Range,
    Boolean,
}

/// A single payload-bitmap predicate atom emitted by the SQL planner and
/// consumed by the vector search handler. The handler ANDs all atoms in
/// `VectorOp::Search::payload_filters`; each atom may itself be a
/// disjunction (`In`).
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[non_exhaustive]
pub enum PayloadAtom {
    /// `field = value` — single equality bitmap lookup.
    Eq(String, crate::Value),
    /// `field IN (v1, v2, ...)` — union of per-value bitmaps.
    In(String, Vec<crate::Value>),
    /// `field >= low AND field <= high` — sorted range scan over a
    /// `PayloadIndexKind::Range` index. Either bound being `None` means
    /// open on that side.
    Range {
        field: String,
        low: Option<crate::Value>,
        low_inclusive: bool,
        high: Option<crate::Value>,
        high_inclusive: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_engine_default_is_document() {
        assert_eq!(PrimaryEngine::default(), PrimaryEngine::Document);
    }

    #[test]
    fn infer_from_collection_type_document_schemaless() {
        let ct = CollectionType::document();
        assert_eq!(
            PrimaryEngine::infer_from_collection_type(&ct),
            PrimaryEngine::Document
        );
    }

    #[test]
    fn infer_from_collection_type_document_strict() {
        use crate::columnar::{ColumnDef, ColumnType, StrictSchema};
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ])
        .unwrap();
        let ct = CollectionType::strict(schema);
        assert_eq!(
            PrimaryEngine::infer_from_collection_type(&ct),
            PrimaryEngine::Strict
        );
    }

    #[test]
    fn infer_from_collection_type_columnar_plain() {
        let ct = CollectionType::columnar();
        assert_eq!(
            PrimaryEngine::infer_from_collection_type(&ct),
            PrimaryEngine::Columnar
        );
    }

    #[test]
    fn infer_from_collection_type_columnar_timeseries() {
        let ct = CollectionType::timeseries("ts", "1h");
        assert_eq!(
            PrimaryEngine::infer_from_collection_type(&ct),
            PrimaryEngine::Columnar
        );
    }

    #[test]
    fn infer_from_collection_type_columnar_spatial() {
        let ct = CollectionType::spatial("geom");
        assert_eq!(
            PrimaryEngine::infer_from_collection_type(&ct),
            PrimaryEngine::Spatial
        );
    }

    #[test]
    fn infer_from_collection_type_kv() {
        use crate::columnar::{ColumnDef, ColumnType, StrictSchema};
        let schema = StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
        ])
        .unwrap();
        let ct = CollectionType::kv(schema);
        assert_eq!(
            PrimaryEngine::infer_from_collection_type(&ct),
            PrimaryEngine::KeyValue
        );
    }

    #[test]
    fn primary_engine_serde_roundtrip() {
        for variant in [
            PrimaryEngine::Document,
            PrimaryEngine::Strict,
            PrimaryEngine::KeyValue,
            PrimaryEngine::Columnar,
            PrimaryEngine::Spatial,
            PrimaryEngine::Vector,
        ] {
            let json = sonic_rs::to_string(&variant).unwrap();
            let back: PrimaryEngine = sonic_rs::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn primary_engine_msgpack_roundtrip() {
        for variant in [
            PrimaryEngine::Document,
            PrimaryEngine::Strict,
            PrimaryEngine::KeyValue,
            PrimaryEngine::Columnar,
            PrimaryEngine::Spatial,
            PrimaryEngine::Vector,
        ] {
            let bytes = zerompk::to_msgpack_vec(&variant).unwrap();
            let back: PrimaryEngine = zerompk::from_msgpack(&bytes).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn vector_primary_config_serde_roundtrip() {
        let cfg = VectorPrimaryConfig {
            vector_field: "embedding".to_string(),
            dim: 1024,
            quantization: VectorQuantization::RaBitQ,
            m: 32,
            ef_construction: 200,
            metric: DistanceMetric::Cosine,
            payload_indexes: vec![
                ("category".to_string(), PayloadIndexKind::Equality),
                ("timestamp".to_string(), PayloadIndexKind::Range),
            ],
        };
        let json = sonic_rs::to_string(&cfg).unwrap();
        let back: VectorPrimaryConfig = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, cfg);
    }

    #[test]
    fn vector_primary_config_msgpack_roundtrip() {
        let cfg = VectorPrimaryConfig {
            vector_field: "vec".to_string(),
            dim: 512,
            quantization: VectorQuantization::Bbq,
            m: 16,
            ef_construction: 100,
            metric: DistanceMetric::L2,
            payload_indexes: vec![],
        };
        let bytes = zerompk::to_msgpack_vec(&cfg).unwrap();
        let back: VectorPrimaryConfig = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, cfg);
    }
}
