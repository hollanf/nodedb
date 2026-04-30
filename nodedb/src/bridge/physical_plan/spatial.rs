//! Spatial engine operations dispatched to the Data Plane.

use nodedb_types::{SurrogateBitmap, geometry::Geometry};

/// Spatial predicate type for R-tree index scan.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum SpatialPredicate {
    /// ST_DWithin: geometry within distance (meters).
    DWithin,
    /// ST_Contains: query geometry contains document geometry.
    Contains,
    /// ST_Intersects: query geometry intersects document geometry.
    Intersects,
    /// ST_Within: document geometry is within query geometry.
    Within,
}

/// Spatial engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SpatialOp {
    /// R-tree index scan with spatial predicate and exact refinement.
    Scan {
        collection: String,
        field: String,
        predicate: SpatialPredicate,
        /// Typed query geometry, parsed and validated on the Control Plane.
        query_geometry: Geometry,
        /// Distance threshold in meters (for ST_DWithin). 0 for non-distance predicates.
        distance_meters: f64,
        /// Additional attribute filters applied after spatial candidates.
        attribute_filters: Vec<u8>,
        limit: usize,
        projection: Vec<String>,
        /// RLS post-candidate filters.
        rls_filters: Vec<u8>,
        /// Optional surrogate prefilter injected by a cross-engine sub-plan.
        /// When present, only candidates whose surrogate is in this bitmap
        /// are returned. `None` = no prefilter; all R-tree candidates pass.
        prefilter: Option<SurrogateBitmap>,
    },
}
