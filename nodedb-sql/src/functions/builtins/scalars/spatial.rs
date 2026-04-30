//! Spatial scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta, SearchTrigger};

use super::super::helpers::{m, no_trigger};

pub(super) fn spatial_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "st_dwithin",
            Scalar,
            3,
            3,
            SearchTrigger::SpatialDWithin,
            Some(ColumnType::Bool),
            arg_types::SPATIAL_3_ARGS,
        ),
        m(
            "st_contains",
            Scalar,
            2,
            2,
            SearchTrigger::SpatialContains,
            Some(ColumnType::Bool),
            arg_types::SPATIAL_2_ARGS,
        ),
        m(
            "st_intersects",
            Scalar,
            2,
            2,
            SearchTrigger::SpatialIntersects,
            Some(ColumnType::Bool),
            arg_types::SPATIAL_2_ARGS,
        ),
        m(
            "st_within",
            Scalar,
            2,
            2,
            SearchTrigger::SpatialWithin,
            Some(ColumnType::Bool),
            arg_types::SPATIAL_2_ARGS,
        ),
        m(
            "st_distance",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Float64),
            arg_types::SPATIAL_2_ARGS,
        ),
        m(
            "st_point",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Geometry),
            arg_types::ST_POINT_ARGS,
        ),
    ]
}
