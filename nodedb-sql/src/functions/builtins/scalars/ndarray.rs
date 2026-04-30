//! Ndarray engine scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta, SearchTrigger};

use super::super::helpers::m;

pub(super) fn ndarray_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "ndarray_slice",
            Scalar,
            2,
            4,
            SearchTrigger::NdArraySlice,
            None,
            arg_types::NDARRAY_SLICE_ARGS,
        ),
        m(
            "ndarray_project",
            Scalar,
            2,
            2,
            SearchTrigger::NdArrayProject,
            None,
            arg_types::NDARRAY_PROJECT_ARGS,
        ),
        m(
            "ndarray_agg",
            Scalar,
            3,
            4,
            SearchTrigger::NdArrayAgg,
            None,
            arg_types::NDARRAY_AGG_ARGS,
        ),
        m(
            "ndarray_elementwise",
            Scalar,
            4,
            4,
            SearchTrigger::NdArrayElementwise,
            None,
            arg_types::NDARRAY_ELEMENTWISE_ARGS,
        ),
        m(
            "ndarray_flush",
            Scalar,
            1,
            1,
            SearchTrigger::NdArrayFlush,
            Some(ColumnType::Bool),
            arg_types::NDARRAY_MAINT_ARGS,
        ),
        m(
            "ndarray_compact",
            Scalar,
            1,
            1,
            SearchTrigger::NdArrayCompact,
            Some(ColumnType::Bool),
            arg_types::NDARRAY_MAINT_ARGS,
        ),
    ]
}
