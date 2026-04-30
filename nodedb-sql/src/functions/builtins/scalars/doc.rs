//! Document helper scalar function registrations.

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};
use nodedb_types::columnar::ColumnType;

use super::super::helpers::{m, no_trigger};

pub(super) fn doc_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "doc_get",
            Scalar,
            2,
            3,
            no_trigger(),
            None,
            arg_types::DOC_GET_ARGS,
        ),
        m(
            "doc_exists",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::DOC_EXISTS_ARGS,
        ),
        m(
            "doc_array_contains",
            Scalar,
            3,
            3,
            no_trigger(),
            None,
            arg_types::DOC_ARRAY_CONTAINS_ARGS,
        ),
        m("nav", Scalar, 2, 2, no_trigger(), None, arg_types::NAV_ARGS),
    ]
}
