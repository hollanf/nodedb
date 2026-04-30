//! String scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn string_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "lower",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        m(
            "upper",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        m(
            "length",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::LENGTH_ARGS,
        ),
        m(
            "trim",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        m(
            "substring",
            Scalar,
            2,
            3,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::SUBSTRING_ARGS,
        ),
        m(
            "concat",
            Scalar,
            1,
            255,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::CONCAT_ARGS,
        ),
        m(
            "replace",
            Scalar,
            3,
            3,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::REPLACE_ARGS,
        ),
    ]
}
