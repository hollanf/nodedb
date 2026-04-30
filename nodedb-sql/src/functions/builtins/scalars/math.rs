//! Math scalar function registrations.

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn math_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "abs",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "ceil",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "floor",
            Scalar,
            1,
            1,
            no_trigger(),
            None,
            arg_types::MATH_1_ARGS,
        ),
        m(
            "round",
            Scalar,
            1,
            2,
            no_trigger(),
            None,
            arg_types::ROUND_ARGS,
        ),
    ]
}
