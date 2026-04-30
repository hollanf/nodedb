//! Date/time scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta, SearchTrigger};

use super::super::helpers::{m, no_trigger};

pub(super) fn datetime_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "time_bucket",
            Scalar,
            2,
            2,
            SearchTrigger::TimeBucket,
            Some(ColumnType::Timestamp),
            arg_types::TIME_BUCKET_ARGS,
        ),
        m(
            "now",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Timestamptz),
            arg_types::NO_ARGS,
        ),
        m(
            "current_timestamp",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Timestamptz),
            arg_types::NO_ARGS,
        ),
    ]
}
