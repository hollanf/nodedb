//! PostgreSQL full-text search operator scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn pg_fts_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "pg_fts_match",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::PG_FTS_MATCH_ARGS,
        ),
        m(
            "pg_to_tsvector",
            Scalar,
            1,
            2,
            no_trigger(),
            None,
            arg_types::PG_TO_TSVECTOR_ARGS,
        ),
        m(
            "pg_to_tsquery",
            Scalar,
            1,
            2,
            no_trigger(),
            None,
            arg_types::PG_TO_TSQUERY_ARGS,
        ),
        m(
            "pg_plainto_tsquery",
            Scalar,
            1,
            2,
            no_trigger(),
            None,
            arg_types::PG_TO_TSQUERY_ARGS,
        ),
        m(
            "pg_phraseto_tsquery",
            Scalar,
            1,
            2,
            no_trigger(),
            None,
            arg_types::PG_TO_TSQUERY_ARGS,
        ),
        m(
            "pg_websearch_to_tsquery",
            Scalar,
            1,
            2,
            no_trigger(),
            None,
            arg_types::PG_TO_TSQUERY_ARGS,
        ),
        m(
            "pg_ts_rank",
            Scalar,
            2,
            4,
            no_trigger(),
            Some(ColumnType::Float64),
            arg_types::PG_TS_RANK_ARGS,
        ),
        m(
            "pg_ts_headline",
            Scalar,
            2,
            4,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_TS_HEADLINE_ARGS,
        ),
    ]
}
