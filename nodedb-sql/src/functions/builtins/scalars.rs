//! Scalar function registrations, split by domain.

mod datetime;
mod doc;
mod math;
mod misc;
mod ndarray;
mod pg_fts;
mod pg_json;
mod spatial;
mod string;
mod vector;

use crate::functions::registry::FunctionMeta;

pub(super) fn scalar_functions() -> Vec<FunctionMeta> {
    let mut fns = Vec::new();
    fns.extend(vector::vector_functions());
    fns.extend(spatial::spatial_functions());
    fns.extend(datetime::datetime_functions());
    fns.extend(doc::doc_functions());
    fns.extend(string::string_functions());
    fns.extend(math::math_functions());
    fns.extend(pg_json::pg_json_functions());
    fns.extend(pg_fts::pg_fts_functions());
    fns.extend(ndarray::ndarray_functions());
    fns.extend(misc::misc_functions());
    fns
}
