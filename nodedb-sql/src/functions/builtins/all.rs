//! Top-level assembler for built-in function registrations.

use super::super::registry::FunctionMeta;
use super::{aggregates, scalars};

pub(crate) fn builtin_functions() -> Vec<FunctionMeta> {
    let mut fns = Vec::new();
    fns.extend(aggregates::aggregate_functions());
    fns.extend(scalars::scalar_functions());
    fns
}
