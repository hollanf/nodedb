//! `SqlPlan::NdArray*` → `PhysicalTask` lowering for the read/maint
//! function surface (NDARRAY_SLICE, NDARRAY_AGG, NDARRAY_PROJECT,
//! NDARRAY_ELEMENTWISE, NDARRAY_FLUSH, NDARRAY_COMPACT).

pub mod aggregate;
pub mod elementwise;
pub mod helpers;
pub mod maint;
pub mod project;
pub mod slice;

pub(super) use aggregate::convert_agg;
pub(super) use elementwise::convert_elementwise;
pub(super) use maint::{convert_compact, convert_flush};
pub(super) use project::convert_project;
pub(super) use slice::convert_slice;
