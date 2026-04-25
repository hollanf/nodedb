//! NDARRAY_SLICE → PhysicalPlan::Array(ArrayOp::Slice).

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::types::ArrayId;
use nodedb_sql::types_array::ArraySliceAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ArrayOp;
use crate::types::{TenantId, VShardId};

use super::super::super::physical::{PhysicalTask, PostSetOp};
use super::super::convert::ConvertContext;
use super::helpers::{coerce_bound, load_schema, resolve_attr_indices};

pub(crate) fn convert_slice(
    name: &str,
    slice_ast: &ArraySliceAst,
    attr_projection: &[String],
    limit: u32,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let schema = load_schema(name, ctx)?;

    let mut dim_ranges: Vec<Option<DimRange>> = vec![None; schema.dims.len()];
    for r in &slice_ast.dim_ranges {
        let idx = schema
            .dims
            .iter()
            .position(|d| d.name == r.dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("NDARRAY_SLICE: array '{name}' has no dim '{}'", r.dim),
            })?;
        let dtype = schema.dims[idx].dtype;
        let lo = coerce_bound(&r.lo, dtype, &r.dim)?;
        let hi = coerce_bound(&r.hi, dtype, &r.dim)?;
        dim_ranges[idx] = Some(DimRange::new(lo, hi));
    }
    let slice = Slice::new(dim_ranges);
    let slice_msgpack =
        zerompk::to_msgpack_vec(&slice).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array slice encode: {e}"),
        })?;

    let attr_indices = resolve_attr_indices(name, attr_projection, &schema)?;

    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Slice {
            array_id: aid,
            slice_msgpack,
            attr_projection: attr_indices,
            limit,
            cell_filter: None,
        }),
        post_set_op: PostSetOp::None,
    }])
}
