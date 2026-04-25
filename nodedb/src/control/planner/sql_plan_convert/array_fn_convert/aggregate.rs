//! NDARRAY_AGG → PhysicalPlan::Array(ArrayOp::Aggregate).

use nodedb_array::types::ArrayId;
use nodedb_sql::types_array::ArrayReducerAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ArrayOp;
use crate::types::{TenantId, VShardId};

use super::super::super::physical::{PhysicalTask, PostSetOp};
use super::super::convert::ConvertContext;
use super::helpers::{load_schema, map_reducer};

pub(crate) fn convert_agg(
    name: &str,
    attr: &str,
    reducer: ArrayReducerAst,
    group_by_dim: Option<&str>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let schema = load_schema(name, ctx)?;
    let attr_idx = schema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("NDARRAY_AGG: array '{name}' has no attr '{attr}'"),
        })? as u32;
    let group_by_dim_idx: i32 = match group_by_dim {
        None => -1,
        Some(dim) => schema
            .dims
            .iter()
            .position(|d| d.name == dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("NDARRAY_AGG: array '{name}' has no dim '{dim}'"),
            })? as i32,
    };
    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Aggregate {
            array_id: aid,
            attr_idx,
            reducer: map_reducer(reducer),
            group_by_dim: group_by_dim_idx,
            cell_filter: None,
        }),
        post_set_op: PostSetOp::None,
    }])
}
