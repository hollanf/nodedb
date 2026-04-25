//! NDARRAY_PROJECT → PhysicalPlan::Array(ArrayOp::Project).

use nodedb_array::types::ArrayId;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ArrayOp;
use crate::types::{TenantId, VShardId};

use super::super::super::physical::{PhysicalTask, PostSetOp};
use super::super::convert::ConvertContext;
use super::helpers::{load_schema, resolve_attr_indices};

pub(crate) fn convert_project(
    name: &str,
    attr_projection: &[String],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let schema = load_schema(name, ctx)?;
    let attr_indices = resolve_attr_indices(name, attr_projection, &schema)?;
    if attr_indices.is_empty() {
        return Err(crate::Error::PlanError {
            detail: format!("NDARRAY_PROJECT: array '{name}': attr list must not be empty"),
        });
    }
    let aid = ArrayId::new(tenant_id, name);
    let vshard = VShardId::from_collection(name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Project {
            array_id: aid,
            attr_indices,
        }),
        post_set_op: PostSetOp::None,
    }])
}
