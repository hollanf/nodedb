//! NDARRAY_ELEMENTWISE → PhysicalPlan::Array(ArrayOp::Elementwise).

use nodedb_array::types::ArrayId;
use nodedb_sql::types_array::ArrayBinaryOpAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ArrayOp;
use crate::types::{TenantId, VShardId};

use super::super::super::physical::{PhysicalTask, PostSetOp};
use super::super::convert::ConvertContext;
use super::helpers::{load_schema, map_binary_op};

pub(crate) fn convert_elementwise(
    left_name: &str,
    right_name: &str,
    op: ArrayBinaryOpAst,
    attr: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let lschema = load_schema(left_name, ctx)?;
    let rschema = load_schema(right_name, ctx)?;
    if lschema.dims.len() != rschema.dims.len() || lschema.attrs.len() != rschema.attrs.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "NDARRAY_ELEMENTWISE: arrays '{left_name}' and '{right_name}' have different shapes"
            ),
        });
    }
    let attr_idx = lschema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("NDARRAY_ELEMENTWISE: array '{left_name}' has no attr '{attr}'"),
        })? as u32;
    if !rschema.attrs.iter().any(|a| a.name == attr) {
        return Err(crate::Error::PlanError {
            detail: format!("NDARRAY_ELEMENTWISE: array '{right_name}' has no attr '{attr}'"),
        });
    }
    let left = ArrayId::new(tenant_id, left_name);
    let right = ArrayId::new(tenant_id, right_name);
    let vshard = VShardId::from_collection(left_name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Array(ArrayOp::Elementwise {
            left,
            right,
            op: map_binary_op(op),
            attr_idx,
            cell_filter: None,
        }),
        post_set_op: PostSetOp::None,
    }])
}
