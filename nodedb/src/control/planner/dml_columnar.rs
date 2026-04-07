//! DML plan conversion for plain and spatial columnar collections.
//!
//! Routes INSERT/UPDATE/DELETE to the appropriate `ColumnarOp` variants.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ColumnarOp;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::converter::PlanConverter;
use super::extract::{extract_insert_values, extract_update_assignments, extract_where_filters};

impl PlanConverter {
    /// Convert DML for a plain or spatial columnar collection.
    ///
    /// Routes INSERT → ColumnarOp::Insert, UPDATE → ColumnarOp::Update,
    /// DELETE → ColumnarOp::Delete.
    pub(super) fn convert_columnar_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        collection: &str,
        tenant_id: TenantId,
        vshard: VShardId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                let values =
                    extract_insert_values(&dml.input).map_err(|_| crate::Error::PlanError {
                        detail: "columnar INSERT requires VALUES clause".into(),
                    })?;

                if values.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "columnar INSERT requires at least one row".into(),
                    });
                }

                // Build msgpack array of Value objects for the columnar insert handler.
                let mut ndb_rows = Vec::with_capacity(values.len());
                for (_doc_id, value_bytes) in &values {
                    let row = nodedb_types::value_from_msgpack(value_bytes)
                        .unwrap_or(nodedb_types::Value::Null);
                    ndb_rows.push(row);
                }
                let payload = nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(ndb_rows))
                    .unwrap_or_default();

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                        collection: collection.to_string(),
                        payload,
                        format: "json".to_string(),
                    }),
                }])
            }
            WriteOp::Update => {
                let updates = extract_update_assignments(&dml.input)?;
                if updates.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "columnar UPDATE has no SET assignments".into(),
                    });
                }

                let filter_bytes = extract_where_filters(&dml.input)?;

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Columnar(ColumnarOp::Update {
                        collection: collection.to_string(),
                        filters: filter_bytes,
                        updates,
                    }),
                }])
            }
            WriteOp::Delete => {
                let filter_bytes = extract_where_filters(&dml.input)?;

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Columnar(ColumnarOp::Delete {
                        collection: collection.to_string(),
                        filters: filter_bytes,
                    }),
                }])
            }
            _ => Err(crate::Error::PlanError {
                detail: format!("{:?} not supported on columnar collections", dml.op),
            }),
        }
    }
}
