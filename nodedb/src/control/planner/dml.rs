//! DML plan conversion: INSERT, UPDATE, DELETE.

use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::converter::PlanConverter;
use super::extract::{extract_delete_targets, extract_insert_values, extract_update_assignments};

impl PlanConverter {
    /// Convert DML operations (INSERT, UPDATE, DELETE) to physical plans.
    pub(super) fn convert_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        let collection = dml.table_name.to_string().to_lowercase();
        let vshard = VShardId::from_collection(&collection);

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                let values = extract_insert_values(&dml.input)?;

                let mut tasks = Vec::with_capacity(values.len());
                for (doc_id, value_bytes) in values {
                    tasks.push(PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::PointPut {
                            collection: collection.clone(),
                            document_id: doc_id,
                            value: value_bytes,
                        },
                    });
                }

                if tasks.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "INSERT requires at least one row".into(),
                    });
                }

                Ok(tasks)
            }
            WriteOp::Delete => {
                let doc_ids = extract_delete_targets(&dml.input, &collection)?;

                if doc_ids.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "DELETE requires a WHERE clause with id = '<value>'".into(),
                    });
                }

                Ok(doc_ids
                    .into_iter()
                    .map(|doc_id| PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::PointDelete {
                            collection: collection.clone(),
                            document_id: doc_id,
                        },
                    })
                    .collect())
            }

            WriteOp::Update => {
                let doc_ids = extract_delete_targets(&dml.input, &collection)?;

                if doc_ids.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "UPDATE requires a WHERE clause with id = '<value>'".into(),
                    });
                }

                let updates = extract_update_assignments(&dml.input)?;

                if updates.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "UPDATE requires at least one SET assignment".into(),
                    });
                }

                Ok(doc_ids
                    .into_iter()
                    .map(|doc_id| PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::PointUpdate {
                            collection: collection.clone(),
                            document_id: doc_id,
                            updates: updates.clone(),
                        },
                    })
                    .collect())
            }
        }
    }

    /// Try to convert equality filters into a point get.
    ///
    /// Matches patterns like `WHERE id = 'value'` or `WHERE document_id = 'value'`.
    pub(super) fn try_point_get(
        &self,
        collection: &str,
        filters: &[Expr],
        tenant_id: TenantId,
        vshard: VShardId,
    ) -> crate::Result<Option<PhysicalTask>> {
        use datafusion::logical_expr::Operator;

        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter
                && binary.op == Operator::Eq
            {
                let (col_name, value) = match (&*binary.left, &*binary.right) {
                    (Expr::Column(col), Expr::Literal(lit, _)) => {
                        (col.name.as_str(), lit.to_string())
                    }
                    (Expr::Literal(lit, _), Expr::Column(col)) => {
                        (col.name.as_str(), lit.to_string())
                    }
                    _ => continue,
                };

                if col_name == "id" || col_name == "document_id" {
                    let doc_id = value.trim_matches('\'').trim_matches('"').to_string();

                    return Ok(Some(PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::PointGet {
                            collection: collection.to_string(),
                            document_id: doc_id,
                        },
                    }));
                }
            }
        }
        Ok(None)
    }
}
