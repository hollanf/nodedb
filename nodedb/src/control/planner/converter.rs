use datafusion::logical_expr::{FetchType, LogicalPlan, Operator};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

/// Converts DataFusion logical plans into NodeDB physical tasks.
///
/// This is the bridge between DataFusion's `Send` logical plans and
/// our `!Send` Data Plane execution. The converter walks the logical
/// plan tree and produces `PhysicalPlan` variants that the CoreLoop
/// can execute.
///
/// Lives on the Control Plane (Send + Sync).
pub struct PlanConverter;

impl PlanConverter {
    pub fn new() -> Self {
        Self
    }

    /// Convert a DataFusion logical plan into one or more physical tasks.
    ///
    /// Simple queries (point gets, scans) produce a single task.
    /// Complex queries (joins, aggregations) may produce multiple tasks
    /// targeting different vShards.
    pub fn convert(
        &self,
        plan: &LogicalPlan,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        match plan {
            LogicalPlan::Projection(proj) => self.convert(&proj.input, tenant_id),

            LogicalPlan::Filter(filter) => {
                // Check if the filter predicate can be converted to a point get
                // before recursing into the input.
                if let LogicalPlan::TableScan(scan) = filter.input.as_ref() {
                    let collection = scan.table_name.to_string();
                    let vshard = VShardId::from_collection(&collection);

                    if let Some(task) = self.try_point_get(
                        &collection,
                        std::slice::from_ref(&filter.predicate),
                        tenant_id,
                        vshard,
                    )? {
                        return Ok(vec![task]);
                    }
                }
                self.convert(&filter.input, tenant_id)
            }

            LogicalPlan::TableScan(scan) => {
                let collection = scan.table_name.to_string();
                let vshard = VShardId::from_collection(&collection);

                // Check for filter pushdown: equality on id → point get.
                if let Some(task) =
                    self.try_point_get(&collection, &scan.filters, tenant_id, vshard)?
                {
                    return Ok(vec![task]);
                }

                // Default: range scan with optional fetch limit.
                let limit = scan.fetch.unwrap_or(1000);
                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::RangeScan {
                        collection,
                        field: "id".into(),
                        lower: None,
                        upper: None,
                        limit,
                    },
                }])
            }

            LogicalPlan::Limit(limit) => {
                let mut tasks = self.convert(&limit.input, tenant_id)?;
                if let Ok(FetchType::Literal(Some(n))) = limit.get_fetch_type() {
                    for task in &mut tasks {
                        if let PhysicalPlan::RangeScan { limit, .. } = &mut task.plan {
                            *limit = n;
                        }
                    }
                }
                Ok(tasks)
            }

            LogicalPlan::SubqueryAlias(alias) => self.convert(&alias.input, tenant_id),

            LogicalPlan::Sort(sort) => {
                // Pass through to input — sorting happens in Data Plane or post-processing.
                self.convert(&sort.input, tenant_id)
            }

            LogicalPlan::Dml(dml) => self.convert_dml(dml, tenant_id),

            _ => Err(crate::Error::PlanError {
                detail: format!("unsupported logical plan type: {}", plan.display()),
            }),
        }
    }

    /// Convert DML operations (INSERT, UPDATE, DELETE) to physical plans.
    fn convert_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        let collection = dml.table_name.to_string();
        let vshard = VShardId::from_collection(&collection);

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                // Extract values from the input plan.
                // DataFusion represents INSERT VALUES as a projection of literals.
                let values = self.extract_insert_values(&dml.input)?;

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
            WriteOp::Delete | WriteOp::Update => {
                // DELETE FROM <collection> WHERE id = '<value>'
                // Extract the id from the filter in the input plan.
                let doc_ids = self.extract_delete_targets(&dml.input, &collection)?;

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
        }
    }

    /// Extract (document_id, value_bytes) pairs from an INSERT input plan.
    ///
    /// DataFusion represents `INSERT INTO t VALUES (...)` as a projection of
    /// literal values. We extract the first column as the document ID and
    /// serialize the remaining columns as a JSON object.
    fn extract_insert_values(&self, plan: &LogicalPlan) -> crate::Result<Vec<(String, Vec<u8>)>> {
        match plan {
            LogicalPlan::Values(values) => {
                let schema = values.schema.fields();
                let mut results = Vec::with_capacity(values.values.len());

                for row in &values.values {
                    // First column is the ID. Remaining columns are data fields.
                    let doc_id = if let Some(first) = row.first() {
                        Self::expr_to_string(first)
                    } else {
                        continue;
                    };

                    let mut obj = serde_json::Map::new();
                    for (i, expr) in row.iter().enumerate() {
                        let field_name = if i < schema.len() {
                            schema[i].name().clone()
                        } else {
                            format!("column{i}")
                        };
                        let val = Self::expr_to_json_value(expr);
                        obj.insert(field_name, val);
                    }

                    let value_bytes =
                        serde_json::to_vec(&obj).map_err(|e| crate::Error::PlanError {
                            detail: format!("failed to serialize insert values: {e}"),
                        })?;

                    results.push((doc_id, value_bytes));
                }

                Ok(results)
            }
            LogicalPlan::Projection(proj) => self.extract_insert_values(&proj.input),
            _ => Err(crate::Error::PlanError {
                detail: format!("unsupported INSERT input plan type: {}", plan.display()),
            }),
        }
    }

    /// Extract document IDs to delete from a DELETE plan's filter.
    fn extract_delete_targets(
        &self,
        plan: &LogicalPlan,
        _collection: &str,
    ) -> crate::Result<Vec<String>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let mut ids = Vec::new();
                Self::collect_eq_ids(&filter.predicate, &mut ids);
                Ok(ids)
            }
            LogicalPlan::TableScan(_) => {
                // DELETE without WHERE — not supported (too dangerous).
                Err(crate::Error::PlanError {
                    detail: "DELETE without WHERE clause is not supported. Use DROP COLLECTION to remove all data.".into(),
                })
            }
            _ => Err(crate::Error::PlanError {
                detail: format!("unsupported DELETE input plan: {}", plan.display()),
            }),
        }
    }

    /// Collect document IDs from equality predicates (id = 'value' OR id = 'value2').
    fn collect_eq_ids(expr: &Expr, ids: &mut Vec<String>) {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                let (col_name, value) = match (&*binary.left, &*binary.right) {
                    (Expr::Column(col), Expr::Literal(lit)) => (col.name.as_str(), lit.to_string()),
                    (Expr::Literal(lit), Expr::Column(col)) => (col.name.as_str(), lit.to_string()),
                    _ => return,
                };
                if col_name == "id" || col_name == "document_id" {
                    ids.push(value.trim_matches('\'').trim_matches('"').to_string());
                }
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                Self::collect_eq_ids(&binary.left, ids);
                Self::collect_eq_ids(&binary.right, ids);
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                Self::collect_eq_ids(&binary.left, ids);
                Self::collect_eq_ids(&binary.right, ids);
            }
            _ => {}
        }
    }

    /// Convert an expression to a string value (for document IDs).
    fn expr_to_string(expr: &Expr) -> String {
        match expr {
            Expr::Literal(lit) => {
                let s = lit.to_string();
                s.trim_matches('\'').trim_matches('"').to_string()
            }
            _ => format!("{expr}"),
        }
    }

    /// Convert an expression to a JSON value (for document fields).
    fn expr_to_json_value(expr: &Expr) -> serde_json::Value {
        match expr {
            Expr::Literal(lit) => {
                let s = lit.to_string();
                // Try parsing as number first.
                if let Ok(n) = s.parse::<i64>() {
                    return serde_json::Value::Number(n.into());
                }
                if let Ok(n) = s.parse::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(n) {
                        return serde_json::Value::Number(num);
                    }
                }
                if s == "true" {
                    return serde_json::Value::Bool(true);
                }
                if s == "false" {
                    return serde_json::Value::Bool(false);
                }
                if s == "NULL" || s == "null" {
                    return serde_json::Value::Null;
                }
                // String value — strip quotes.
                serde_json::Value::String(s.trim_matches('\'').trim_matches('"').to_string())
            }
            _ => serde_json::Value::String(format!("{expr}")),
        }
    }

    /// Try to convert equality filters into a point get.
    ///
    /// Matches patterns like `WHERE id = 'value'` or `WHERE document_id = 'value'`.
    fn try_point_get(
        &self,
        collection: &str,
        filters: &[Expr],
        tenant_id: TenantId,
        vshard: VShardId,
    ) -> crate::Result<Option<PhysicalTask>> {
        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter {
                if binary.op == Operator::Eq {
                    let (col_name, value) = match (&*binary.left, &*binary.right) {
                        (Expr::Column(col), Expr::Literal(lit)) => {
                            (col.name.as_str(), lit.to_string())
                        }
                        (Expr::Literal(lit), Expr::Column(col)) => {
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
        }
        Ok(None)
    }
}

impl Default for PlanConverter {
    fn default() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    async fn make_context_with_table() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE users (id VARCHAR, name VARCHAR, email VARCHAR) AS VALUES ('u1', 'alice', 'a@b.com'), ('u2', 'bob', 'b@c.com')")
            .await
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn converts_point_get() {
        let ctx = make_context_with_table().await;
        let df = ctx
            .sql("SELECT * FROM users WHERE id = 'u1'")
            .await
            .unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);

        match &tasks[0].plan {
            PhysicalPlan::PointGet { document_id, .. } => {
                assert_eq!(document_id, "u1");
            }
            other => panic!("expected PointGet, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn converts_table_scan() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT name FROM users").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(matches!(&tasks[0].plan, PhysicalPlan::RangeScan { .. }));
    }

    #[tokio::test]
    async fn limit_propagates() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT * FROM users LIMIT 5").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::RangeScan { limit, .. } => assert_eq!(*limit, 5),
            other => panic!("expected RangeScan, got {other:?}"),
        }
    }
}
