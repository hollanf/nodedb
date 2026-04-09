//! Detect equality on primary key → convert Scan to PointGet.

use crate::types::*;

/// If a Scan has a single equality filter on the primary key, convert to PointGet.
pub fn optimize(plan: SqlPlan) -> SqlPlan {
    match plan {
        SqlPlan::Scan {
            ref collection,
            ref alias,
            ref engine,
            ref filters,
            ..
        } if filters.len() == 1 => {
            if let Some((key_col, key_val)) = extract_pk_equality(&filters[0]) {
                return SqlPlan::PointGet {
                    collection: collection.clone(),
                    alias: alias.clone(),
                    engine: *engine,
                    key_column: key_col,
                    key_value: key_val,
                };
            }
            plan
        }
        _ => plan,
    }
}

/// Extract a simple equality filter on a known PK column.
fn extract_pk_equality(filter: &Filter) -> Option<(String, SqlValue)> {
    match &filter.expr {
        FilterExpr::Comparison {
            field,
            op: CompareOp::Eq,
            value,
        } => {
            // Only optimize for known PK columns.
            let f = field.to_lowercase();
            if f == "id" || f == "document_id" || f == "key" {
                Some((f, value.clone()))
            } else {
                None
            }
        }
        FilterExpr::Expr(SqlExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        }) => {
            let col = match left.as_ref() {
                SqlExpr::Column { name, .. } => name.to_lowercase(),
                _ => return None,
            };
            if col != "id" && col != "document_id" && col != "key" {
                return None;
            }
            let val = match right.as_ref() {
                SqlExpr::Literal(v) => v.clone(),
                _ => return None,
            };
            Some((col, val))
        }
        _ => None,
    }
}
