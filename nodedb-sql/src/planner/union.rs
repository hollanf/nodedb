//! UNION / UNION ALL planning.

use sqlparser::ast::{self, SetExpr, SetOperator, SetQuantifier};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::types::*;

/// Plan a UNION / UNION ALL / INTERSECT / EXCEPT operation.
pub fn plan_set_operation(
    op: &SetOperator,
    left: &SetExpr,
    right: &SetExpr,
    quantifier: &SetQuantifier,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    let left_plan = plan_set_expr(left, catalog, functions, temporal)?;
    let right_plan = plan_set_expr(right, catalog, functions, temporal)?;

    match op {
        SetOperator::Union => {
            let distinct = matches!(quantifier, SetQuantifier::Distinct | SetQuantifier::None);
            Ok(SqlPlan::Union {
                inputs: vec![left_plan, right_plan],
                distinct,
            })
        }
        SetOperator::Intersect => {
            let all = matches!(quantifier, SetQuantifier::All);
            Ok(SqlPlan::Intersect {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                all,
            })
        }
        SetOperator::Except => {
            let all = matches!(quantifier, SetQuantifier::All);
            Ok(SqlPlan::Except {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                all,
            })
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("set operation: {op}"),
        }),
    }
}

fn plan_set_expr(
    expr: &SetExpr,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<SqlPlan> {
    match expr {
        SetExpr::Select(select) => {
            // Wrap in a dummy Query to reuse plan_query.
            let query = ast::Query {
                with: None,
                body: Box::new(SetExpr::Select(select.clone())),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            };
            super::select::plan_query(&query, catalog, functions, temporal)
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => plan_set_operation(
            op,
            left,
            right,
            set_quantifier,
            catalog,
            functions,
            temporal,
        ),
        _ => Err(SqlError::Unsupported {
            detail: format!("set expression: {expr}"),
        }),
    }
}
