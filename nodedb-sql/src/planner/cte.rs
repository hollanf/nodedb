//! CTE (WITH clause) and WITH RECURSIVE planning.

use sqlparser::ast::{self, Query, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::types::*;

/// Plan a WITH RECURSIVE query.
pub fn plan_recursive_cte(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    let with = query.with.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "expected WITH clause".into(),
    })?;

    // Get the CTE definition.
    let cte = with.cte_tables.first().ok_or_else(|| SqlError::Parse {
        detail: "empty WITH clause".into(),
    })?;

    let cte_query = &cte.query;

    // The CTE body should be a UNION of base case and recursive case.
    match &*cte_query.body {
        SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            left,
            right,
            ..
        } => {
            let base = plan_cte_branch(left, catalog, functions)?;
            let recursive = plan_cte_branch(right, catalog, functions)?;

            // Extract collection and filters from base/recursive plans.
            let collection = extract_collection(&base)
                .or_else(|| extract_collection(&recursive))
                .unwrap_or_default();

            Ok(SqlPlan::RecursiveScan {
                collection,
                base_filters: extract_filters(&base),
                recursive_filters: extract_filters(&recursive),
                max_iterations: 100,
                distinct: true,
                limit: 10000,
            })
        }
        _ => Err(SqlError::Unsupported {
            detail: "WITH RECURSIVE requires UNION in CTE body".into(),
        }),
    }
}

fn plan_cte_branch(
    expr: &SetExpr,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    match expr {
        SetExpr::Select(select) => {
            let query = Query {
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
            super::select::plan_query(&query, catalog, functions)
        }
        _ => Err(SqlError::Unsupported {
            detail: "CTE branch must be SELECT".into(),
        }),
    }
}

fn extract_collection(plan: &SqlPlan) -> Option<String> {
    match plan {
        SqlPlan::Scan { collection, .. } => Some(collection.clone()),
        _ => None,
    }
}

fn extract_filters(plan: &SqlPlan) -> Vec<Filter> {
    match plan {
        SqlPlan::Scan { filters, .. } => filters.clone(),
        _ => Vec::new(),
    }
}
