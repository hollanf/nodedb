//! Single SELECT statement planning (no UNION, no CTE wrapper).

use sqlparser::ast::{self, Select};

use super::helpers::{convert_projection, convert_where_to_filters, eval_constant_expr};
use super::where_search::try_extract_where_search;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::resolver::columns::TableScope;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Plan a single SELECT statement (no UNION, no CTE wrapper).
pub(super) fn plan_select(
    select: &Select,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    // 0. Intercept array table-valued functions before catalog resolution
    //    so a name like `NDARRAY_SLICE` is not looked up as a collection.
    if let Some(plan) =
        crate::planner::array_fn::try_plan_array_table_fn(&select.from, catalog, temporal)?
    {
        return Ok(plan);
    }

    // 1. Resolve FROM tables.
    let scope = TableScope::resolve_from(catalog, &select.from)?;

    // 2. Handle constant queries (no FROM clause): SELECT 1, SELECT 'hello', etc.
    if select.from.is_empty() {
        // Intercept maintenance functions (NDARRAY_FLUSH / NDARRAY_COMPACT)
        // before falling through to constant evaluation.
        if let Some(plan) =
            crate::planner::array_fn::try_plan_array_maint_fn(&select.projection, catalog)?
        {
            return Ok(plan);
        }
        let projection = convert_projection(&select.projection)?;
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for (i, proj) in projection.iter().enumerate() {
            match proj {
                Projection::Computed { expr, alias } => {
                    columns.push(alias.clone());
                    values.push(eval_constant_expr(expr, functions));
                }
                Projection::Column(name) => {
                    columns.push(name.clone());
                    values.push(SqlValue::Null);
                }
                _ => {
                    columns.push(format!("col{i}"));
                    values.push(SqlValue::Null);
                }
            }
        }
        return Ok(SqlPlan::ConstantResult { columns, values });
    }

    // 3. Check for JOINs.
    if let Some(plan) = try_plan_join(select, &scope, catalog, functions, temporal)? {
        return Ok(plan);
    }

    // 4. Single-table query.
    let table = scope.single_table().ok_or_else(|| SqlError::Unsupported {
        detail: "multi-table FROM without JOIN".into(),
    })?;

    // 4. Extract subqueries from WHERE and rewrite as semi/anti joins.
    let (subquery_joins, effective_where) = if let Some(expr) = &select.selection {
        let extraction =
            crate::planner::subquery::extract_subqueries(expr, catalog, functions, temporal)?;
        (extraction.joins, extraction.remaining_where)
    } else {
        (Vec::new(), None)
    };

    // 5. Convert remaining WHERE filters.
    let filters = match &effective_where {
        Some(expr) => {
            // Check for search-triggering functions in WHERE.
            if let Some(plan) = try_extract_where_search(expr, table, functions)? {
                return Ok(plan);
            }
            convert_where_to_filters(expr)?
        }
        None => Vec::new(),
    };

    // 6. Check for GROUP BY / aggregation.
    if has_aggregation(select, functions) {
        let mut plan = crate::planner::aggregate::plan_aggregate(
            select, table, &filters, &scope, functions, &temporal,
        )?;

        // Semi/anti subquery joins belong below the aggregate so they filter
        // the input rows before grouping. Scalar subqueries remain above the
        // aggregate because their column-vs-column comparison is evaluated
        // after the cross join materializes the scalar result row.
        if let SqlPlan::Aggregate { input, .. } = &mut plan {
            let mut base_input = std::mem::replace(
                input,
                Box::new(SqlPlan::ConstantResult {
                    columns: Vec::new(),
                    values: Vec::new(),
                }),
            );
            for sq in subquery_joins
                .iter()
                .filter(|sq| sq.join_type != JoinType::Cross)
            {
                base_input = Box::new(SqlPlan::Join {
                    left: base_input,
                    right: Box::new(sq.inner_plan.clone()),
                    on: vec![(sq.outer_column.clone(), sq.inner_column.clone())],
                    join_type: sq.join_type,
                    condition: None,
                    limit: 10000,
                    projection: Vec::new(),
                    filters: Vec::new(),
                });
            }
            *input = base_input;
        }

        for sq in subquery_joins
            .into_iter()
            .filter(|sq| sq.join_type == JoinType::Cross)
        {
            plan = SqlPlan::Join {
                left: Box::new(plan),
                right: Box::new(sq.inner_plan),
                on: vec![(sq.outer_column, sq.inner_column)],
                join_type: sq.join_type,
                condition: None,
                limit: 10000,
                projection: Vec::new(),
                filters: Vec::new(),
            };
        }
        return Ok(plan);
    }

    // 7. Convert projection.
    let projection = convert_projection(&select.projection)?;

    // 8. Convert window functions (SELECT with OVER).
    let window_functions =
        crate::planner::window::extract_window_functions(&select.projection, functions)?;

    // 9. Build base scan plan.
    let scan_projection = if subquery_joins.is_empty() {
        projection.clone()
    } else {
        Vec::new()
    };

    let rules = crate::engine_rules::resolve_engine_rules(table.info.engine);
    let mut plan = rules.plan_scan(crate::engine_rules::ScanParams {
        collection: table.name.clone(),
        alias: table.alias.clone(),
        filters,
        projection: scan_projection,
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: select.distinct.is_some(),
        window_functions,
        indexes: table.info.indexes.clone(),
        temporal,
        bitemporal: table.info.bitemporal,
    })?;

    // 10. Wrap with subquery joins (semi/anti/cross) if any.
    for sq in subquery_joins {
        // For cross-joins (scalar subqueries), move column-referencing filters
        // from the base scan to the join's post-filters. The filter compares
        // a field from the base scan with a field from the subquery result,
        // so it can only be evaluated after the join merges both sides.
        let join_filters = if sq.join_type == JoinType::Cross {
            if let SqlPlan::Scan {
                ref mut filters, ..
            } = plan
            {
                // Move filters that reference the scalar result column to the join.
                let mut moved = Vec::new();
                filters.retain(|f| {
                    if has_column_ref_filter(&f.expr) {
                        moved.push(f.clone());
                        false
                    } else {
                        true
                    }
                });
                moved
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        plan = SqlPlan::Join {
            left: Box::new(plan),
            right: Box::new(sq.inner_plan),
            on: vec![(sq.outer_column, sq.inner_column)],
            join_type: sq.join_type,
            condition: None,
            limit: 10000,
            projection: Vec::new(),
            filters: join_filters,
        };
    }

    if let SqlPlan::Join {
        projection: ref mut join_projection,
        ..
    } = plan
    {
        *join_projection = projection;
    }

    Ok(plan)
}

/// Check if a filter expression contains a column-vs-column comparison
/// (from scalar subquery rewriting). These filters must be evaluated
/// post-join, not pre-join, since one column comes from the subquery result.
fn has_column_ref_filter(expr: &FilterExpr) -> bool {
    match expr {
        FilterExpr::Expr(sql_expr) => has_column_comparison(sql_expr),
        FilterExpr::And(filters) => filters.iter().any(|f| has_column_ref_filter(&f.expr)),
        FilterExpr::Or(filters) => filters.iter().any(|f| has_column_ref_filter(&f.expr)),
        _ => false,
    }
}

fn has_column_comparison(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::BinaryOp { left, right, .. } => {
            let left_is_col = matches!(left.as_ref(), SqlExpr::Column { .. });
            let right_is_col = matches!(right.as_ref(), SqlExpr::Column { .. });
            if left_is_col && right_is_col {
                return true;
            }
            has_column_comparison(left) || has_column_comparison(right)
        }
        _ => false,
    }
}

/// Check if a SELECT has aggregation (GROUP BY or aggregate functions in projection).
fn has_aggregation(select: &Select, functions: &FunctionRegistry) -> bool {
    let group_by_non_empty = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    };
    if group_by_non_empty {
        return true;
    }
    for item in &select.projection {
        if let ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } =
            item
            && crate::aggregate_walk::contains_aggregate(expr, functions)
        {
            return true;
        }
    }
    false
}

/// Dispatch to the JOIN planner if the FROM contains joins.
fn try_plan_join(
    select: &Select,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<Option<SqlPlan>> {
    if select.from.len() != 1 {
        return Ok(None);
    }
    let from = &select.from[0];
    if from.joins.is_empty() {
        return Ok(None);
    }
    crate::planner::join::plan_join_from_select(select, scope, catalog, functions, temporal)
}
