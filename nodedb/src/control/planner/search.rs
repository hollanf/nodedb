//! Search pattern detection helpers for the plan converter.
//! Handles both vector similarity and BM25 full-text search patterns.
//!
//! These are standalone functions extracted from `PlanConverter` to keep
//! `converter.rs` under the 500-line limit. They handle the
//! `ORDER BY vector_distance(...) LIMIT k` pattern detection and related
//! utilities.

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;

/// Detect `ORDER BY vector_distance(field, ARRAY[...]) LIMIT k` pattern.
///
/// Returns `(collection, query_vector, top_k)` if the sort expression
/// contains a `vector_distance()` function call over a table scan.
pub(super) fn try_extract_vector_search(
    sort_exprs: &[datafusion::logical_expr::expr::Sort],
    input: &LogicalPlan,
    fetch: Option<usize>,
) -> crate::Result<Option<(String, Vec<f32>, usize)>> {
    // Check if the first sort expression is a ScalarFunction named "vector_distance".
    let first = match sort_exprs.first() {
        Some(s) => s,
        None => return Ok(None),
    };

    let func_name = match &first.expr {
        Expr::ScalarFunction(func) => func.name(),
        _ => return Ok(None),
    };

    if func_name != "vector_distance" {
        return Ok(None);
    }

    let func = match &first.expr {
        Expr::ScalarFunction(f) => f,
        _ => return Ok(None),
    };

    // Extract query vector from the second argument (ARRAY literal).
    let query_vector = if func.args.len() >= 2 {
        extract_float_array(&func.args[1])?
    } else {
        return Ok(None);
    };

    if query_vector.is_empty() {
        return Ok(None);
    }

    // Extract collection from the input plan (should be a TableScan).
    let collection = match extract_table_name(input) {
        Some(c) => c,
        None => return Ok(None),
    };

    // top_k from LIMIT, default 10.
    let top_k = fetch.unwrap_or(10);

    Ok(Some((collection, query_vector, top_k)))
}

/// Extract a float array from an Expr (for vector_distance query vector).
pub(super) fn extract_float_array(expr: &Expr) -> crate::Result<Vec<f32>> {
    match expr {
        Expr::Literal(lit) => {
            let s = lit.to_string();
            // Try to parse as comma-separated floats: [0.1, 0.2, 0.3]
            let trimmed = s.trim_matches(|c| c == '[' || c == ']');
            let values: Vec<f32> = trimmed
                .split(',')
                .filter_map(|v| v.trim().parse::<f32>().ok())
                .collect();
            Ok(values)
        }
        Expr::ScalarFunction(func) if func.name() == "make_array" => {
            // DataFusion represents ARRAY[1,2,3] as make_array(1,2,3).
            let mut values = Vec::with_capacity(func.args.len());
            for arg in &func.args {
                if let Expr::Literal(lit) = arg {
                    let s = lit.to_string();
                    if let Ok(f) = s.parse::<f32>() {
                        values.push(f);
                    }
                }
            }
            Ok(values)
        }
        _ => Ok(Vec::new()),
    }
}

/// Extract the table name from a logical plan (traversing projections/filters).
pub(super) fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan.table_name.to_string()),
        LogicalPlan::Projection(proj) => extract_table_name(&proj.input),
        LogicalPlan::Filter(filter) => extract_table_name(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => extract_table_name(&alias.input),
        _ => None,
    }
}

/// Detect `ORDER BY bm25_score(field, 'query') LIMIT k` and extract search parameters.
///
/// Returns `(collection, query_text, top_k)` if the pattern matches.
pub(super) fn try_extract_text_search(
    sort_exprs: &[datafusion::logical_expr::SortExpr],
    input: &LogicalPlan,
    fetch: Option<usize>,
) -> Option<(String, String, usize)> {
    let sort_expr = sort_exprs.first()?;
    let Expr::ScalarFunction(func) = &sort_expr.expr else {
        return None;
    };
    if func.name() != "bm25_score" || func.args.len() != 2 {
        return None;
    }
    let (Expr::Literal(_field_lit), Expr::Literal(query_lit)) = (&func.args[0], &func.args[1])
    else {
        return None;
    };
    let query_text = query_lit.to_string().trim_matches('\'').to_string();
    let collection = extract_table_name(input)?;
    let top_k = fetch.unwrap_or(10);
    Some((collection, query_text, top_k))
}
