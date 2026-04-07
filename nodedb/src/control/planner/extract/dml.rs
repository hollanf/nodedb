//! DML extraction: INSERT values, UPDATE assignments, point targets.

use datafusion::logical_expr::{LogicalPlan, Operator};
use datafusion::prelude::*;

use super::super::expr_convert::{expr_to_string, expr_to_value};

/// Strip Alias and Cast wrappers to get to the core expression.
fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(a) => strip_alias(&a.expr),
        Expr::Cast(c) => strip_alias(&c.expr),
        Expr::TryCast(c) => strip_alias(&c.expr),
        other => other,
    }
}

/// Extract SET field assignments from an UPDATE DML input plan.
///
/// DataFusion represents UPDATE SET as a projection with assignment expressions.
/// Returns `Vec<(field_name, json_value_bytes)>`.
pub(in crate::control::planner) fn extract_update_assignments(
    plan: &LogicalPlan,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let mut updates = Vec::new();
            let schema = proj.schema.fields();
            for (i, expr) in proj.expr.iter().enumerate() {
                let field_name = if i < schema.len() {
                    schema[i].name().clone()
                } else {
                    continue;
                };
                if field_name == "id" || field_name == "document_id" {
                    continue;
                }
                // Skip Column references — they represent unchanged columns that DataFusion
                // carries through the UPDATE projection. Only Literal (or Alias(Literal))
                // expressions are actual SET assignments.
                let core = strip_alias(expr);
                if matches!(core, Expr::Column(_)) {
                    continue;
                }
                let value = expr_to_value(core);
                let value_bytes = nodedb_types::value_to_msgpack(&value).map_err(|e| {
                    crate::Error::PlanError {
                        detail: format!("failed to serialize update value: {e}"),
                    }
                })?;
                updates.push((field_name, value_bytes));
            }
            Ok(updates)
        }
        _ => {
            // DataFusion may wrap UPDATE in various ways. Return empty if we can't parse.
            Ok(Vec::new())
        }
    }
}

/// Collect primary key values from equality predicates (`pk_col = 'value'` OR chains).
pub(in crate::control::planner) fn collect_eq_ids(
    expr: &Expr,
    pk_col: &str,
    ids: &mut Vec<String>,
) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (col_name, value) = match (&*binary.left, &*binary.right) {
                (Expr::Column(col), Expr::Literal(lit, _)) => (col.name.as_str(), lit.to_string()),
                (Expr::Literal(lit, _), Expr::Column(col)) => (col.name.as_str(), lit.to_string()),
                _ => return,
            };
            if col_name == pk_col {
                ids.push(value.trim_matches('\'').trim_matches('"').to_string());
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            collect_eq_ids(&binary.left, pk_col, ids);
            collect_eq_ids(&binary.right, pk_col, ids);
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_eq_ids(&binary.left, pk_col, ids);
            collect_eq_ids(&binary.right, pk_col, ids);
        }
        _ => {}
    }
}

/// Extract (document_id, value_bytes) pairs from an INSERT input plan.
///
/// DataFusion wraps `INSERT INTO t (col1, col2) VALUES (v1, v2)` as:
///   Projection(output=[col1, col2]) → Values([[column1, column2]])
///
/// The inner Values schema uses positional names (`column1`, `column2`).
/// The real column names live in the Projection's schema + expressions.
/// We use those to build the JSON object sent to the Data Plane.
pub(in crate::control::planner) fn extract_insert_values(
    plan: &LogicalPlan,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    match plan {
        // Projection wrapping Values: use Projection schema for column names
        // and evaluate each Projection expression against the single Values row.
        LogicalPlan::Projection(proj) => {
            if let LogicalPlan::Values(values) = proj.input.as_ref() {
                let schema = proj.schema.fields();
                let mut results = Vec::with_capacity(values.values.len());
                for row in &values.values {
                    // Resolve each Projection expression against the Values row.
                    // Projection expressions are typically Column("columnN") references
                    // or NULL literals for omitted columns. We evaluate them inline.
                    let mut exprs = Vec::with_capacity(proj.expr.len());
                    for expr in &proj.expr {
                        let resolved = resolve_projection_expr(expr, row);
                        exprs.push(resolved);
                    }

                    let doc_id = exprs.first().map(|e| expr_to_string(e)).unwrap_or_default();

                    let mut obj = std::collections::HashMap::new();
                    for (i, expr) in exprs.iter().enumerate() {
                        let field_name = schema
                            .get(i)
                            .map(|f| f.name().clone())
                            .unwrap_or_else(|| format!("column{i}"));
                        obj.insert(field_name, expr_to_value(expr));
                    }

                    let value_bytes = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(
                        obj,
                    ))
                    .map_err(|e| crate::Error::PlanError {
                        detail: format!("failed to serialize insert values: {e}"),
                    })?;
                    results.push((doc_id, value_bytes));
                }
                return Ok(results);
            }
            // Non-Values projection: recurse.
            extract_insert_values(&proj.input)
        }

        // Bare Values without projection (INSERT without explicit column list).
        // DataFusion may still use positional names here — best-effort.
        LogicalPlan::Values(values) => {
            let schema = values.schema.fields();
            let mut results = Vec::with_capacity(values.values.len());
            for row in &values.values {
                let doc_id = if let Some(first) = row.first() {
                    expr_to_string(first)
                } else {
                    continue;
                };
                let mut obj = std::collections::HashMap::new();
                for (i, expr) in row.iter().enumerate() {
                    let field_name = schema
                        .get(i)
                        .map(|f| f.name().clone())
                        .unwrap_or_else(|| format!("column{i}"));
                    obj.insert(field_name, expr_to_value(expr));
                }
                let value_bytes = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(obj))
                    .map_err(|e| crate::Error::PlanError {
                        detail: format!("failed to serialize insert values: {e}"),
                    })?;
                results.push((doc_id, value_bytes));
            }
            Ok(results)
        }

        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported INSERT input plan type: {}", plan.display()),
        }),
    }
}

/// Resolve a Projection expression against a Values row.
///
/// Projection expressions for INSERT are typically:
/// - `Alias(Column("columnN"), "real_name")` — reference to Values position N (1-based)
/// - `Column("columnN")` — same without alias
/// - `Alias(Cast(Column("columnN"), _), _)` — with type coercion
/// - `Literal(NULL)` — for columns omitted from the INSERT list
///
/// Returns the resolved `Expr` (literal) that can be passed to `expr_to_json_value`.
fn resolve_projection_expr<'a>(expr: &'a Expr, row: &'a [Expr]) -> &'a Expr {
    // Recursively strip Alias and Cast wrappers to reach the underlying Column or Literal.
    fn strip(e: &Expr) -> &Expr {
        match e {
            Expr::Alias(a) => strip(&a.expr),
            Expr::Cast(c) => strip(&c.expr),
            Expr::TryCast(c) => strip(&c.expr),
            other => other,
        }
    }

    let core = strip(expr);

    if let Expr::Column(col) = core {
        // DataFusion uses "columnN" (1-based) for Values schema references.
        let name = col.name.as_str();
        if let Some(rest) = name.strip_prefix("column") {
            if let Ok(idx) = rest.parse::<usize>() {
                // 1-based: "column1" → row[0]. Also strip any Alias/Cast
                // wrapping in the Values row itself (DataFusion type-coerces
                // literal values and aliases the original type as the alias name,
                // e.g., `Utf8("99.99") AS Float64(99.99)` for a coerced float).
                if idx > 0 && idx <= row.len() {
                    return strip(&row[idx - 1]);
                }
            }
        }
    }

    // For literals and other exprs, return the stripped form so
    // expr_to_json_value can handle it as a plain literal.
    core
}

/// Extract document IDs from a DML plan's `WHERE id = '...'` predicates.
///
/// Used by both DELETE and UPDATE to detect point operations (single-doc
/// targets) vs bulk operations (complex WHERE predicates).
pub(in crate::control::planner) fn extract_point_targets(
    plan: &LogicalPlan,
    pk_col: &str,
) -> crate::Result<Vec<String>> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut ids = Vec::new();
            collect_eq_ids(&filter.predicate, pk_col, &mut ids);
            Ok(ids)
        }
        // UPDATE wraps the Filter in a Projection (the SET assignments).
        LogicalPlan::Projection(proj) => extract_point_targets(&proj.input, pk_col),
        LogicalPlan::SubqueryAlias(alias) => extract_point_targets(&alias.input, pk_col),
        LogicalPlan::TableScan(_) => Err(crate::Error::PlanError {
            detail: "operation without WHERE clause is not supported".into(),
        }),
        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported input plan: {}", plan.display()),
        }),
    }
}
