//! Filter and expression extraction helpers for the plan converter.
//!
//! These are standalone functions used by `PlanConverter` to translate
//! DataFusion `Expr` trees into NodeDB scan filters, update assignments,
//! and scalar values.

use datafusion::logical_expr::{BinaryExpr, LogicalPlan, Operator};
use datafusion::prelude::*;
use tracing::warn;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

/// Convert a DataFusion expression to scan filter predicates.
///
/// Supports: eq, ne, gt, gte, lt, lte on any field.
/// AND: flattened to a list of predicates (all must match).
/// OR: emitted as `{"op": "or", "clauses": [[...], [...]]}` — each clause
///     is a list of AND predicates. The document matches if ANY clause matches.
///
/// This representation lets the Data Plane evaluate OR without changing the
/// simple `ScanFilter` struct: the executor handles the `"or"` filter type
/// by evaluating each clause group as an AND-set and short-circuiting on
/// the first match.
pub(super) fn expr_to_scan_filters(expr: &Expr) -> Vec<serde_json::Value> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let mut filters = expr_to_scan_filters(&binary.left);
            filters.extend(expr_to_scan_filters(&binary.right));
            filters
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            // Collect each side of the OR as an independent AND-group.
            let left_filters = expr_to_scan_filters(&binary.left);
            let right_filters = expr_to_scan_filters(&binary.right);

            // If either side produced no filters (unsupported expression),
            // we can't safely evaluate the OR — both sides must be evaluable
            // or we risk returning rows that should be excluded. Fall back to
            // full scan (no filter) rather than partial evaluation.
            if left_filters.is_empty() || right_filters.is_empty() {
                warn!("OR predicate has unsupported branch; falling back to unfiltered scan");
                return Vec::new();
            }

            vec![serde_json::json!({
                "op": "or",
                "clauses": [left_filters, right_filters],
            })]
        }
        Expr::BinaryExpr(binary) => {
            let op_str = match binary.op {
                Operator::Eq => "eq",
                Operator::NotEq => "ne",
                Operator::Gt => "gt",
                Operator::GtEq => "gte",
                Operator::Lt => "lt",
                Operator::LtEq => "lte",
                _ => return Vec::new(),
            };

            let (field, value) = match (&*binary.left, &*binary.right) {
                (Expr::Column(col), Expr::Literal(lit)) => (
                    col.name.clone(),
                    expr_to_json_value(&Expr::Literal(lit.clone())),
                ),
                (Expr::Literal(lit), Expr::Column(col)) => (
                    col.name.clone(),
                    expr_to_json_value(&Expr::Literal(lit.clone())),
                ),
                _ => return Vec::new(),
            };

            vec![serde_json::json!({
                "field": field,
                "op": op_str,
                "value": value,
            })]
        }
        Expr::Like(like) => {
            // LIKE / ILIKE: extract field name and pattern.
            // DataFusion represents LIKE as: Like { negated, expr, pattern, escape_char, case_insensitive }
            if let Expr::Column(col) = &*like.expr {
                let pattern = expr_to_json_value(&like.pattern);
                let op = if like.case_insensitive {
                    if like.negated { "not_ilike" } else { "ilike" }
                } else if like.negated {
                    "not_like"
                } else {
                    "like"
                };
                vec![serde_json::json!({
                    "field": col.name,
                    "op": op,
                    "value": pattern,
                })]
            } else {
                Vec::new()
            }
        }
        Expr::IsNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                vec![serde_json::json!({
                    "field": col.name,
                    "op": "is_null",
                    "value": null,
                })]
            } else {
                Vec::new()
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                vec![serde_json::json!({
                    "field": col.name,
                    "op": "is_not_null",
                    "value": null,
                })]
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}

/// Extract a usize from an Expr (for OFFSET values).
pub(super) fn expr_to_usize(expr: &Expr) -> crate::Result<usize> {
    match expr {
        Expr::Literal(lit) => {
            let s = lit.to_string();
            s.parse::<usize>().map_err(|_| crate::Error::PlanError {
                detail: format!("expected integer for OFFSET, got: {s}"),
            })
        }
        _ => Err(crate::Error::PlanError {
            detail: format!("expected literal for OFFSET, got: {expr}"),
        }),
    }
}

/// Extract SET field assignments from an UPDATE DML input plan.
///
/// DataFusion represents UPDATE SET as a projection with assignment expressions.
/// Returns `Vec<(field_name, json_value_bytes)>`.
pub(super) fn extract_update_assignments(
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
                // Skip the id column — it's the WHERE target, not a SET.
                if field_name == "id" || field_name == "document_id" {
                    continue;
                }
                let value = expr_to_json_value(expr);
                let value_bytes =
                    serde_json::to_vec(&value).map_err(|e| crate::Error::Serialization {
                        format: "json".into(),
                        detail: format!("filter serialization: {e}"),
                    })?;
                updates.push((field_name, value_bytes));
            }
            Ok(updates)
        }
        LogicalPlan::Filter(filter) => extract_update_assignments(&filter.input),
        _ => {
            // DataFusion may wrap UPDATE in various ways. Return empty if we can't parse.
            Ok(Vec::new())
        }
    }
}

/// Collect document IDs from equality predicates (id = 'value' OR id = 'value2').
pub(super) fn collect_eq_ids(expr: &Expr, ids: &mut Vec<String>) {
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
            collect_eq_ids(&binary.left, ids);
            collect_eq_ids(&binary.right, ids);
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_eq_ids(&binary.left, ids);
            collect_eq_ids(&binary.right, ids);
        }
        _ => {}
    }
}

/// Convert an expression to a string value (for document IDs).
pub(super) fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Literal(lit) => {
            let s = lit.to_string();
            s.trim_matches('\'').trim_matches('"').to_string()
        }
        _ => format!("{expr}"),
    }
}

/// Convert an expression to a JSON value (for document fields).
pub(super) fn expr_to_json_value(expr: &Expr) -> serde_json::Value {
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

/// Extract (document_id, value_bytes) pairs from an INSERT input plan.
///
/// DataFusion represents `INSERT INTO t VALUES (...)` as a projection of
/// literal values. The first column is the document ID; remaining columns
/// are serialized as a JSON object.
pub(super) fn extract_insert_values(plan: &LogicalPlan) -> crate::Result<Vec<(String, Vec<u8>)>> {
    match plan {
        LogicalPlan::Values(values) => {
            let schema = values.schema.fields();
            let mut results = Vec::with_capacity(values.values.len());
            for row in &values.values {
                let doc_id = if let Some(first) = row.first() {
                    expr_to_string(first)
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
                    let val = expr_to_json_value(expr);
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
        LogicalPlan::Projection(proj) => extract_insert_values(&proj.input),
        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported INSERT input plan type: {}", plan.display()),
        }),
    }
}

/// Extract document IDs to delete from a DELETE plan's filter.
pub(super) fn extract_delete_targets(
    plan: &LogicalPlan,
    _collection: &str,
) -> crate::Result<Vec<String>> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut ids = Vec::new();
            collect_eq_ids(&filter.predicate, &mut ids);
            Ok(ids)
        }
        LogicalPlan::TableScan(_) => Err(crate::Error::PlanError {
            detail: "DELETE without WHERE clause is not supported. Use DROP COLLECTION to remove all data.".into(),
        }),
        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported DELETE input plan: {}", plan.display()),
        }),
    }
}

/// Try to convert a predicate on a non-id field into an index-backed RangeScan.
///
/// Supports:
/// - Equality: `WHERE field = value` → exact range `[value, value\0)`
/// - Range: `WHERE field > lower AND field < upper` → range `(lower, upper)`
/// - Single-bound: `WHERE field >= value` → range `[value, ...)`
///
/// The Data Plane's RangeScan scans the INDEXES table
/// (`{tenant}:{collection}:{field}:{value}:*`) and returns matching
/// document IDs directly — much faster than scanning all documents.
///
/// Only fires for single-field predicates. Multi-field WHERE clauses
/// fall through to DocumentScan.
pub(super) fn try_range_scan_from_predicate(
    collection: &str,
    predicate: &Expr,
    tenant_id: TenantId,
    vshard: VShardId,
) -> Option<PhysicalTask> {
    match predicate {
        // Simple equality: field = value
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (col_name, value) = extract_col_literal(binary)?;
            if col_name == "id" || col_name == "document_id" {
                return None;
            }
            let value_clean = value.trim_matches('\'').trim_matches('"').to_string();
            Some(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::RangeScan {
                    collection: collection.to_string(),
                    field: col_name.to_string(),
                    lower: Some(value_clean.as_bytes().to_vec()),
                    upper: Some(format!("{value_clean}\x00").as_bytes().to_vec()),
                    limit: 1000,
                },
            })
        }

        // Range predicates on single field: GT, GTE, LT, LTE
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
            ) =>
        {
            let (col_name, value) = extract_col_literal(binary)?;
            if col_name == "id" || col_name == "document_id" {
                return None;
            }
            let value_clean = value.trim_matches('\'').trim_matches('"').to_string();
            let value_bytes = value_clean.as_bytes().to_vec();

            let (lower, upper) = match binary.op {
                // field > value: exclusive lower bound (append \0 to skip exact match).
                Operator::Gt => (Some(format!("{value_clean}\x00").as_bytes().to_vec()), None),
                // field >= value: inclusive lower bound.
                Operator::GtEq => (Some(value_bytes), None),
                // field < value: exclusive upper bound.
                Operator::Lt => (None, Some(value_bytes)),
                // field <= value: inclusive upper bound (append \0 to include exact match).
                Operator::LtEq => (None, Some(format!("{value_clean}\x00").as_bytes().to_vec())),
                _ => return None,
            };

            Some(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::RangeScan {
                    collection: collection.to_string(),
                    field: col_name.to_string(),
                    lower,
                    upper,
                    limit: 1000,
                },
            })
        }

        // AND of two range predicates on the same field:
        // WHERE field >= lower AND field < upper
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left_scan =
                try_range_scan_from_predicate(collection, &binary.left, tenant_id, vshard)?;
            let right_scan =
                try_range_scan_from_predicate(collection, &binary.right, tenant_id, vshard)?;

            // Both must be RangeScans on the same field.
            if let (
                PhysicalPlan::RangeScan {
                    field: f1,
                    lower: l1,
                    upper: u1,
                    ..
                },
                PhysicalPlan::RangeScan {
                    field: f2,
                    lower: l2,
                    upper: u2,
                    ..
                },
            ) = (&left_scan.plan, &right_scan.plan)
            {
                if f1 == f2 {
                    // Merge bounds: take whichever side provides lower/upper.
                    let merged_lower = l1.clone().or_else(|| l2.clone());
                    let merged_upper = u1.clone().or_else(|| u2.clone());
                    return Some(PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::RangeScan {
                            collection: collection.to_string(),
                            field: f1.clone(),
                            lower: merged_lower,
                            upper: merged_upper,
                            limit: 1000,
                        },
                    });
                }
            }
            None
        }

        _ => None,
    }
}

/// Extract (column_name, literal_value_string) from a binary expression
/// where one side is a Column and the other is a Literal.
pub(super) fn extract_col_literal(binary: &BinaryExpr) -> Option<(String, String)> {
    match (&*binary.left, &*binary.right) {
        (Expr::Column(col), Expr::Literal(lit)) => Some((col.name.clone(), lit.to_string())),
        (Expr::Literal(lit), Expr::Column(col)) => Some((col.name.clone(), lit.to_string())),
        _ => None,
    }
}
