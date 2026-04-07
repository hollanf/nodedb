//! Pre-processor for schemaless document DML (UPDATE / DELETE).
//!
//! DataFusion validates UPDATE/DELETE column names against the catalog schema.
//! Schemaless collections only expose `(id, document)` to DataFusion, so any
//! `UPDATE docs SET name = 'Bob'` is rejected before reaching `convert_dml`.
//!
//! This module parses UPDATE/DELETE SQL with `sqlparser` directly, detects
//! schemaless targets, and builds physical tasks without going through
//! DataFusion's column validator.

use nodedb_types::CollectionType;
use nodedb_types::columnar::DocumentMode;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Expr as SqlExpr, FromTable, Statement,
    TableFactor, Update, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::DocumentOp;
use crate::control::planner::converter::PlanConverter;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

/// Try to plan UPDATE or DELETE SQL for a schemaless document collection,
/// bypassing DataFusion's column-name validation.
///
/// Returns:
/// - `None`           — not a schemaless DML; caller should proceed normally.
/// - `Some(Ok(..))`   — successfully built tasks.
/// - `Some(Err(..))`  — parsing or extraction error; propagate the error.
pub(super) fn try_plan_schemaless_dml(
    sql: &str,
    tenant_id: TenantId,
    converter: &PlanConverter,
    returning: bool,
) -> Option<crate::Result<Vec<PhysicalTask>>> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    let stmt = stmts.into_iter().next()?;

    match stmt {
        Statement::Update(update) => plan_update(update, tenant_id, converter, returning),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            let first = tables.first()?;
            let collection = table_factor_name(&first.relation)?.to_lowercase();

            if !is_schemaless(converter, tenant_id, &collection) {
                return None;
            }

            let vshard = VShardId::from_collection(&collection);
            let doc_ids = delete
                .selection
                .as_ref()
                .map(extract_id_eq)
                .unwrap_or_default();

            if !doc_ids.is_empty() {
                return Some(Ok(doc_ids
                    .into_iter()
                    .map(|doc_id| PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                            collection: collection.clone(),
                            document_id: doc_id,
                        }),
                    })
                    .collect()));
            }

            let filters = match serialize_selection(delete.selection.as_ref()) {
                Ok(f) => f,
                Err(e) => return Some(Err(e)),
            };
            Some(Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                    collection,
                    filters,
                }),
            }]))
        }

        _ => None,
    }
}

fn plan_update(
    update: Update,
    tenant_id: TenantId,
    converter: &PlanConverter,
    returning: bool,
) -> Option<crate::Result<Vec<PhysicalTask>>> {
    let collection = table_factor_name(&update.table.relation)?.to_lowercase();

    if !is_schemaless(converter, tenant_id, &collection) {
        return None;
    }

    let vshard = VShardId::from_collection(&collection);
    let updates = match build_updates(&update.assignments) {
        Ok(u) => u,
        Err(e) => return Some(Err(e)),
    };

    if updates.is_empty() {
        return Some(Err(crate::Error::PlanError {
            detail: "UPDATE requires at least one SET assignment".into(),
        }));
    }

    let doc_ids = update
        .selection
        .as_ref()
        .map(extract_id_eq)
        .unwrap_or_default();

    if !doc_ids.is_empty() {
        return Some(Ok(doc_ids
            .into_iter()
            .map(|doc_id| PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.clone(),
                    document_id: doc_id,
                    updates: updates.clone(),
                    returning,
                }),
            })
            .collect()));
    }

    // No point targets → bulk update.
    let filters = match serialize_selection(update.selection.as_ref()) {
        Ok(f) => f,
        Err(e) => return Some(Err(e)),
    };
    Some(Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning,
        }),
    }]))
}

/// Check whether a collection is a schemaless document collection.
fn is_schemaless(converter: &PlanConverter, tenant_id: TenantId, collection: &str) -> bool {
    match converter.collection_type(tenant_id, collection) {
        Some(CollectionType::Document(DocumentMode::Schemaless)) => true,
        // No catalog entry → treat as schemaless (default for unknown collections).
        None => true,
        _ => false,
    }
}

/// Extract the bare table name from a sqlparser `TableFactor`.
fn table_factor_name(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, .. } => {
            // `ObjectName` is `Vec<ObjectNamePart>`; take the last part (bare table name).
            name.0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

/// Build `Vec<(field_name, json_value_bytes)>` from sqlparser UPDATE assignments.
fn build_updates(assignments: &[Assignment]) -> crate::Result<Vec<(String, Vec<u8>)>> {
    let mut updates = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let field_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.clone())
                .unwrap_or_default(),
            _ => continue,
        };

        if field_name.is_empty() || field_name == "id" || field_name == "document_id" {
            continue;
        }

        let json_val = sql_expr_to_json(&assignment.value)?;
        let value_bytes = serde_json::to_vec(&json_val).map_err(|e| crate::Error::PlanError {
            detail: format!("failed to serialize update value: {e}"),
        })?;
        updates.push((field_name, value_bytes));
    }
    Ok(updates)
}

/// Convert a sqlparser expression to a `serde_json::Value` for storage.
fn sql_expr_to_json(expr: &SqlExpr) -> crate::Result<serde_json::Value> {
    match expr {
        SqlExpr::Value(v) => sql_value_to_json(&v.value),
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let inner = sql_expr_to_json(expr)?;
            match inner {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(serde_json::Value::Number((-i).into()))
                    } else if let Some(f) = n.as_f64() {
                        Ok(serde_json::Number::from_f64(-f)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null))
                    } else {
                        Ok(serde_json::Value::Null)
                    }
                }
                _ => Ok(serde_json::Value::Null),
            }
        }
        SqlExpr::Identifier(ident) => Ok(serde_json::Value::String(ident.value.clone())),
        _ => Err(crate::Error::PlanError {
            detail: format!("UPDATE SET value must be a literal, got: {expr}"),
        }),
    }
}

fn sql_value_to_json(val: &SqlValue) -> crate::Result<serde_json::Value> {
    match val {
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(serde_json::Value::String(s.clone()))
        }
        SqlValue::Number(s, _) => {
            if s.contains('.') {
                s.parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| crate::Error::PlanError {
                        detail: format!("invalid float literal: {s}"),
                    })
            } else {
                s.parse::<i64>()
                    .map(|i| serde_json::Value::Number(i.into()))
                    .map_err(|_| crate::Error::PlanError {
                        detail: format!("invalid integer literal: {s}"),
                    })
            }
        }
        SqlValue::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        SqlValue::Null => Ok(serde_json::Value::Null),
        other => Err(crate::Error::PlanError {
            detail: format!("unsupported literal type in SET clause: {other}"),
        }),
    }
}

/// Extract document IDs from `WHERE id = '<value>'` (and OR chains).
fn extract_id_eq(expr: &SqlExpr) -> Vec<String> {
    let mut ids = Vec::new();
    collect_id_eq(expr, &mut ids);
    ids
}

fn collect_id_eq(expr: &SqlExpr, ids: &mut Vec<String>) {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                let (col_expr, val_expr) = match (left.as_ref(), right.as_ref()) {
                    (SqlExpr::Identifier(_), SqlExpr::Value(_)) => (left.as_ref(), right.as_ref()),
                    (SqlExpr::Value(_), SqlExpr::Identifier(_)) => (right.as_ref(), left.as_ref()),
                    _ => return,
                };
                let col_name = if let SqlExpr::Identifier(ident) = col_expr {
                    ident.value.as_str()
                } else {
                    return;
                };
                if col_name == "id" || col_name == "document_id" {
                    if let SqlExpr::Value(vws) = val_expr {
                        match &vws.value {
                            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                                ids.push(s.clone())
                            }
                            _ => {}
                        }
                    }
                }
            }
            BinaryOperator::Or | BinaryOperator::And => {
                collect_id_eq(left, ids);
                collect_id_eq(right, ids);
            }
            _ => {}
        },
        _ => {}
    }
}

/// Serialize a sqlparser WHERE expression as ScanFilter bytes for bulk operations.
fn serialize_selection(selection: Option<&SqlExpr>) -> crate::Result<Vec<u8>> {
    let Some(expr) = selection else {
        return Ok(Vec::new());
    };
    let filters = sql_expr_to_scan_filters(expr);
    zerompk::to_msgpack_vec(&filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("filter serialization: {e}"),
    })
}

fn sql_expr_to_scan_filters(expr: &SqlExpr) -> Vec<crate::bridge::scan_filter::ScanFilter> {
    use crate::bridge::scan_filter::FilterOp;

    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => extract_cmp_filter(left, right, FilterOp::Eq),
            BinaryOperator::Gt => extract_cmp_filter(left, right, FilterOp::Gt),
            BinaryOperator::GtEq => extract_cmp_filter(left, right, FilterOp::Gte),
            BinaryOperator::Lt => extract_cmp_filter(left, right, FilterOp::Lt),
            BinaryOperator::LtEq => extract_cmp_filter(left, right, FilterOp::Lte),
            BinaryOperator::And => {
                let mut filters = sql_expr_to_scan_filters(left);
                filters.extend(sql_expr_to_scan_filters(right));
                filters
            }
            _ => vec![],
        },
        _ => vec![],
    }
}

fn extract_cmp_filter(
    left: &SqlExpr,
    right: &SqlExpr,
    op: crate::bridge::scan_filter::FilterOp,
) -> Vec<crate::bridge::scan_filter::ScanFilter> {
    match (left, right) {
        (SqlExpr::Identifier(id), SqlExpr::Value(vws)) => {
            sql_value_to_scan_filter(id.value.as_str(), op, &vws.value)
        }
        _ => vec![],
    }
}

fn sql_value_to_scan_filter(
    field: &str,
    op: crate::bridge::scan_filter::FilterOp,
    val: &SqlValue,
) -> Vec<crate::bridge::scan_filter::ScanFilter> {
    use crate::bridge::scan_filter::ScanFilter;
    use nodedb_types::value::Value;

    let value = match val {
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Value::String(s.clone())
        }
        SqlValue::Number(s, _) => {
            if s.contains('.') {
                s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
            } else {
                s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null)
            }
        }
        SqlValue::Boolean(b) => Value::Bool(*b),
        SqlValue::Null => Value::Null,
        _ => return vec![],
    };

    vec![ScanFilter {
        field: field.to_string(),
        op,
        value,
        clauses: Vec::new(),
    }]
}
