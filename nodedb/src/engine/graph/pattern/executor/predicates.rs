//! WHERE predicate application and RETURN column projection.

use super::super::ast::*;
use super::{BindingRow, execute_clause};
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;

/// Apply a WHERE predicate to filter rows.
pub(super) fn apply_predicate(
    rows: &[BindingRow],
    predicate: &WherePredicate,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
) -> Result<Vec<BindingRow>, crate::Error> {
    match predicate {
        WherePredicate::Equals {
            binding,
            field,
            value,
        } => {
            if field.is_empty() {
                Ok(rows
                    .iter()
                    .filter(|row| row.get(binding).is_some_and(|v| v == value))
                    .cloned()
                    .collect())
            } else {
                Ok(rows
                    .iter()
                    .filter(|row| {
                        if let Some(node_id) = row.get(binding) {
                            check_property(edge_store, node_id, field, value)
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect())
            }
        }

        WherePredicate::Comparison {
            binding,
            field,
            op,
            value,
        } => {
            let _ = (binding, field, op, value);
            Ok(rows.to_vec())
        }

        WherePredicate::NotExists { sub_pattern } => {
            let mut result = Vec::new();
            for row in rows {
                let sub_rows = execute_clause(sub_pattern, csr, std::slice::from_ref(row))?;
                if sub_rows.is_empty() {
                    result.push(row.clone());
                }
            }
            Ok(result)
        }
    }
}

/// Check if a node has a property with the expected value.
fn check_property(_edge_store: &EdgeStore, _node_id: &str, _field: &str, _expected: &str) -> bool {
    // Property lookups require the sparse engine (document store).
    // For MATCH patterns, primary filtering is structural (edge traversal).
    // Property predicates will be pushed down when sparse engine is wired.
    true
}

/// Project RETURN columns from rows.
pub(super) fn project_columns(rows: &[BindingRow], columns: &[ReturnColumn]) -> Vec<BindingRow> {
    rows.iter()
        .map(|row| {
            let mut projected = BindingRow::new();
            for col in columns {
                let key = col.alias.as_deref().unwrap_or(&col.expr);

                let value = if let Some(dot) = col.expr.find('.') {
                    let binding = &col.expr[..dot];
                    row.get(binding)
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string())
                } else {
                    row.get(&col.expr)
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string())
                };

                projected.insert(key.to_string(), value);
            }
            projected
        })
        .collect()
}
