//! Search-trigger detection in WHERE clauses (text match, spatial predicates).

use sqlparser::ast;

use super::helpers::{
    extract_column_name, extract_float, extract_func_args, extract_geometry_arg,
    extract_string_literal,
};
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::types::*;

/// Try to detect search-triggering patterns in WHERE clause.
pub(super) fn try_extract_where_search(
    expr: &ast::Expr,
    table: &crate::resolver::columns::ResolvedTable,
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    match expr {
        ast::Expr::Function(func) => {
            let name = func
                .name
                .0
                .iter()
                .map(|p| match p {
                    ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                    _ => String::new(),
                })
                .collect::<Vec<_>>()
                .join(".");
            match functions.search_trigger(&name) {
                SearchTrigger::TextMatch => {
                    let args = extract_func_args(func)?;
                    if args.len() >= 2 {
                        let query_text = extract_string_literal(&args[1])?;
                        return Ok(Some(SqlPlan::TextSearch {
                            collection: table.name.clone(),
                            query: crate::fts_types::FtsQuery::Plain {
                                text: query_text,
                                fuzzy: true,
                            },
                            top_k: 1000,
                            filters: Vec::new(),
                        }));
                    }
                }
                SearchTrigger::SpatialDWithin
                | SearchTrigger::SpatialContains
                | SearchTrigger::SpatialIntersects
                | SearchTrigger::SpatialWithin => {
                    return plan_spatial_from_where(&name, func, table);
                }
                _ => {}
            }
        }
        // AND: check left and right for search triggers, combine non-search as filters.
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            if let Some(plan) = try_extract_where_search(left, table, functions)? {
                return Ok(Some(plan));
            }
            if let Some(plan) = try_extract_where_search(right, table, functions)? {
                return Ok(Some(plan));
            }
        }
        _ => {}
    }
    Ok(None)
}

fn plan_spatial_from_where(
    name: &str,
    func: &ast::Function,
    table: &crate::resolver::columns::ResolvedTable,
) -> Result<Option<SqlPlan>> {
    let predicate = match name {
        "st_dwithin" => SpatialPredicate::DWithin,
        "st_contains" => SpatialPredicate::Contains,
        "st_intersects" => SpatialPredicate::Intersects,
        "st_within" => SpatialPredicate::Within,
        _ => return Ok(None),
    };
    let args = extract_func_args(func)?;
    if args.is_empty() {
        return Err(SqlError::MissingField {
            field: "geometry column".into(),
            context: name.into(),
        });
    }
    let field = extract_column_name(&args[0])?;
    let geom_arg = args.get(1).ok_or_else(|| SqlError::MissingField {
        field: "query geometry".into(),
        context: name.into(),
    })?;
    let geom_str = extract_geometry_arg(geom_arg)?;
    let geometry: nodedb_types::geometry::Geometry =
        sonic_rs::from_str(&geom_str).map_err(|e| SqlError::InvalidFunction {
            detail: format!("invalid geometry in {name}: {e}"),
        })?;
    let issues = nodedb_spatial::validate::validate_geometry(&geometry);
    if !issues.is_empty() {
        return Err(SqlError::InvalidFunction {
            detail: format!("invalid geometry in {name}: {}", issues.join("; ")),
        });
    }
    let distance = if args.len() >= 3 {
        extract_float(&args[2]).unwrap_or(0.0)
    } else {
        0.0
    };
    Ok(Some(SqlPlan::SpatialScan {
        collection: table.name.clone(),
        field,
        predicate,
        query_geometry: geometry,
        distance_meters: distance,
        attribute_filters: Vec::new(),
        limit: 1000,
        projection: Vec::new(),
    }))
}
