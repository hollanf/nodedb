//! Shared helpers for SELECT planning: projection conversion, WHERE
//! filter conversion, and AST literal extraction utilities.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Convert SELECT projection items.
pub fn convert_projection(items: &[ast::SelectItem]) -> Result<Vec<Projection>> {
    let mut result = Vec::new();
    for item in items {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                let sql_expr = convert_expr(expr)?;
                match &sql_expr {
                    SqlExpr::Column { table, name } => {
                        result.push(Projection::Column(qualified_name(table.as_deref(), name)));
                    }
                    SqlExpr::Wildcard => {
                        result.push(Projection::Star);
                    }
                    _ => {
                        result.push(Projection::Computed {
                            expr: sql_expr,
                            alias: format!("{expr}"),
                        });
                    }
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                let sql_expr = convert_expr(expr)?;
                result.push(Projection::Computed {
                    expr: sql_expr,
                    alias: normalize_ident(alias),
                });
            }
            ast::SelectItem::Wildcard(_) => {
                result.push(Projection::Star);
            }
            ast::SelectItem::QualifiedWildcard(kind, _) => {
                let table_name = match kind {
                    ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        crate::parser::normalize::normalize_object_name(name)
                    }
                    _ => String::new(),
                };
                result.push(Projection::QualifiedStar(table_name));
            }
        }
    }
    Ok(result)
}

/// Build a qualified column reference (`table.name` or just `name`).
pub fn qualified_name(table: Option<&str>, name: &str) -> String {
    table.map_or_else(|| name.to_string(), |table| format!("{table}.{name}"))
}

/// Convert a WHERE expression into a list of Filter.
pub fn convert_where_to_filters(expr: &ast::Expr) -> Result<Vec<Filter>> {
    let sql_expr = convert_expr(expr)?;
    Ok(vec![Filter {
        expr: FilterExpr::Expr(sql_expr),
    }])
}

pub fn extract_func_args(func: &ast::Function) -> Result<Vec<ast::Expr>> {
    match &func.args {
        ast::FunctionArguments::List(args) => Ok(args
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
                _ => None,
            })
            .collect()),
        _ => Ok(Vec::new()),
    }
}

/// Evaluate a constant SqlExpr to a SqlValue. Delegates to the shared
/// `const_fold::fold_constant` helper so that zero-arg scalar functions
/// like `now()` and `current_timestamp` go through the same evaluator
/// as the runtime expression path.
pub(super) fn eval_constant_expr(expr: &SqlExpr, functions: &FunctionRegistry) -> SqlValue {
    crate::planner::const_fold::fold_constant(expr, functions).unwrap_or(SqlValue::Null)
}

/// Extract a geometry argument: handles ST_Point(lon, lat), ST_GeomFromGeoJSON('...'),
/// or a raw string literal containing GeoJSON.
pub(super) fn extract_geometry_arg(expr: &ast::Expr) -> Result<String> {
    match expr {
        // ST_Point(lon, lat) → GeoJSON Point
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
            let args = extract_func_args(func)?;
            match name.as_str() {
                "st_point" if args.len() >= 2 => {
                    let lon = extract_float(&args[0])?;
                    let lat = extract_float(&args[1])?;
                    Ok(format!(r#"{{"type":"Point","coordinates":[{lon},{lat}]}}"#))
                }
                "st_geomfromgeojson" if !args.is_empty() => extract_string_literal(&args[0]),
                _ => Ok(format!("{expr}")),
            }
        }
        // Raw string literal: assumed to be GeoJSON.
        _ => extract_string_literal(expr).or_else(|_| Ok(format!("{expr}"))),
    }
}

pub(super) fn extract_column_name(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        ast::Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(normalize_ident)
            .collect::<Vec<_>>()
            .join(".")),
        _ => Err(SqlError::Unsupported {
            detail: format!("expected column name, got: {expr}"),
        }),
    }
}

pub fn extract_string_literal(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            _ => Err(SqlError::Unsupported {
                detail: format!("expected string literal, got: {expr}"),
            }),
        },
        _ => Err(SqlError::Unsupported {
            detail: format!("expected string literal, got: {expr}"),
        }),
    }
}

pub fn extract_float(expr: &ast::Expr) -> Result<f64> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("expected number: {n}"),
            }),
            _ => Err(SqlError::TypeMismatch {
                detail: format!("expected number, got: {expr}"),
            }),
        },
        // Handle negative numbers: -73.9855 is parsed as UnaryOp { Minus, 73.9855 }
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => extract_float(inner).map(|f| -f),
        _ => Err(SqlError::TypeMismatch {
            detail: format!("expected number, got: {expr}"),
        }),
    }
}

/// Extract a float array from ARRAY[...] or make_array(...) expression.
pub(super) fn extract_float_array(expr: &ast::Expr) -> Result<Vec<f32>> {
    match expr {
        ast::Expr::Array(ast::Array { elem, .. }) => elem
            .iter()
            .map(|e| extract_float(e).map(|f| f as f32))
            .collect(),
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
            if name == "make_array" || name == "array" {
                let args = extract_func_args(func)?;
                args.iter()
                    .map(|e| extract_float(e).map(|f| f as f32))
                    .collect()
            } else {
                Err(SqlError::Unsupported {
                    detail: format!("expected array, got function: {name}"),
                })
            }
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("expected array literal, got: {expr}"),
        }),
    }
}
