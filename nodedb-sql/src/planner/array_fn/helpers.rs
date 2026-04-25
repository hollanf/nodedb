//! Shared helpers for NDARRAY_* function planners: argument extraction
//! and validation against the array catalog.

use sqlparser::ast;

use nodedb_types::Value;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::types::SqlCatalog;
use crate::types_array::ArrayCoordLiteral;

pub(super) fn collect_args(args: &[ast::FunctionArg]) -> Vec<ast::Expr> {
    args.iter()
        .filter_map(|a| match a {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn require_array_name(
    args: &[ast::Expr],
    idx: usize,
    fn_name: &str,
    catalog: &dyn SqlCatalog,
) -> Result<String> {
    let expr = args.get(idx).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array name at arg {idx}"),
    })?;
    let name = match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident),
        ast::Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(normalize_ident)
            .collect::<Vec<_>>()
            .join("."),
        _ => expect_string_literal(expr, &format!("{fn_name} array name"))?,
    };
    if !catalog.array_exists(&name) {
        return Err(SqlError::Unsupported {
            detail: format!("{fn_name}: array '{name}' not found"),
        });
    }
    Ok(name)
}

pub(super) fn expect_string_literal(expr: &ast::Expr, ctx: &str) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            other => Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected string literal, got {other}"),
            }),
        },
        ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        _ => Err(SqlError::Unsupported {
            detail: format!("{ctx}: expected string literal, got {expr}"),
        }),
    }
}

pub(super) fn expect_u32(expr: &ast::Expr, ctx: &str) -> Result<u32> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<u32>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("{ctx}: expected u32, got {n}"),
            }),
            other => Err(SqlError::TypeMismatch {
                detail: format!("{ctx}: expected number, got {other}"),
            }),
        },
        _ => Err(SqlError::TypeMismatch {
            detail: format!("{ctx}: expected number literal, got {expr}"),
        }),
    }
}

pub(super) fn expect_string_array(expr: &ast::Expr, ctx: &str) -> Result<Vec<String>> {
    let elems = match expr {
        ast::Expr::Array(arr) => arr.elem.clone(),
        ast::Expr::Function(f)
            if matches!(
                crate::parser::normalize::normalize_object_name(&f.name).as_str(),
                "make_array" | "array"
            ) =>
        {
            match &f.args {
                ast::FunctionArguments::List(list) => collect_args(&list.args),
                _ => Vec::new(),
            }
        }
        _ => {
            return Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected array literal, got {expr}"),
            });
        }
    };
    elems
        .iter()
        .map(|e| expect_string_literal(e, ctx))
        .collect()
}

pub(super) fn is_null_literal(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Value(v) if matches!(v.value, ast::Value::Null)
    )
}

pub(super) fn value_to_coord_literal(v: &Value, dim: &str) -> Result<ArrayCoordLiteral> {
    match v {
        Value::Integer(i) => Ok(ArrayCoordLiteral::Int64(*i)),
        Value::Float(f) => Ok(ArrayCoordLiteral::Float64(*f)),
        Value::String(s) => Ok(ArrayCoordLiteral::String(s.clone())),
        other => Err(SqlError::TypeMismatch {
            detail: format!("NDARRAY_SLICE dim '{dim}' bound: unsupported value kind {other:?}"),
        }),
    }
}
