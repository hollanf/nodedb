//! Maintenance NDARRAY_* functions: bare `SELECT NDARRAY_FLUSH(name)` /
//! `SELECT NDARRAY_COMPACT(name)` with no FROM clause.

use sqlparser::ast;

use super::helpers::{collect_args, require_array_name};
use crate::error::Result;
use crate::types::{SqlCatalog, SqlPlan};

/// Try to intercept a no-FROM `SELECT ndarray_flush(name)` /
/// `SELECT ndarray_compact(name)`. The single projection item must be
/// a bare function call carrying one string-literal argument.
pub fn try_plan_array_maint_fn(
    items: &[ast::SelectItem],
    catalog: &dyn SqlCatalog,
) -> Result<Option<SqlPlan>> {
    if items.len() != 1 {
        return Ok(None);
    }
    let func = match &items[0] {
        ast::SelectItem::UnnamedExpr(ast::Expr::Function(f))
        | ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::Function(f),
            ..
        } => f,
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name_checked(&func.name)?;
    let arg_exprs = match &func.args {
        ast::FunctionArguments::List(list) => collect_args(&list.args),
        _ => Vec::new(),
    };
    match fn_name.as_str() {
        "ndarray_flush" => {
            let name = require_array_name(&arg_exprs, 0, "NDARRAY_FLUSH", catalog)?;
            Ok(Some(SqlPlan::NdArrayFlush { name }))
        }
        "ndarray_compact" => {
            let name = require_array_name(&arg_exprs, 0, "NDARRAY_COMPACT", catalog)?;
            Ok(Some(SqlPlan::NdArrayCompact { name }))
        }
        _ => Ok(None),
    }
}
