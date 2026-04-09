//! AST-level parameter binding for prepared statements.
//!
//! Replaces `Value::Placeholder("$1")` nodes in the sqlparser AST with
//! concrete literal values, eliminating the need for SQL text substitution.

use sqlparser::ast::{
    self, Expr, GroupByExpr, Query, Select, SelectItem, SetExpr, Statement, Value,
};

/// Parameter value for AST substitution.
///
/// Converted from pgwire binary parameters + type info in the Control Plane.
#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
}

/// Substitute all `$N` placeholders in a parsed statement with concrete values.
///
/// Walks the AST and replaces every `Value::Placeholder("$N")` with the
/// corresponding literal from `params` (0-indexed: `$1` → `params[0]`).
pub fn bind_params(stmt: &mut Statement, params: &[ParamValue]) {
    if params.is_empty() {
        return;
    }
    bind_statement(stmt, params);
}

fn placeholder_to_value(placeholder: &str, params: &[ParamValue]) -> Option<Value> {
    let idx_str = placeholder.strip_prefix('$')?;
    let idx: usize = idx_str.parse().ok()?;
    let param = params.get(idx.checked_sub(1)?)?;
    Some(match param {
        ParamValue::Null => Value::Null,
        ParamValue::Bool(true) => Value::Boolean(true),
        ParamValue::Bool(false) => Value::Boolean(false),
        ParamValue::Int64(n) => Value::Number(n.to_string(), false),
        ParamValue::Float64(f) => Value::Number(f.to_string(), false),
        ParamValue::Text(s) => Value::SingleQuotedString(s.clone()),
    })
}

// ── AST walkers ─────────────────────────────────────────────────────

fn bind_statement(stmt: &mut Statement, params: &[ParamValue]) {
    match stmt {
        Statement::Query(q) => bind_query(q, params),
        Statement::Insert(ins) => {
            if let Some(ref mut src) = ins.source {
                bind_query(src, params);
            }
            if let Some(ref mut sel) = ins.returning {
                for item in sel {
                    bind_select_item(item, params);
                }
            }
        }
        Statement::Update(upd) => {
            for a in &mut upd.assignments {
                bind_expr(&mut a.value, params);
            }
            if let Some(ref mut w) = upd.selection {
                bind_expr(w, params);
            }
        }
        Statement::Delete(del) => {
            if let Some(ref mut w) = del.selection {
                bind_expr(w, params);
            }
        }
        _ => {}
    }
}

fn bind_query(query: &mut Query, params: &[ParamValue]) {
    bind_set_expr(&mut query.body, params);
    if let Some(ref mut order_by) = query.order_by
        && let ast::OrderByKind::Expressions(ref mut exprs) = order_by.kind
    {
        for item in exprs {
            bind_expr(&mut item.expr, params);
        }
    }
    if let Some(limit_clause) = &mut query.limit_clause
        && let ast::LimitClause::LimitOffset { limit, offset, .. } = limit_clause
    {
        if let Some(limit_expr) = limit {
            bind_expr(limit_expr, params);
        }
        if let Some(offset_val) = offset {
            bind_expr(&mut offset_val.value, params);
        }
    }
}

fn bind_set_expr(body: &mut SetExpr, params: &[ParamValue]) {
    match body {
        SetExpr::Select(sel) => bind_select(sel, params),
        SetExpr::Query(q) => bind_query(q, params),
        SetExpr::SetOperation { left, right, .. } => {
            bind_set_expr(left, params);
            bind_set_expr(right, params);
        }
        SetExpr::Values(vals) => {
            for row in &mut vals.rows {
                for expr in row {
                    bind_expr(expr, params);
                }
            }
        }
        _ => {}
    }
}

fn bind_select(sel: &mut Select, params: &[ParamValue]) {
    for item in &mut sel.projection {
        bind_select_item(item, params);
    }
    if let Some(ref mut w) = sel.selection {
        bind_expr(w, params);
    }
    match &mut sel.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            for e in exprs {
                bind_expr(e, params);
            }
        }
        GroupByExpr::All(_) => {}
    }
    if let Some(ref mut having) = sel.having {
        bind_expr(having, params);
    }
}

fn bind_select_item(item: &mut SelectItem, params: &[ParamValue]) {
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            bind_expr(e, params);
        }
        _ => {}
    }
}

fn bind_expr(expr: &mut Expr, params: &[ParamValue]) {
    match expr {
        Expr::Value(ast::ValueWithSpan { value, .. }) => {
            if let Value::Placeholder(p) = value
                && let Some(v) = placeholder_to_value(p, params)
            {
                *value = v;
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            bind_expr(left, params);
            bind_expr(right, params);
        }
        Expr::UnaryOp { expr: e, .. } => bind_expr(e, params),
        Expr::Nested(e) => bind_expr(e, params),
        Expr::Between {
            expr: e, low, high, ..
        } => {
            bind_expr(e, params);
            bind_expr(low, params);
            bind_expr(high, params);
        }
        Expr::InList { expr: e, list, .. } => {
            bind_expr(e, params);
            for item in list {
                bind_expr(item, params);
            }
        }
        Expr::InSubquery {
            expr: e, subquery, ..
        } => {
            bind_expr(e, params);
            bind_query(subquery, params);
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => bind_expr(e, params),
        Expr::IsFalse(e) | Expr::IsTrue(e) => bind_expr(e, params),
        Expr::IsNotFalse(e) | Expr::IsNotTrue(e) => bind_expr(e, params),
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => {
            bind_expr(e, params);
            bind_expr(pattern, params);
        }
        Expr::Cast { expr: e, .. } => {
            bind_expr(e, params);
        }
        Expr::Function(f) => {
            if let ast::FunctionArguments::List(ref mut args) = f.args {
                for arg in &mut args.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg {
                        bind_expr(e, params);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(e) = operand {
                bind_expr(e, params);
            }
            for cw in conditions {
                bind_expr(&mut cw.condition, params);
                bind_expr(&mut cw.result, params);
            }
            if let Some(e) = else_result {
                bind_expr(e, params);
            }
        }
        Expr::Exists { subquery, .. } => bind_query(subquery, params),
        Expr::Subquery(q) => bind_query(q, params),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    fn bind_and_format(sql: &str, params: &[ParamValue]) -> String {
        let mut stmts = parse_sql(sql).unwrap();
        for stmt in &mut stmts {
            bind_params(stmt, params);
        }
        stmts
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    #[test]
    fn bind_select_where() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE id = $1",
            &[ParamValue::Int64(42)],
        );
        assert!(result.contains("id = 42"), "got: {result}");
    }

    #[test]
    fn bind_string_param() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE name = $1",
            &[ParamValue::Text("alice".into())],
        );
        assert!(result.contains("name = 'alice'"), "got: {result}");
    }

    #[test]
    fn bind_null_param() {
        let result = bind_and_format("SELECT * FROM users WHERE name = $1", &[ParamValue::Null]);
        assert!(result.contains("name = NULL"), "got: {result}");
    }

    #[test]
    fn bind_multiple_params() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE age > $1 AND name = $2",
            &[ParamValue::Int64(18), ParamValue::Text("bob".into())],
        );
        assert!(result.contains("age > 18"), "got: {result}");
        assert!(result.contains("name = 'bob'"), "got: {result}");
    }

    #[test]
    fn bind_insert_values() {
        let result = bind_and_format(
            "INSERT INTO users (id, name) VALUES ($1, $2)",
            &[ParamValue::Int64(1), ParamValue::Text("eve".into())],
        );
        assert!(result.contains("1, 'eve'"), "got: {result}");
    }

    #[test]
    fn bind_bool_param() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE active = $1",
            &[ParamValue::Bool(true)],
        );
        assert!(result.contains("active = true"), "got: {result}");
    }

    #[test]
    fn no_params_noop() {
        let result = bind_and_format("SELECT 1", &[]);
        assert!(result.contains("SELECT 1"));
    }
}
