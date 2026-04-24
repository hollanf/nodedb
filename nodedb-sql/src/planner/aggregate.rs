//! GROUP BY and aggregate planning.

use sqlparser::ast::{self, GroupByExpr};

use crate::engine_rules::{self, AggregateParams};
use crate::error::Result;
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::resolver::columns::ResolvedTable;
use crate::resolver::expr::convert_expr;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Plan an aggregate query (GROUP BY + aggregate functions).
pub fn plan_aggregate(
    select: &ast::Select,
    table: &ResolvedTable,
    filters: &[Filter],
    _scope: &crate::resolver::columns::TableScope,
    functions: &FunctionRegistry,
    temporal: &TemporalScope,
) -> Result<SqlPlan> {
    let group_by_exprs = convert_group_by(&select.group_by)?;
    let aggregates = extract_aggregates_from_projection(&select.projection, functions)?;
    let having = match &select.having {
        Some(expr) => super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    // Extract timeseries-specific params (bucket interval, group columns) if applicable.
    let (bucket_interval_ms, group_columns) =
        extract_timeseries_params(&select.group_by, &select.projection, functions)?;

    let rules = engine_rules::resolve_engine_rules(table.info.engine);
    rules.plan_aggregate(AggregateParams {
        collection: table.name.clone(),
        alias: table.alias.clone(),
        filters: filters.to_vec(),
        group_by: group_by_exprs,
        aggregates,
        having,
        limit: 10000,
        bucket_interval_ms,
        group_columns,
        has_auto_tier: table.info.has_auto_tier,
        bitemporal: table.info.bitemporal,
        temporal: *temporal,
    })
}

/// Extract timeseries-specific parameters from GROUP BY (bucket interval, group columns).
///
/// Returns `(Some(interval_ms), group_columns)` if a `time_bucket()` call is found,
/// or `(None, empty)` otherwise. Non-timeseries engines ignore these values.
fn extract_timeseries_params(
    raw_group_by: &GroupByExpr,
    select_items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<(Option<i64>, Vec<String>)> {
    let mut bucket_interval_ms: Option<i64> = None;
    let mut group_columns = Vec::new();

    if let GroupByExpr::Expressions(exprs, _) = raw_group_by {
        for expr in exprs {
            let resolved = resolve_group_by_expr(expr, select_items);
            let check_expr = resolved.unwrap_or(expr);

            if let Some(interval) = try_extract_time_bucket(check_expr, functions)? {
                bucket_interval_ms = Some(interval);
                continue;
            }

            if let ast::Expr::Identifier(ident) = expr {
                group_columns.push(normalize_ident(ident));
            }
        }
    }

    Ok((bucket_interval_ms, group_columns))
}

/// Check if an expression is a time_bucket() call and extract the interval.
fn try_extract_time_bucket(expr: &ast::Expr, functions: &FunctionRegistry) -> Result<Option<i64>> {
    if let ast::Expr::Function(func) = expr {
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
        if functions.search_trigger(&name) == SearchTrigger::TimeBucket {
            return Ok(Some(extract_bucket_interval(func)?));
        }
    }
    Ok(None)
}

/// Resolve a GROUP BY expression that references a SELECT alias or ordinal.
///
/// `GROUP BY b` where `b` is an alias → returns the aliased expression.
/// `GROUP BY 1` → returns the 1st SELECT expression (0-indexed).
fn resolve_group_by_expr<'a>(
    expr: &ast::Expr,
    select_items: &'a [ast::SelectItem],
) -> Option<&'a ast::Expr> {
    match expr {
        ast::Expr::Identifier(ident) => {
            let alias_name = normalize_ident(ident);
            select_items.iter().find_map(|item| {
                if let ast::SelectItem::ExprWithAlias { expr, alias } = item
                    && normalize_ident(alias) == alias_name
                {
                    Some(expr)
                } else {
                    None
                }
            })
        }
        ast::Expr::Value(v) => {
            if let ast::Value::Number(n, _) = &v.value
                && let Ok(idx) = n.parse::<usize>()
                && idx >= 1
                && idx <= select_items.len()
            {
                match &select_items[idx - 1] {
                    ast::SelectItem::UnnamedExpr(e) => Some(e),
                    ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract the bucket interval from a time_bucket() call.
///
/// Handles both argument orders:
/// - `time_bucket('1 hour', timestamp)` — interval first
/// - `time_bucket(timestamp, '1 hour')` — timestamp first
/// - `time_bucket(3600, timestamp)` — integer seconds
fn extract_bucket_interval(func: &ast::Function) -> Result<i64> {
    let args = match &func.args {
        ast::FunctionArguments::List(args) => &args.args,
        _ => return Ok(0),
    };
    // Try each argument position for the interval literal.
    for arg in args {
        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(v))) = arg {
            match &v.value {
                ast::Value::SingleQuotedString(s) => {
                    let ms = parse_interval_to_ms(s);
                    if ms > 0 {
                        return Ok(ms);
                    }
                }
                ast::Value::Number(n, _) => {
                    if let Ok(secs) = n.parse::<i64>()
                        && secs > 0
                    {
                        return Ok(secs * 1000);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(0)
}

/// Parse an interval string to milliseconds.
///
/// Delegates to the canonical `nodedb_types::kv_parsing::parse_interval_to_ms`.
fn parse_interval_to_ms(s: &str) -> i64 {
    nodedb_types::kv_parsing::parse_interval_to_ms(s)
        .map(|ms| ms as i64)
        .unwrap_or(0)
}

/// Convert GROUP BY clause to SqlExpr list.
pub fn convert_group_by(group_by: &GroupByExpr) -> Result<Vec<SqlExpr>> {
    match group_by {
        GroupByExpr::All(_) => Ok(Vec::new()),
        GroupByExpr::Expressions(exprs, _) => exprs.iter().map(convert_expr).collect(),
    }
}

/// Extract aggregate expressions from SELECT projection.
pub fn extract_aggregates_from_projection(
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<Vec<AggregateExpr>> {
    let mut aggregates = Vec::new();
    for item in items {
        let (expr, alias): (&ast::Expr, String) = match item {
            ast::SelectItem::UnnamedExpr(expr) => (expr, format!("{expr}")),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        let mut extracted = crate::aggregate_walk::extract_aggregates(expr, &alias, functions)?;
        aggregates.append(&mut extracted);
    }
    Ok(aggregates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_intervals() {
        assert_eq!(parse_interval_to_ms("1h"), 3_600_000);
        assert_eq!(parse_interval_to_ms("15m"), 900_000);
        assert_eq!(parse_interval_to_ms("30s"), 30_000);
        assert_eq!(parse_interval_to_ms("7d"), 604_800_000);
        // Word-form intervals.
        assert_eq!(parse_interval_to_ms("1 hour"), 3_600_000);
        assert_eq!(parse_interval_to_ms("2 hours"), 7_200_000);
        assert_eq!(parse_interval_to_ms("15 minutes"), 900_000);
        assert_eq!(parse_interval_to_ms("30 seconds"), 30_000);
        assert_eq!(parse_interval_to_ms("1 day"), 86_400_000);
        assert_eq!(parse_interval_to_ms("5 min"), 300_000);
    }

    /// Helper: parse a SQL SELECT and return the select body + projection.
    fn parse_select(sql: &str) -> ast::Select {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            ast::Statement::Query(q) => match *q.body {
                ast::SetExpr::Select(s) => *s,
                _ => panic!("expected SELECT"),
            },
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn resolve_group_by_alias_to_time_bucket() {
        let select = parse_select(
            "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM t GROUP BY b",
        );
        let functions = FunctionRegistry::new();

        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            let resolved = resolve_group_by_expr(&exprs[0], &select.projection);
            assert!(resolved.is_some(), "alias 'b' should resolve");
            let interval = try_extract_time_bucket(resolved.unwrap(), &functions).unwrap();
            assert_eq!(interval, Some(3_600_000));
        } else {
            panic!("expected GROUP BY expressions");
        }
    }

    #[test]
    fn resolve_group_by_ordinal_to_time_bucket() {
        let select =
            parse_select("SELECT time_bucket('5 minutes', timestamp), COUNT(*) FROM t GROUP BY 1");
        let functions = FunctionRegistry::new();

        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            let resolved = resolve_group_by_expr(&exprs[0], &select.projection);
            assert!(resolved.is_some(), "ordinal 1 should resolve");
            let interval = try_extract_time_bucket(resolved.unwrap(), &functions).unwrap();
            assert_eq!(interval, Some(300_000));
        } else {
            panic!("expected GROUP BY expressions");
        }
    }

    #[test]
    fn resolve_group_by_plain_column_not_time_bucket() {
        let select = parse_select("SELECT qtype, COUNT(*) FROM t GROUP BY qtype");
        let functions = FunctionRegistry::new();

        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            let resolved = resolve_group_by_expr(&exprs[0], &select.projection);
            // 'qtype' is not an alias in SELECT, so resolve returns None.
            assert!(resolved.is_none());
            let interval = try_extract_time_bucket(&exprs[0], &functions).unwrap();
            assert_eq!(interval, None);
        } else {
            panic!("expected GROUP BY expressions");
        }
    }
}
