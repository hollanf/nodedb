use sqlparser::ast::{Expr, SelectItem, Statement, Value};

use crate::error::SqlError;
use crate::parser::statement::parse_sql;
use crate::resolver::expr::convert::convert_expr;
use crate::resolver::expr::value::{convert_value, parse_interval_to_micros};
use crate::types::*;

/// Extract the first SELECT item expression from a simple `SELECT <expr> FROM <tbl>`.
fn first_select_expr(sql: &str) -> Expr {
    let stmts = parse_sql(sql).expect("parse failed");
    let Statement::Query(q) = &stmts[0] else {
        panic!("expected query");
    };
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("expected select body");
    };
    match &sel.projection[0] {
        SelectItem::UnnamedExpr(e) => e.clone(),
        SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
        other => panic!("unexpected projection item: {other:?}"),
    }
}

#[test]
fn compound_identifier_two_parts_is_column() {
    let expr = first_select_expr("SELECT t.col FROM t");
    let result = convert_expr(&expr).expect("should succeed");
    match result {
        SqlExpr::Column {
            table: Some(t),
            name,
        } => {
            assert_eq!(t, "t");
            assert_eq!(name, "col");
        }
        other => panic!("expected Column with table, got {other:?}"),
    }
}

#[test]
fn compound_identifier_three_parts_rejected() {
    // schema.table.col — should be rejected.
    use sqlparser::ast::Ident;
    let parts = vec![Ident::new("schema"), Ident::new("table"), Ident::new("col")];
    let expr = Expr::CompoundIdentifier(parts);
    let err = convert_expr(&expr).unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported { .. }),
        "expected Unsupported, got {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("schema.table.col") || msg.contains("schema-qualified"),
        "error should mention the qualified name: {msg}"
    );
}

#[test]
fn compound_identifier_four_parts_rejected() {
    use sqlparser::ast::Ident;
    let parts = vec![
        Ident::new("a"),
        Ident::new("b"),
        Ident::new("c"),
        Ident::new("d"),
    ];
    let expr = Expr::CompoundIdentifier(parts);
    let err = convert_expr(&expr).unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported { .. }),
        "expected Unsupported, got {err:?}"
    );
}

/// `"userId"` with the PostgreSQL dialect is an identifier (quoted,
/// case-preserved), not a string literal.
#[test]
fn double_quoted_is_identifier_not_literal() {
    let expr = first_select_expr(r#"SELECT "userId" FROM users"#);
    match expr {
        Expr::Identifier(ident) => {
            assert_eq!(ident.value, "userId");
            assert_eq!(ident.quote_style, Some('"'));
        }
        other => panic!("expected Identifier, got {other:?}"),
    }
}

/// `'userId'` is a single-quoted string literal.
#[test]
fn single_quoted_is_string_literal() {
    let expr = first_select_expr("SELECT 'userId' FROM users");
    match &expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => assert_eq!(s, "userId"),
            other => panic!("expected SingleQuotedString, got {other:?}"),
        },
        other => panic!("expected Value, got {other:?}"),
    }
    // And convert_value maps it to SqlValue::String.
    let Expr::Value(v) = expr else { unreachable!() };
    assert!(matches!(
        convert_value(&v.value),
        Ok(SqlValue::String(s)) if s == "userId"
    ));
}

/// `Value::DoubleQuotedString` (non-Postgres dialect) falls through
/// `convert_value` to `SqlError::Unsupported`. With PostgreSQL dialect
/// this variant is never produced, but constructing it directly verifies
/// the arm is absent and not silently accepted.
#[test]
fn double_quoted_string_value_unsupported() {
    // Construct the variant directly — it cannot be produced by parsing
    // with PostgreSqlDialect, which is exactly why the arm was dead code.
    let val = Value::DoubleQuotedString("userId".into());
    assert!(
        matches!(convert_value(&val), Err(SqlError::Unsupported { .. })),
        "DoubleQuotedString should be Unsupported, not silently accepted"
    );
}

/// `"col" = 'literal'` — double-quoted identifier on the left, single-quoted
/// string literal on the right — must lower to `BinaryOp(Column("col"), Eq,
/// Literal(String("literal")))`.  This is the canonical mixed-quotation form
/// used in WHERE clauses (e.g. WHERE "userId" = 'alice').
#[test]
fn double_quoted_col_eq_single_quoted_literal() {
    let expr = where_sql_expr(r#"SELECT * FROM t WHERE "col" = 'literal'"#);
    match expr {
        SqlExpr::BinaryOp { left, right, .. } => {
            assert!(
                matches!(*left, SqlExpr::Column { ref name, .. } if name == "col"),
                "left should be Column(col), got {left:?}"
            );
            assert!(
                matches!(*right, SqlExpr::Literal(SqlValue::String(ref s)) if s == "literal"),
                "right should be Literal(String(\"literal\")), got {right:?}"
            );
        }
        other => panic!("expected BinaryOp, got {other:?}"),
    }
}

/// `"colA" = "colB"` — both sides are double-quoted identifiers; both must
/// resolve as column references, not string literals.
#[test]
fn double_quoted_col_eq_double_quoted_col() {
    let expr = where_sql_expr(r#"SELECT * FROM t WHERE "colA" = "colB""#);
    match expr {
        SqlExpr::BinaryOp { left, right, .. } => {
            assert!(
                matches!(*left, SqlExpr::Column { ref name, .. } if name == "colA"),
                "left should be Column(colA), got {left:?}"
            );
            assert!(
                matches!(*right, SqlExpr::Column { ref name, .. } if name == "colB"),
                "right should be Column(colB), got {right:?}"
            );
        }
        other => panic!("expected BinaryOp, got {other:?}"),
    }
}

/// A double-quoted identifier in the SELECT list resolves as `SqlExpr::Column`
/// with the exact case preserved (not lowercased, because it was quoted).
#[test]
fn double_quoted_select_col_case_preserved() {
    let expr = first_select_expr(r#"SELECT "userId" FROM users"#);
    let sql_expr = convert_expr(&expr).expect("convert_expr should succeed");
    match sql_expr {
        SqlExpr::Column { name, table } => {
            assert_eq!(
                name, "userId",
                "case must be preserved for quoted identifier"
            );
            assert_eq!(table, None, "no table qualifier expected");
        }
        other => panic!("expected Column, got {other:?}"),
    }
}

#[test]
fn parse_interval_sql_word_forms() {
    assert_eq!(parse_interval_to_micros("1 hour"), Some(3_600_000_000));
    assert_eq!(parse_interval_to_micros("5 days"), Some(5 * 86_400_000_000));
    assert_eq!(
        parse_interval_to_micros("30 minutes"),
        Some(30 * 60_000_000)
    );
    assert_eq!(
        parse_interval_to_micros("2 hours 30 minutes"),
        Some(9_000_000_000)
    );
    assert_eq!(parse_interval_to_micros("1 week"), Some(604_800_000_000));
    assert_eq!(parse_interval_to_micros("100 milliseconds"), Some(100_000));
}

#[test]
fn parse_interval_shorthand() {
    assert_eq!(parse_interval_to_micros("1h"), Some(3_600_000_000));
    assert_eq!(parse_interval_to_micros("30m"), Some(30 * 60_000_000));
    assert_eq!(parse_interval_to_micros("1h30m"), Some(5_400_000_000));
    assert_eq!(parse_interval_to_micros("500ms"), Some(500_000));
}

#[test]
fn parse_interval_invalid() {
    assert_eq!(parse_interval_to_micros(""), None);
    assert_eq!(parse_interval_to_micros("abc"), None);
}

/// Extract and convert the WHERE predicate from a simple
/// `SELECT * FROM tbl WHERE <expr>` statement.
fn where_sql_expr(sql: &str) -> SqlExpr {
    let stmts = parse_sql(sql).expect("parse failed");
    let Statement::Query(q) = &stmts[0] else {
        panic!("expected query");
    };
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("expected select body");
    };
    let raw = sel.selection.as_ref().expect("expected WHERE clause");
    convert_expr(raw).expect("convert_expr failed")
}

#[test]
fn like_is_case_sensitive() {
    let expr = where_sql_expr("SELECT * FROM t WHERE name LIKE 'foo%'");
    match expr {
        SqlExpr::Like {
            negated,
            case_insensitive,
            ..
        } => {
            assert!(!negated, "LIKE should not be negated");
            assert!(!case_insensitive, "LIKE should be case-sensitive");
        }
        other => panic!("expected SqlExpr::Like, got {other:?}"),
    }
}

#[test]
fn ilike_is_case_insensitive() {
    let expr = where_sql_expr("SELECT * FROM t WHERE name ILIKE 'foo%'");
    match expr {
        SqlExpr::Like {
            negated,
            case_insensitive,
            ..
        } => {
            assert!(!negated, "ILIKE should not be negated");
            assert!(case_insensitive, "ILIKE should be case-insensitive");
        }
        other => panic!("expected SqlExpr::Like, got {other:?}"),
    }
}

#[test]
fn not_like_is_negated_case_sensitive() {
    let expr = where_sql_expr("SELECT * FROM t WHERE name NOT LIKE 'foo%'");
    match expr {
        SqlExpr::Like {
            negated,
            case_insensitive,
            ..
        } => {
            assert!(negated, "NOT LIKE should be negated");
            assert!(!case_insensitive, "NOT LIKE should be case-sensitive");
        }
        other => panic!("expected SqlExpr::Like, got {other:?}"),
    }
}

#[test]
fn not_ilike_is_negated_case_insensitive() {
    let expr = where_sql_expr("SELECT * FROM t WHERE name NOT ILIKE 'foo%'");
    match expr {
        SqlExpr::Like {
            negated,
            case_insensitive,
            ..
        } => {
            assert!(negated, "NOT ILIKE should be negated");
            assert!(case_insensitive, "NOT ILIKE should be case-insensitive");
        }
        other => panic!("expected SqlExpr::Like, got {other:?}"),
    }
}

// ── JSON operator lowering tests ───────────────────────────────────────

/// Parses `SELECT <expr> FROM t` and returns the lowered `SqlExpr` for `<expr>`.
fn select_expr_lowered(sql: &str) -> SqlExpr {
    let stmts = parse_sql(sql).expect("parse failed");
    let Statement::Query(q) = &stmts[0] else {
        panic!("expected query");
    };
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("expected select body");
    };
    let raw = &sel.projection[0];
    let raw_expr = match raw {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        other => panic!("unexpected projection: {other:?}"),
    };
    convert_expr(raw_expr).expect("convert_expr failed")
}

fn assert_json_fn(sql: &str, expected_fn: &str) {
    let expr = select_expr_lowered(sql);
    match expr {
        SqlExpr::Function { name, args, .. } => {
            assert_eq!(name, expected_fn, "wrong function name");
            assert_eq!(args.len(), 2, "expected 2 args");
        }
        other => panic!("expected Function, got {other:?}"),
    }
}

#[test]
fn arrow_lowers_to_pg_json_get() {
    assert_json_fn("SELECT data->'key' FROM t", "pg_json_get");
}

#[test]
fn long_arrow_lowers_to_pg_json_get_text() {
    assert_json_fn("SELECT data->>'key' FROM t", "pg_json_get_text");
}

#[test]
fn hash_arrow_lowers_to_pg_json_path_get() {
    assert_json_fn("SELECT data#>'{a,b}' FROM t", "pg_json_path_get");
}

#[test]
fn hash_long_arrow_lowers_to_pg_json_path_get_text() {
    assert_json_fn("SELECT data#>>'{a,b}' FROM t", "pg_json_path_get_text");
}

#[test]
fn at_arrow_lowers_to_pg_json_contains() {
    assert_json_fn("SELECT data @> '{\"a\":1}' FROM t", "pg_json_contains");
}

#[test]
fn arrow_at_lowers_to_pg_json_contained_by() {
    assert_json_fn("SELECT '{\"a\":1}' <@ data FROM t", "pg_json_contained_by");
}

#[test]
fn question_lowers_to_pg_json_has_key() {
    assert_json_fn("SELECT data ? 'key' FROM t", "pg_json_has_key");
}

#[test]
fn question_and_lowers_to_pg_json_has_all_keys() {
    assert_json_fn(
        "SELECT data ?& ARRAY['a','b'] FROM t",
        "pg_json_has_all_keys",
    );
}

#[test]
fn question_pipe_lowers_to_pg_json_has_any_key() {
    assert_json_fn(
        "SELECT data ?| ARRAY['a','b'] FROM t",
        "pg_json_has_any_key",
    );
}

#[test]
fn chained_arrow_lowers_nested() {
    // data->'a'->'b' → pg_json_get(pg_json_get(data, 'a'), 'b')
    let expr = select_expr_lowered("SELECT data->'a'->'b' FROM t");
    match expr {
        SqlExpr::Function { name, ref args, .. } => {
            assert_eq!(name, "pg_json_get", "outer fn should be pg_json_get");
            match &args[0] {
                SqlExpr::Function {
                    name: inner_name, ..
                } => {
                    assert_eq!(inner_name, "pg_json_get", "inner fn should be pg_json_get");
                }
                other => panic!("expected inner pg_json_get, got {other:?}"),
            }
        }
        other => panic!("expected outer pg_json_get, got {other:?}"),
    }
}

// ── FTS operator / function lowering tests ────────────────────────────────

fn where_fn(sql: &str) -> SqlExpr {
    where_sql_expr(sql)
}

#[test]
fn at_at_lowers_to_pg_fts_match() {
    // col @@ to_tsquery('rust & lang') → pg_fts_match(col, pg_to_tsquery('rust & lang'))
    let expr = where_fn("SELECT * FROM t WHERE body @@ to_tsquery('rust & lang')");
    match expr {
        SqlExpr::Function {
            ref name, ref args, ..
        } => {
            assert_eq!(
                name, "pg_fts_match",
                "operator @@ should lower to pg_fts_match"
            );
            assert_eq!(args.len(), 2, "expected 2 args");
            match &args[1] {
                SqlExpr::Function { name: inner, .. } => {
                    assert_eq!(inner, "pg_to_tsquery");
                }
                other => panic!("expected pg_to_tsquery as right arg, got {other:?}"),
            }
        }
        other => panic!("expected pg_fts_match Function, got {other:?}"),
    }
}

#[test]
fn at_at_with_plainto_tsquery() {
    // col @@ plainto_tsquery('rust lang') → pg_fts_match(col, pg_plainto_tsquery(...))
    let expr = where_fn("SELECT * FROM t WHERE body @@ plainto_tsquery('rust lang')");
    match expr {
        SqlExpr::Function {
            ref name, ref args, ..
        } => {
            assert_eq!(name, "pg_fts_match");
            match &args[1] {
                SqlExpr::Function { name: inner, .. } => {
                    assert_eq!(inner, "pg_plainto_tsquery");
                }
                other => panic!("expected pg_plainto_tsquery, got {other:?}"),
            }
        }
        other => panic!("expected pg_fts_match, got {other:?}"),
    }
}

#[test]
fn tsvector_cast_elided() {
    // 'foo'::tsvector → Literal("foo")
    let expr = select_expr_lowered("SELECT 'foo'::tsvector FROM t");
    assert!(
        matches!(expr, SqlExpr::Literal(SqlValue::String(ref s)) if s == "foo"),
        "expected Literal(\"foo\"), got {expr:?}"
    );
}

#[test]
fn tsquery_cast_elided() {
    // 'rust'::tsquery → Literal("rust")
    let expr = select_expr_lowered("SELECT 'rust'::tsquery FROM t");
    assert!(
        matches!(expr, SqlExpr::Literal(SqlValue::String(ref s)) if s == "rust"),
        "expected Literal(\"rust\"), got {expr:?}"
    );
}

#[test]
fn ts_rank_cd_is_unsupported() {
    use crate::parser::statement::parse_sql;
    let sql = "SELECT ts_rank_cd(body, to_tsquery('rust')) FROM t";
    let stmts = parse_sql(sql).expect("parse ok");
    let Statement::Query(q) = &stmts[0] else {
        panic!("expected query");
    };
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("expected select body");
    };
    let raw = match &sel.projection[0] {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        other => panic!("unexpected projection: {other:?}"),
    };
    let err = convert_expr(raw).unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported { .. }),
        "ts_rank_cd should be Unsupported, got {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("ts_rank_cd"),
        "error should mention ts_rank_cd: {msg}"
    );
}

#[test]
fn to_tsquery_lowers_to_pg_to_tsquery() {
    let expr = select_expr_lowered("SELECT to_tsquery('rust & lang') FROM t");
    match expr {
        SqlExpr::Function { ref name, .. } => {
            assert_eq!(name, "pg_to_tsquery");
        }
        other => panic!("expected pg_to_tsquery Function, got {other:?}"),
    }
}

#[test]
fn plainto_tsquery_lowers_correctly() {
    let expr = select_expr_lowered("SELECT plainto_tsquery('rust lang') FROM t");
    match expr {
        SqlExpr::Function { ref name, .. } => {
            assert_eq!(name, "pg_plainto_tsquery");
        }
        other => panic!("expected pg_plainto_tsquery, got {other:?}"),
    }
}

#[test]
fn ts_rank_lowers_to_pg_ts_rank() {
    let expr = select_expr_lowered("SELECT ts_rank(body, to_tsquery('rust')) FROM t");
    match expr {
        SqlExpr::Function { ref name, .. } => {
            assert_eq!(name, "pg_ts_rank");
        }
        other => panic!("expected pg_ts_rank, got {other:?}"),
    }
}
