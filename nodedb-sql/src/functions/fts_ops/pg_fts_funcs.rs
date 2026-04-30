//! Lowering helpers for PG full-text search surface operators.
//!
//! These functions are called from `resolver/expr.rs` to lower PG FTS
//! operators/casts to internal `SqlExpr::Function` nodes before the generic
//! expression lowering path runs.

use crate::types_expr::SqlExpr;

/// Lower `col @@ query_expr` to `pg_fts_match(col, query_expr)`.
pub fn lower_pg_fts_match(col: SqlExpr, query_expr: SqlExpr) -> SqlExpr {
    SqlExpr::Function {
        name: "pg_fts_match".into(),
        args: vec![col, query_expr],
        distinct: false,
    }
}

/// Lower `to_tsquery('...')` / `to_tsquery(lang, '...')` args.
///
/// The caller (resolver) has already converted the function args; this just
/// wraps them in a `pg_to_tsquery` function call.
pub fn lower_pg_to_tsquery(args: Vec<SqlExpr>) -> SqlExpr {
    SqlExpr::Function {
        name: "pg_to_tsquery".into(),
        args,
        distinct: false,
    }
}

/// Lower `plainto_tsquery('...')` / `plainto_tsquery(lang, '...')` args.
pub fn lower_pg_plainto_tsquery(args: Vec<SqlExpr>) -> SqlExpr {
    SqlExpr::Function {
        name: "pg_plainto_tsquery".into(),
        args,
        distinct: false,
    }
}

/// Lower `phraseto_tsquery('...')` args.
///
/// The resulting `pg_phraseto_tsquery` call is evaluated by the executor
/// to `FtsQuery::Phrase`, which is then rejected with `Unsupported`.
pub fn lower_phraseto_tsquery(args: Vec<SqlExpr>) -> SqlExpr {
    SqlExpr::Function {
        name: "pg_phraseto_tsquery".into(),
        args,
        distinct: false,
    }
}

/// Lower `websearch_to_tsquery('...')` / `websearch_to_tsquery(lang, '...')` args.
pub fn lower_pg_websearch_to_tsquery(args: Vec<SqlExpr>) -> SqlExpr {
    SqlExpr::Function {
        name: "pg_websearch_to_tsquery".into(),
        args,
        distinct: false,
    }
}
