//! SQL parsing via sqlparser-rs.

use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::{Result, SqlError};

/// Parse a SQL string into one or more statements.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    if statements.is_empty() {
        return Err(SqlError::Parse {
            detail: "empty SQL statement".into(),
        });
    }
    Ok(statements)
}

/// Classify a parsed statement for routing.
#[derive(Debug)]
pub enum StatementKind<'a> {
    Select(&'a sqlparser::ast::Query),
    Insert(&'a sqlparser::ast::Insert),
    Update(&'a sqlparser::ast::Statement),
    Delete(&'a sqlparser::ast::Statement),
    Truncate(&'a sqlparser::ast::Statement),
    Other,
}

/// Classify a parsed statement.
pub fn classify(stmt: &Statement) -> StatementKind<'_> {
    match stmt {
        Statement::Query(q) => StatementKind::Select(q),
        Statement::Insert(ins) => StatementKind::Insert(ins),
        Statement::Update(_) => StatementKind::Update(stmt),
        Statement::Delete(_) => StatementKind::Delete(stmt),
        Statement::Truncate(_) => StatementKind::Truncate(stmt),
        _ => StatementKind::Other,
    }
}
