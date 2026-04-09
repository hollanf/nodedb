//! nodedb-sql: SQL parser, planner, and optimizer for NodeDB.
//!
//! Parses SQL via sqlparser-rs, resolves against a catalog, and produces
//! `SqlPlan` — an intermediate representation that both Origin (server)
//! and Lite (embedded) map to their own execution model.
//!
//! ```text
//! SQL → parse → resolve → plan → optimize → SqlPlan
//! ```

pub mod error;
pub mod functions;
pub mod optimizer;
pub mod params;
pub mod parser;
pub mod planner;
pub mod resolver;
pub mod types;

pub use error::{Result, SqlError};
pub use params::ParamValue;
pub use types::*;

use functions::registry::FunctionRegistry;
use parser::statement::{StatementKind, classify, parse_sql};

/// Plan one or more SQL statements against the given catalog.
///
/// Returns a list of `SqlPlan` — one per statement (some statements
/// like multi-row INSERT may produce multiple plans).
pub fn plan_sql(sql: &str, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let functions = FunctionRegistry::new();
    let statements = parse_sql(sql)?;
    let mut plans = Vec::new();

    for stmt in &statements {
        match classify(stmt) {
            StatementKind::Select(query) => {
                let plan = planner::select::plan_query(query, catalog, &functions)?;
                let plan = optimizer::optimize(plan);
                plans.push(plan);
            }
            StatementKind::Insert(ins) => {
                let mut insert_plans = planner::dml::plan_insert(ins, catalog)?;
                plans.append(&mut insert_plans);
            }
            StatementKind::Update(stmt) => {
                let mut update_plans = planner::dml::plan_update(stmt, catalog)?;
                plans.append(&mut update_plans);
            }
            StatementKind::Delete(stmt) => {
                let mut delete_plans = planner::dml::plan_delete(stmt, catalog)?;
                plans.append(&mut delete_plans);
            }
            StatementKind::Truncate(stmt) => {
                let mut trunc_plans = planner::dml::plan_truncate_stmt(stmt)?;
                plans.append(&mut trunc_plans);
            }
            StatementKind::Other => {
                return Err(SqlError::Unsupported {
                    detail: format!("statement type: {stmt}"),
                });
            }
        }
    }

    Ok(plans)
}

/// Plan SQL with bound parameters (prepared statement execution).
///
/// Parses the SQL (which may contain `$1`, `$2`, ... placeholders), substitutes
/// placeholder AST nodes with concrete literal values from `params`, then plans
/// normally. This avoids SQL text substitution entirely — parameters are bound
/// at the AST level, not the string level.
pub fn plan_sql_with_params(
    sql: &str,
    params: &[ParamValue],
    catalog: &dyn SqlCatalog,
) -> Result<Vec<SqlPlan>> {
    let functions = FunctionRegistry::new();
    let mut statements = parse_sql(sql)?;

    // Bind parameters at AST level before planning.
    for stmt in &mut statements {
        params::bind_params(stmt, params);
    }

    let mut plans = Vec::new();
    for stmt in &statements {
        match classify(stmt) {
            StatementKind::Select(query) => {
                let plan = planner::select::plan_query(query, catalog, &functions)?;
                let plan = optimizer::optimize(plan);
                plans.push(plan);
            }
            StatementKind::Insert(ins) => {
                let mut insert_plans = planner::dml::plan_insert(ins, catalog)?;
                plans.append(&mut insert_plans);
            }
            StatementKind::Update(stmt) => {
                let mut update_plans = planner::dml::plan_update(stmt, catalog)?;
                plans.append(&mut update_plans);
            }
            StatementKind::Delete(stmt) => {
                let mut delete_plans = planner::dml::plan_delete(stmt, catalog)?;
                plans.append(&mut delete_plans);
            }
            StatementKind::Truncate(stmt) => {
                let mut trunc_plans = planner::dml::plan_truncate_stmt(stmt)?;
                plans.append(&mut trunc_plans);
            }
            StatementKind::Other => {
                return Err(SqlError::Unsupported {
                    detail: format!("statement type: {stmt}"),
                });
            }
        }
    }

    Ok(plans)
}
