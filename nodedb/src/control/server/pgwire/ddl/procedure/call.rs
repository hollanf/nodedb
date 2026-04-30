//! `CALL <procedure>(args)` execution handler.
//!
//! Parses the CALL statement, resolves the procedure from the catalog,
//! binds arguments to parameters, and executes the body via the statement
//! executor with fuel metering and timeout.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::StatementExecutor;
use crate::control::planner::procedural::executor::fuel::ExecutionBudget;
use crate::control::security::catalog::procedure_types::ParamDirection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// Handle `CALL <procedure>(arg1, arg2, ...)`
pub async fn call_procedure(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (name, args) = parse_call(sql)?;
    let tenant_id = identity.tenant_id;

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let proc = catalog
        .get_procedure(tenant_id.as_u64(), &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42883", &format!("procedure '{name}' does not exist")))?;

    // Validate argument count matches IN parameters.
    let in_params: Vec<_> = proc
        .parameters
        .iter()
        .filter(|p| matches!(p.direction, ParamDirection::In | ParamDirection::InOut))
        .collect();

    if args.len() != in_params.len() {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "procedure '{}' expects {} argument(s), got {}",
                name,
                in_params.len(),
                args.len()
            ),
        ));
    }

    // Build parameter bindings: param_name → argument value (as SQL literal).
    let mut param_map = std::collections::HashMap::new();
    for (param, arg) in in_params.iter().zip(args.iter()) {
        param_map.insert(param.name.clone(), arg.clone());
    }
    let bindings = RowBindings::with_params(param_map);

    // Parse the procedure body.
    let block = crate::control::planner::procedural::parse_block(&proc.body_sql)
        .map_err(|e| sqlstate_error("42601", &format!("procedure body parse error: {e}")))?;

    // Execute with fuel metering, timeout, and transaction context.
    let mut budget = ExecutionBudget::new(proc.max_iterations, proc.timeout_secs);
    let executor =
        StatementExecutor::new(state, identity.clone(), tenant_id, 0).with_transaction_context();

    executor
        .execute_block_with_budget(&block, &bindings, &mut budget)
        .await
        .map_err(|e| sqlstate_error("P0001", &e.to_string()))?;

    // Check for OUT parameter values.
    let out_params: Vec<_> = proc
        .parameters
        .iter()
        .filter(|p| matches!(p.direction, ParamDirection::Out | ParamDirection::InOut))
        .collect();

    if out_params.is_empty() {
        return Ok(vec![Response::Execution(Tag::new("CALL"))]);
    }

    // Return OUT values as a single-row result set.
    let out_values = executor.take_out_values();
    build_out_response(&out_params, &out_values)
}

/// Build a single-row result set from OUT parameter values.
fn build_out_response(
    out_params: &[&crate::control::security::catalog::procedure_types::ProcedureParam],
    out_values: &std::collections::HashMap<String, nodedb_types::Value>,
) -> PgWireResult<Vec<Response>> {
    use futures::stream;
    use pgwire::api::results::{DataRowEncoder, QueryResponse};

    let schema = std::sync::Arc::new(
        out_params
            .iter()
            .map(|p| super::super::super::types::text_field(&p.name))
            .collect::<Vec<_>>(),
    );

    let mut encoder = DataRowEncoder::new(schema.clone());
    for param in out_params {
        let value = out_values
            .get(&param.name)
            // Also check __return for single-OUT-param procedures using RETURN.
            .or_else(|| {
                if out_params.len() == 1 {
                    out_values.get("__return")
                } else {
                    None
                }
            });
        let text = match value {
            Some(nodedb_types::Value::Null) | None => String::new(),
            Some(nodedb_types::Value::String(s)) => s.clone(),
            Some(v) => v.to_sql_literal(),
        };
        let _ = encoder.encode_field(&text);
    }
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// Parse `CALL <name>(arg1, arg2, ...)`.
///
/// Returns (procedure_name, argument_values_as_sql_strings).
fn parse_call(sql: &str) -> PgWireResult<(String, Vec<String>)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    if !upper.starts_with("CALL ") {
        return Err(sqlstate_error("42601", "expected CALL <procedure>(...)"));
    }
    let after_call = &trimmed["CALL ".len()..].trim();

    // Find the paren that starts the argument list.
    let paren_pos = after_call
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '(' after procedure name in CALL"))?;

    let name = after_call[..paren_pos].trim().to_lowercase();
    if name.is_empty() {
        return Err(sqlstate_error("42601", "procedure name required in CALL"));
    }

    // Extract arguments between parens.
    let close_paren = super::super::parse_utils::find_matching_paren(after_call, paren_pos)
        .ok_or_else(|| sqlstate_error("42601", "unmatched '(' in CALL"))?;

    let args_str = &after_call[paren_pos + 1..close_paren];
    let args = if args_str.trim().is_empty() {
        Vec::new()
    } else {
        split_call_args(args_str)
    };

    Ok((name, args))
}

/// Split comma-separated arguments, respecting parentheses and string literals.
fn split_call_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_string = false;

    for ch in s.chars() {
        if in_string {
            current.push(ch);
            if ch == '\'' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '\'' => {
                in_string = true;
                current.push(ch);
            }
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let last = current.trim().to_string();
    if !last.is_empty() {
        args.push(last);
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_call_basic() {
        let (name, args) = parse_call("CALL archive(90)").unwrap();
        assert_eq!(name, "archive");
        assert_eq!(args, vec!["90"]);
    }

    #[test]
    fn parse_call_multiple_args() {
        let (name, args) = parse_call("CALL migrate('users', 100)").unwrap();
        assert_eq!(name, "migrate");
        assert_eq!(args, vec!["'users'", "100"]);
    }

    #[test]
    fn parse_call_no_args() {
        let (name, args) = parse_call("CALL cleanup()").unwrap();
        assert_eq!(name, "cleanup");
        assert!(args.is_empty());
    }

    #[test]
    fn parse_call_nested_parens() {
        let (_, args) = parse_call("CALL p(func(1, 2), 3)").unwrap();
        assert_eq!(args, vec!["func(1, 2)", "3"]);
    }

    #[test]
    fn parse_call_with_semicolon() {
        let (name, _) = parse_call("CALL cleanup();").unwrap();
        assert_eq!(name, "cleanup");
    }
}
