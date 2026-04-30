//! `CREATE AGGREGATE FUNCTION ... LANGUAGE WASM AS <base64>` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::planner::wasm;
use crate::control::security::catalog::FunctionParam;
use crate::control::security::catalog::function_types::*;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};
use super::parse::{find_matching_paren, parse_parameters, validate_identifier};

/// Handle `CREATE [OR REPLACE] AGGREGATE FUNCTION <name>(<input_type>)
///         RETURNS <type> LANGUAGE WASM AS '<base64>'`
pub fn create_wasm_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create WASM aggregate functions")?;

    let parsed = parse_aggregate_create(sql)?;
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_function(tenant_id, &parsed.name)
    {
        return Err(sqlstate_error(
            "42723",
            &format!("function '{}' already exists", parsed.name),
        ));
    }

    // Decode base64 binary.
    use base64::Engine;
    let wasm_bytes = base64::engine::general_purpose::STANDARD
        .decode(&parsed.base64_body)
        .map_err(|e| sqlstate_error("42601", &format!("invalid base64: {e}")))?;

    // Store the WASM binary.
    let config = wasm::WasmConfig::default();
    let hash = wasm::store::store_wasm_binary(catalog, &wasm_bytes, config.max_binary_size)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // Validate aggregate exports (init, accumulate, merge, finalize).
    let runtime =
        wasm::runtime::WasmRuntime::new().map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let module = runtime
        .get_or_compile(&wasm_bytes)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    wasm::wit::validate_aggregate_exports(&module)
        .map_err(|e| sqlstate_error("42601", &e.to_string()))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock"))?
        .as_secs();

    // Store as a function with language=WASM. The "aggregate" nature is
    // indicated by the name prefix "agg_" in the WASM exports and by
    // the fact that it will be registered as AggregateUDF, not ScalarUDF.
    let stored = StoredFunction {
        tenant_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        return_type: parsed.return_type,
        body_sql: "AGGREGATE".into(), // Marker for aggregate functions
        compiled_body_sql: None,
        volatility: FunctionVolatility::Volatile,
        security: FunctionSecurity::Invoker,
        language: FunctionLanguage::Wasm,
        wasm_hash: Some(hash),
        wasm_fuel: config.default_fuel,
        wasm_memory: config.default_memory_bytes,
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    catalog
        .put_function(&stored)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE AGGREGATE FUNCTION {} LANGUAGE WASM", stored.name),
    );

    Ok(vec![Response::Execution(Tag::new(
        "CREATE AGGREGATE FUNCTION",
    ))])
}

struct ParsedAggregateCreate {
    or_replace: bool,
    name: String,
    parameters: Vec<FunctionParam>,
    return_type: String,
    base64_body: String,
}

fn parse_aggregate_create(sql: &str) -> PgWireResult<ParsedAggregateCreate> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    let (or_replace, after) = if upper.starts_with("CREATE OR REPLACE AGGREGATE FUNCTION ") {
        (
            true,
            &trimmed["CREATE OR REPLACE AGGREGATE FUNCTION ".len()..],
        )
    } else if upper.starts_with("CREATE AGGREGATE FUNCTION ") {
        (false, &trimmed["CREATE AGGREGATE FUNCTION ".len()..])
    } else {
        return Err(sqlstate_error(
            "42601",
            "expected CREATE AGGREGATE FUNCTION",
        ));
    };

    let paren_open = after
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '('"))?;
    let name = after[..paren_open].trim().to_lowercase();
    validate_identifier(&name)?;

    let paren_close = find_matching_paren(after, paren_open)
        .ok_or_else(|| sqlstate_error("42601", "unmatched '('"))?;
    let params_str = &after[paren_open + 1..paren_close];
    let parameters = parse_parameters(params_str)?;

    let rest = after[paren_close + 1..].trim();
    let rest_upper = rest.to_uppercase();

    if !rest_upper.starts_with("RETURNS ") {
        return Err(sqlstate_error("42601", "expected RETURNS <type>"));
    }
    let after_returns = rest["RETURNS ".len()..].trim();

    let lang_pos = after_returns
        .to_uppercase()
        .find("LANGUAGE")
        .ok_or_else(|| sqlstate_error("42601", "expected LANGUAGE WASM"))?;
    let return_type = after_returns[..lang_pos].trim().to_uppercase();

    let after_lang = after_returns[lang_pos + "LANGUAGE".len()..].trim();
    if !after_lang.to_uppercase().starts_with("WASM") {
        return Err(sqlstate_error("42601", "expected LANGUAGE WASM"));
    }
    let after_wasm = after_lang["WASM".len()..].trim();

    let after_upper = after_wasm.to_uppercase();
    if !after_upper.starts_with("AS") {
        return Err(sqlstate_error("42601", "expected AS '<base64>'"));
    }
    let body = after_wasm["AS".len()..].trim();
    let base64_body = if body.starts_with('\'') && body.ends_with('\'') {
        body[1..body.len() - 1].replace("''", "'")
    } else {
        body.to_string()
    };

    Ok(ParsedAggregateCreate {
        or_replace,
        name,
        parameters,
        return_type,
        base64_body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let sql =
            "CREATE AGGREGATE FUNCTION my_sum(val INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        let parsed = parse_aggregate_create(sql).unwrap();
        assert_eq!(parsed.name, "my_sum");
        assert_eq!(parsed.parameters.len(), 1);
        assert_eq!(parsed.return_type, "INT");
        assert!(!parsed.or_replace);
    }

    #[test]
    fn parse_or_replace() {
        let sql =
            "CREATE OR REPLACE AGGREGATE FUNCTION f(x INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        assert!(parse_aggregate_create(sql).unwrap().or_replace);
    }
}
