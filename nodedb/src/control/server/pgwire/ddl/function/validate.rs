//! Function body validation via DataFusion SQL parser.
//!
//! Creates a temporary DataFusion session, registers parameter columns as a
//! dummy table, and attempts to plan the body expression. Verifies that:
//! - The SQL expression is syntactically valid.
//! - All referenced identifiers resolve (parameters + system UDFs).
//! - The output type matches the declared return type.

use pgwire::error::PgWireResult;

use super::super::super::types::sqlstate_error;
use super::create::ParsedCreateFunction;
use super::parse::{
    default_values_for_params, sql_type_to_arrow, sql_type_to_arrow_sql, types_compatible,
};

/// Validate function body by attempting to parse it as a SQL expression.
///
/// Creates a temporary DataFusion session with a dummy table whose columns
/// match the function parameters, then tries to plan `SELECT <body> FROM __params`.
pub(super) fn validate_function_body(parsed: &ParsedCreateFunction) -> PgWireResult<()> {
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::SessionConfig;

    let config = SessionConfig::new()
        .with_information_schema(false)
        .with_default_catalog_and_schema("nodedb", "public");
    let ctx = SessionContext::new_with_config(config);

    // Use a runtime to drive the async DataFusion call.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| sqlstate_error("XX000", &format!("runtime: {e}")))?;

    // Register system UDFs so body can reference them.
    crate::control::planner::context::register_udfs_on(&ctx);

    if !parsed.parameters.is_empty() {
        // Build CREATE TABLE with parameter columns so they resolve in the body.
        let cols: Vec<String> = parsed
            .parameters
            .iter()
            .map(|p| format!("{} {}", p.name, sql_type_to_arrow_sql(&p.data_type)))
            .collect();
        let create_sql = format!(
            "CREATE TABLE __params ({}) AS VALUES ({})",
            cols.join(", "),
            default_values_for_params(&parsed.parameters),
        );

        rt.block_on(async {
            ctx.sql(&create_sql)
                .await
                .map_err(|e| sqlstate_error("42601", &format!("invalid parameter types: {e}")))
        })?;
    }

    // Build the test SQL.
    let body = &parsed.body_sql;
    let test_sql = if parsed.parameters.is_empty() {
        if body.to_uppercase().starts_with("SELECT ") {
            body.to_string()
        } else {
            format!("SELECT {body}")
        }
    } else if body.to_uppercase().starts_with("SELECT ") {
        format!("{body} FROM __params")
    } else {
        format!("SELECT {body} FROM __params")
    };

    rt.block_on(async {
        let df = ctx
            .sql(&test_sql)
            .await
            .map_err(|e| sqlstate_error("42601", &format!("invalid function body: {e}")))?;
        let plan = df
            .into_optimized_plan()
            .map_err(|e| sqlstate_error("42601", &format!("function body optimization: {e}")))?;

        // Verify return type matches declared type.
        let output_schema = plan.schema();
        if let Some(field) = output_schema.fields().first()
            && let Some(expected_dt) = sql_type_to_arrow(&parsed.return_type)
            && !types_compatible(field.data_type(), &expected_dt)
        {
            return Err(sqlstate_error(
                "42P13",
                &format!(
                    "return type mismatch: body returns {}, declared {}",
                    field.data_type(),
                    parsed.return_type
                ),
            ));
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::super::create::ParsedCreateFunction;
    use super::validate_function_body;
    use crate::control::security::catalog::{FunctionParam, FunctionVolatility};

    fn make_parsed(
        params: Vec<(&str, &str)>,
        return_type: &str,
        body: &str,
    ) -> ParsedCreateFunction {
        ParsedCreateFunction {
            or_replace: false,
            name: "test_fn".into(),
            parameters: params
                .into_iter()
                .map(|(n, t)| FunctionParam {
                    name: n.into(),
                    data_type: t.into(),
                })
                .collect(),
            return_type: return_type.into(),
            volatility: FunctionVolatility::Immutable,
            body_sql: body.into(),
        }
    }

    #[test]
    fn valid_text_body() {
        let p = make_parsed(vec![("email", "TEXT")], "TEXT", "SELECT LOWER(TRIM(email))");
        assert!(validate_function_body(&p).is_ok());
    }

    #[test]
    fn valid_numeric_body() {
        let p = make_parsed(vec![("x", "FLOAT")], "FLOAT", "SELECT x * 2.0");
        assert!(validate_function_body(&p).is_ok());
    }

    #[test]
    fn valid_no_params() {
        let p = make_parsed(vec![], "FLOAT", "SELECT 3.14159");
        assert!(validate_function_body(&p).is_ok());
    }

    #[test]
    fn invalid_body_unknown_function() {
        let p = make_parsed(vec![("x", "TEXT")], "TEXT", "SELECT NONEXISTENT_FUNC(x)");
        assert!(validate_function_body(&p).is_err());
    }

    #[test]
    fn type_mismatch() {
        let p = make_parsed(vec![("x", "INT")], "TEXT", "SELECT x + 1");
        assert!(validate_function_body(&p).is_err());
    }
}
