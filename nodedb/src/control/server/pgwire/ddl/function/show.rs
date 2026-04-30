//! `SHOW FUNCTIONS` DDL handler.
//!
//! Lists all user-defined functions for the current tenant, plus system functions.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

/// Handle `SHOW FUNCTIONS`
///
/// Returns a result set with columns: name, type, parameters, return_type, volatility, owner.
pub fn show_functions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("type"),
        text_field("parameters"),
        text_field("return_type"),
        text_field("volatility"),
        text_field("owner"),
    ]);

    let mut rows = Vec::new();

    // User-defined functions from catalog.
    if let Some(catalog) = state.credentials.catalog().as_ref()
        && let Ok(functions) = catalog.load_functions_for_tenant(tenant_id)
    {
        for func in &functions {
            let params_str = func
                .parameters
                .iter()
                .map(|p| format!("{} {}", p.name, p.data_type))
                .collect::<Vec<_>>()
                .join(", ");

            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&func.name);
            let _ = encoder.encode_field(&"expression".to_string());
            let _ = encoder.encode_field(&params_str);
            let _ = encoder.encode_field(&func.return_type);
            let _ = encoder.encode_field(&func.volatility.as_str().to_string());
            let _ = encoder.encode_field(&func.owner);
            rows.push(Ok(encoder.take_row()));
        }
    }

    // System functions (built-in UDFs) — listed with type "system".
    for name in crate::control::planner::context::SYSTEM_FUNCTION_NAMES {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&name.to_string());
        let _ = encoder.encode_field(&"system".to_string());
        let _ = encoder.encode_field(&String::new()); // params vary
        let _ = encoder.encode_field(&String::new()); // return type varies
        let _ = encoder.encode_field(&"immutable".to_string());
        let _ = encoder.encode_field(&"system".to_string());
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
