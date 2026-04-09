//! Execute a prepared statement from an extended query portal.
//!
//! Binds parameter values from the portal into the SQL, then executes
//! through the same `execute_sql` path as SimpleQuery — preserving
//! all DDL dispatch, transaction handling, and permission checks.

use std::fmt::Debug;

use bytes::Bytes;
use futures::sink::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use super::super::core::NodeDbPgHandler;
use super::statement::ParsedStatement;

impl NodeDbPgHandler {
    /// Execute a prepared statement from a portal.
    ///
    /// Called by the `ExtendedQueryHandler::do_query` implementation.
    /// Binds parameters at the AST level (not SQL text substitution), then
    /// plans and dispatches through the standard pipeline.
    pub(crate) async fn execute_prepared<C>(
        &self,
        client: &mut C,
        portal: &Portal<ParsedStatement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        let identity = self.resolve_identity(client)?;
        let stmt = &portal.statement.statement;
        let tenant_id = identity.tenant_id;

        // Convert pgwire binary parameters to typed ParamValues for AST binding.
        let params = convert_portal_params(&portal.parameters, &stmt.param_types)?;

        // Execute through the planned SQL path with AST-level parameter binding.
        let mut results = self
            .execute_planned_sql_with_params(&identity, &stmt.sql, tenant_id, &addr, &params)
            .await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

/// Convert pgwire portal parameters to typed `ParamValue` for AST-level binding.
fn convert_portal_params(
    params: &[Option<Bytes>],
    param_types: &[Option<Type>],
) -> PgWireResult<Vec<nodedb_sql::ParamValue>> {
    let mut result = Vec::with_capacity(params.len());
    for (i, param) in params.iter().enumerate() {
        let pv = match param {
            None => nodedb_sql::ParamValue::Null,
            Some(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22021".to_owned(),
                        format!("invalid UTF-8 in parameter ${}", i + 1),
                    )))
                })?;

                let pg_type = param_types
                    .get(i)
                    .and_then(|t| t.as_ref())
                    .unwrap_or(&Type::UNKNOWN);

                pgwire_text_to_param(text, pg_type)
            }
        };
        result.push(pv);
    }
    Ok(result)
}

/// Convert a pgwire text parameter + type to a typed `ParamValue`.
fn pgwire_text_to_param(text: &str, pg_type: &Type) -> nodedb_sql::ParamValue {
    match *pg_type {
        Type::BOOL => {
            let lower = text.to_lowercase();
            if lower == "t" || lower == "true" || lower == "1" {
                return nodedb_sql::ParamValue::Bool(true);
            }
            if lower == "f" || lower == "false" || lower == "0" {
                return nodedb_sql::ParamValue::Bool(false);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::INT2 | Type::INT4 | Type::INT8 => {
            if let Ok(n) = text.parse::<i64>() {
                return nodedb_sql::ParamValue::Int64(n);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => {
            if let Ok(f) = text.parse::<f64>() {
                return nodedb_sql::ParamValue::Float64(f);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        _ => nodedb_sql::ParamValue::Text(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_null_param() {
        let params = vec![None];
        let types = vec![Some(Type::INT8)];
        let result = convert_portal_params(&params, &types).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], nodedb_sql::ParamValue::Null));
    }

    #[test]
    fn convert_typed_params() {
        let params = vec![
            Some(Bytes::from_static(b"42")),
            Some(Bytes::from_static(b"hello")),
            Some(Bytes::from_static(b"true")),
        ];
        let types = vec![Some(Type::INT8), Some(Type::TEXT), Some(Type::BOOL)];
        let result = convert_portal_params(&params, &types).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int64(42)));
        assert!(matches!(&result[1], nodedb_sql::ParamValue::Text(s) if s == "hello"));
        assert!(matches!(result[2], nodedb_sql::ParamValue::Bool(true)));
    }

    #[test]
    fn convert_float_param() {
        let params = vec![Some(Bytes::from_static(b"2.78"))];
        let types = vec![Some(Type::FLOAT8)];
        let result = convert_portal_params(&params, &types).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.78).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_bool_variants() {
        for (input, expected) in [("t", true), ("f", false), ("1", true), ("0", false)] {
            let params = vec![Some(Bytes::from(input))];
            let types = vec![Some(Type::BOOL)];
            let result = convert_portal_params(&params, &types).unwrap();
            assert!(matches!(result[0], nodedb_sql::ParamValue::Bool(v) if v == expected));
        }
    }
}
