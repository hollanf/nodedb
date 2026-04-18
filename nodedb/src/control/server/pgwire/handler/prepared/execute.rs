//! Execute a prepared statement from an extended query portal.
//!
//! Binds parameter values from the portal into the SQL, then executes
//! through the same `execute_sql` path as SimpleQuery — preserving
//! all DDL dispatch, transaction handling, and permission checks.

use std::fmt::Debug;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::sink::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use sonic_rs;

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

        // J.4: mirror `do_query`'s audit scope. The extended-query
        // path also triggers DDL (a prepared `CREATE COLLECTION`
        // binds parameters then dispatches), so audit context must
        // be installed here too or followers receive a plain
        // `CatalogDdl` with no SQL trail.
        let _audit_scope = crate::control::server::pgwire::session::audit_context::AuditScope::new(
            crate::control::server::pgwire::session::audit_context::AuditCtx {
                auth_user_id: identity.user_id.to_string(),
                auth_user_name: identity.username.clone(),
                sql_text: stmt.sql.clone(),
            },
        );

        // Wire-streaming COPY shapes for backup/restore. Recognised before
        // sqlparser-based execution because the shapes aren't standard COPY
        // grammar. See `control::backup::detect`.
        if let Some(intent) = crate::control::backup::detect(&stmt.sql) {
            return self.intent_to_response(&identity, addr, intent).await;
        }

        // DSL passthroughs (SEARCH, GRAPH, MATCH, UPSERT INTO, etc.) cannot be
        // handled by the planned-SQL path. Route them through the same full DSL
        // dispatcher used by the simple-query handler. DSL statements do not use
        // SQL parameter placeholders, so bound parameters are intentionally ignored.
        if stmt.is_dsl {
            let mut results = self.execute_sql(&identity, &addr, &stmt.sql).await?;
            return Ok(results.pop().unwrap_or(Response::EmptyQuery));
        }

        // Convert pgwire binary parameters to typed ParamValues for AST binding.
        let params = convert_portal_params(&portal.parameters, &stmt.param_types)?;

        // Execute through the planned SQL path with AST-level parameter binding.
        let mut results = self
            .execute_planned_sql_with_params(&identity, &stmt.sql, tenant_id, &addr, &params)
            .await?;
        let result = results.pop().unwrap_or(Response::EmptyQuery);

        // When the statement declared typed result columns via Describe, the
        // client expects DataRow messages with one field per declared column.
        //
        // The generic `payload_to_response` path produces a single-column
        // QueryResponse with the full JSON as one text field. In the extended-
        // query protocol the RowDescription was already sent by Describe, so
        // pgwire sends only the DataRow messages on Execute — the client maps
        // them against the previously-described schema. A 1-field row against
        // an N-column schema causes null values for columns 2..N.
        //
        // Fix: when result_fields is non-empty, consume the single-field stream,
        // parse each JSON object, and re-encode with one pgwire field per
        // declared column.
        if !stmt.result_fields.is_empty() {
            reproject_response(result, &stmt.result_fields).await
        } else {
            Ok(result)
        }
    }
}

/// Re-encode a query response to match the column schema declared by Describe.
///
/// Each DataRow from `payload_to_response` contains a single text field holding
/// a JSON object. We parse each object and extract fields in `result_fields`
/// order, producing a new QueryResponse whose rows have one field per declared
/// column. Missing fields are sent as SQL NULL.
///
/// Non-query responses (execution tags) pass through unchanged.
async fn reproject_response(
    response: Response,
    result_fields: &[FieldInfo],
) -> PgWireResult<Response> {
    let qr = match response {
        Response::Query(qr) => qr,
        other => return Ok(other),
    };

    let schema = Arc::new(result_fields.to_vec());
    let field_names: Vec<String> = result_fields.iter().map(|f| f.name().to_string()).collect();

    // Collect JSON objects from the single-column stream produced by
    // payload_to_response. Each DataRow has exactly one field: a JSON string.
    let json_rows = collect_json_rows(qr).await?;

    let mut pgwire_rows = Vec::with_capacity(json_rows.len());
    for obj in &json_rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for name in &field_names {
            match obj.get(name) {
                None | Some(serde_json::Value::Null) => {
                    let _ = encoder.encode_field(&Option::<String>::None);
                }
                Some(v) => {
                    let text = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let _ = encoder.encode_field(&text);
                }
            }
        }
        pgwire_rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(pgwire_rows),
    )))
}

/// Consume a `QueryResponse` stream and decode the single text field of each
/// `DataRow` as a JSON object.
///
/// `payload_to_response` always produces rows where field[0] is a JSON string.
/// The pgwire `DataRow.data` format is: for each field, 4-byte length (i32,
/// big-endian) followed by the field bytes. `-1` (0xFFFFFFFF) means SQL NULL.
async fn collect_json_rows(mut qr: QueryResponse) -> PgWireResult<Vec<serde_json::Value>> {
    let mut rows = Vec::new();
    while let Some(row_result) = qr.data_rows.next().await {
        let row = row_result?;
        // Decode field[0] from the raw DataRow wire format.
        let text = decode_first_field_text(&row.data);
        if let Some(t) = text {
            let val: serde_json::Value =
                sonic_rs::from_str(t).unwrap_or_else(|_| serde_json::Value::String(t.to_string()));
            rows.push(val);
        }
    }
    Ok(rows)
}

/// Decode the text bytes of the first field from a pgwire `DataRow` wire buffer.
///
/// Wire format: for each field, 4-byte big-endian length followed by bytes.
/// Returns `None` for NULL fields or invalid encodings.
fn decode_first_field_text(data: &bytes::BytesMut) -> Option<&str> {
    if data.len() < 4 {
        return None;
    }
    let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if len < 0 {
        // NULL field.
        return None;
    }
    let len = len as usize;
    if data.len() < 4 + len {
        return None;
    }
    std::str::from_utf8(&data[4..4 + len]).ok()
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

    #[test]
    fn decode_first_field_text_normal() {
        // Wire format: 4-byte length (big-endian) + UTF-8 bytes.
        let text = b"hello";
        let mut data = bytes::BytesMut::new();
        data.extend_from_slice(&(text.len() as i32).to_be_bytes());
        data.extend_from_slice(text);
        assert_eq!(decode_first_field_text(&data), Some("hello"));
    }

    #[test]
    fn decode_first_field_text_null() {
        // -1 length means SQL NULL.
        let mut data = bytes::BytesMut::new();
        data.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(decode_first_field_text(&data), None);
    }

    #[test]
    fn decode_first_field_text_empty() {
        assert_eq!(decode_first_field_text(&bytes::BytesMut::new()), None);
    }
}
