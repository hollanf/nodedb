//! `SHOW STORAGE FOR collection` and `SHOW COMPACTION STATUS` handlers.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

/// Handle `SHOW STORAGE FOR collection`.
pub fn handle_show_storage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // SHOW STORAGE FOR collection
    let collection = parts
        .get(3)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "syntax: SHOW STORAGE FOR <collection>".to_owned(),
            )))
        })?
        .to_lowercase();

    let tenant_id = identity.tenant_id.as_u64();

    // Verify collection exists.
    if let Some(catalog) = state.credentials.catalog()
        && catalog
            .get_collection(tenant_id, &collection)
            .ok()
            .flatten()
            .is_none()
    {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("collection \"{collection}\" does not exist"),
        ))));
    }

    // Load column stats if available (from last ANALYZE).
    let stats = state
        .credentials
        .catalog()
        .as_ref()
        .and_then(|c| c.load_column_stats(tenant_id, &collection).ok())
        .unwrap_or_default();

    let schema = Arc::new(vec![
        text_field("collection"),
        text_field("columns"),
        text_field("row_count"),
        text_field("last_analyzed"),
    ]);

    let row_count = stats.first().map(|s| s.row_count).unwrap_or(0);
    let last_analyzed = stats
        .first()
        .map(|s| {
            if s.analyzed_at > 0 {
                format!("{}ms ago", now_ms().saturating_sub(s.analyzed_at))
            } else {
                "never".to_string()
            }
        })
        .unwrap_or_else(|| "never".to_string());

    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&collection);
    let _ = encoder.encode_field(&stats.len().to_string());
    let _ = encoder.encode_field(&row_count.to_string());
    let _ = encoder.encode_field(&last_analyzed);
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// Handle `SHOW COMPACTION STATUS`.
pub fn handle_show_compaction_status(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("status"),
        text_field("pending_jobs"),
        text_field("compaction_debt"),
    ]);

    // Compaction runs automatically in the Data Plane. We report the current
    // state as "idle" — detailed stats require Data Plane query support.
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&"idle");
    let _ = encoder.encode_field(&"0");
    let _ = encoder.encode_field(&"0");
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
