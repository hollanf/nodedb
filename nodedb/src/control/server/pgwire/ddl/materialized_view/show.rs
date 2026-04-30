//! `SHOW MATERIALIZED VIEWS [FOR <source>]` handler.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

pub fn show_materialized_views(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parts[4].to_lowercase())
    } else {
        None
    };

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("source"),
        text_field("refresh_mode"),
        text_field("owner"),
        text_field("query"),
    ]);

    let views = if let Some(catalog) = state.credentials.catalog() {
        catalog
            .list_materialized_views(tenant_id.as_u64())
            .map_err(|e| sqlstate_error("XX000", &format!("catalog read failed: {e}")))?
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    for view in &views {
        if let Some(ref filter) = source_filter
            && view.source != *filter
        {
            continue;
        }

        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&view.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.source)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.refresh_mode)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&view.query_sql)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
