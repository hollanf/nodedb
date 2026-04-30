//! `SHOW TRIGGERS` DDL handler.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

/// Handle `SHOW TRIGGERS [ON <collection>]`
pub fn show_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();

    // Optional collection filter: SHOW TRIGGERS ON <collection>
    let collection_filter = if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("ON") {
        Some(parts[3].to_lowercase())
    } else {
        None
    };

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("collection"),
        text_field("timing"),
        text_field("events"),
        text_field("granularity"),
        text_field("execution"),
        text_field("enabled"),
        text_field("priority"),
        text_field("owner"),
    ]);

    let triggers = state.trigger_registry.list_for_tenant(tenant_id);
    let mut sorted = triggers;
    sorted.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));

    let mut rows = Vec::new();
    for t in &sorted {
        if let Some(ref filter) = collection_filter
            && &t.collection != filter
        {
            continue;
        }
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&t.name);
        let _ = encoder.encode_field(&t.collection);
        let _ = encoder.encode_field(&t.timing.as_str().to_string());
        let _ = encoder.encode_field(&t.events.display());
        let _ = encoder.encode_field(&t.granularity.as_str().to_string());
        let _ = encoder.encode_field(&t.execution_mode.as_str().to_string());
        let _ = encoder.encode_field(&t.enabled.to_string());
        let _ = encoder.encode_field(&t.priority.to_string());
        let _ = encoder.encode_field(&t.owner);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
