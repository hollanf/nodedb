//! `SHOW TOPICS` DDL handler.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

pub fn show_topics(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("retention_secs"),
        text_field("buffered_events"),
        text_field("owner"),
    ]);

    let topics = state.ep_topic_registry.list_for_tenant(tenant_id);
    let mut rows = Vec::new();

    for t in &topics {
        let buffer_key = format!("topic:{}", t.name);
        let buffered = state
            .cdc_router
            .get_buffer(tenant_id, &buffer_key)
            .map(|b| b.len())
            .unwrap_or(0);

        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&t.name);
        let _ = encoder.encode_field(&t.retention.max_age_secs.to_string());
        let _ = encoder.encode_field(&buffered.to_string());
        let _ = encoder.encode_field(&t.owner);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
