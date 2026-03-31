//! `SHOW SCHEDULES` DDL handler.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

/// Handle `SHOW SCHEDULES`
pub fn show_schedules(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("cron"),
        text_field("scope"),
        text_field("target"),
        text_field("overlap"),
        text_field("missed_policy"),
        text_field("enabled"),
        text_field("last_status"),
        text_field("owner"),
    ]);

    let schedules = state.schedule_registry.list_for_tenant(tenant_id);

    let mut rows = Vec::new();
    for s in &schedules {
        let last_run = state.job_history.last_run(tenant_id, &s.name);
        let last_status = match last_run {
            Some(ref r) if r.success => "ok".to_string(),
            Some(ref r) => format!("error: {}", r.error.as_deref().unwrap_or("unknown")),
            None => "never".to_string(),
        };

        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&s.name);
        let _ = encoder.encode_field(&s.cron_expr);
        let _ = encoder.encode_field(&s.scope.as_str().to_string());
        let target = s.target_collection.as_deref().unwrap_or("*");
        let _ = encoder.encode_field(&target.to_string());
        let _ = encoder.encode_field(&s.allow_overlap.to_string());
        let _ = encoder.encode_field(&s.missed_policy.as_str().to_string());
        let _ = encoder.encode_field(&s.enabled.to_string());
        let _ = encoder.encode_field(&last_status);
        let _ = encoder.encode_field(&s.owner);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
