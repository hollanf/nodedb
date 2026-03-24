//! Pub/Sub DDL handlers: CREATE TOPIC, DROP TOPIC, SHOW TOPICS, PUBLISH TO, SUBSCRIBE TO.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// CREATE TOPIC <name>
pub fn create_topic(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let name = parts
        .get(2)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| sqlstate_error("42601", "CREATE TOPIC requires a name"))?
        .to_lowercase();

    state
        .topic_registry
        .create_topic(&name)
        .map_err(|e| sqlstate_error("42P07", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("created topic '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TOPIC"))])
}

/// DROP TOPIC <name>
pub fn drop_topic(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let name = parts.get(2).unwrap_or(&"").to_lowercase();
    state
        .topic_registry
        .drop_topic(&name)
        .map_err(|e| sqlstate_error("42P01", &e.to_string()))?;
    Ok(vec![Response::Execution(Tag::new("DROP TOPIC"))])
}

/// SHOW TOPICS
pub fn show_topics(state: &SharedState) -> PgWireResult<Vec<Response>> {
    let topics = state.topic_registry.list_topics();
    let schema = Arc::new(vec![
        text_field("topic"),
        text_field("messages"),
        text_field("subscribers"),
    ]);

    let mut rows = Vec::new();
    for name in &topics {
        if let Some(stats) = state.topic_registry.topic_stats(name) {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&stats.name);
            let _ = encoder.encode_field(&stats.message_count.to_string());
            let _ = encoder.encode_field(&stats.subscriber_count.to_string());
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// PUBLISH TO <topic> '<payload>'
pub fn publish_to(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let topic_name = parts.get(2).unwrap_or(&"").to_lowercase();

    // Extract payload: everything after "PUBLISH TO <topic>"
    let remainder = if sql.len() > parts[..3].iter().map(|p| p.len() + 1).sum::<usize>() {
        &sql[parts[..3].iter().map(|p| p.len() + 1).sum::<usize>()..]
    } else {
        ""
    };
    let payload = remainder.trim().trim_matches('\'').to_string();

    if payload.is_empty() {
        return Err(sqlstate_error("42601", "PUBLISH TO requires a payload"));
    }

    let seq = state
        .topic_registry
        .publish(&topic_name, payload, &identity.username)
        .map_err(|e| sqlstate_error("42P01", &e.to_string()))?;

    let schema = Arc::new(vec![text_field("seq")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&seq.to_string());
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// SUBSCRIBE TO <topic> [SINCE <seq>]
pub fn subscribe_to(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    sql: &str,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let topic_name = parts.get(2).unwrap_or(&"").to_lowercase();
    let upper = sql.to_uppercase();
    let since_seq: u64 = upper
        .find(" SINCE ")
        .and_then(|pos| sql[pos + 7..].split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let (sub_id, _rx, backlog) = state
        .topic_registry
        .subscribe(&topic_name, since_seq)
        .map_err(|e| sqlstate_error("42P01", &e.to_string()))?;

    let schema = Arc::new(vec![
        text_field("subscription_id"),
        text_field("topic"),
        text_field("backlog"),
    ]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&sub_id.to_string());
    let _ = encoder.encode_field(&topic_name);
    let _ = encoder.encode_field(&backlog.len().to_string());
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}
