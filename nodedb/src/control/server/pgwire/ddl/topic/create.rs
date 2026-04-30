//! `CREATE TOPIC` DDL handler.
//!
//! Syntax: `CREATE TOPIC <name> [WITH (RETENTION = '1 hour')]`

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::stream_def::RetentionConfig;
use crate::event::topic::TopicDef;

use super::super::super::types::{require_admin, sqlstate_error};

pub fn create_topic(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create topics")?;

    // parts: ["CREATE", "TOPIC", "<name>", ...]
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "expected CREATE TOPIC <name>"));
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    if state.ep_topic_registry.get(tenant_id, &name).is_some() {
        return Err(sqlstate_error(
            "42710",
            &format!("topic '{name}' already exists"),
        ));
    }

    // Parse optional retention from WITH clause.
    let mut retention = RetentionConfig {
        max_events: 10_000,
        max_age_secs: 3_600, // 1 hour default for topics.
    };

    let upper = sql.to_uppercase();
    if let Some(with_pos) = upper.find("WITH") {
        let with_section = sql[with_pos + 4..].trim();
        if let Some(inner) = with_section
            .strip_prefix('(')
            .and_then(|s| s.split_once(')'))
            .map(|(inner, _)| inner)
        {
            let inner_upper = inner.to_uppercase();
            if let Some(ret_pos) = inner_upper.find("RETENTION") {
                let after = inner[ret_pos + 9..].trim().trim_start_matches('=').trim();
                let val = after
                    .trim_start_matches('\'')
                    .split('\'')
                    .next()
                    .unwrap_or("");
                retention.max_age_secs = parse_duration_secs(val);
            }
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock error"))?
        .as_secs();

    let def = TopicDef {
        tenant_id,
        name: name.clone(),
        retention,
        owner: identity.username.clone(),
        created_at: now,
    };

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_ep_topic(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.ep_topic_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE TOPIC {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TOPIC"))])
}

/// Parse a human-friendly duration string into seconds.
fn parse_duration_secs(s: &str) -> u64 {
    let s = s.trim().to_lowercase();
    if let Some(h) = s.strip_suffix("hour").or_else(|| s.strip_suffix("hours")) {
        return h.trim().parse::<u64>().unwrap_or(1) * 3600;
    }
    if let Some(m) = s
        .strip_suffix("minute")
        .or_else(|| s.strip_suffix("minutes"))
    {
        return m.trim().parse::<u64>().unwrap_or(1) * 60;
    }
    if let Some(d) = s.strip_suffix("day").or_else(|| s.strip_suffix("days")) {
        return d.trim().parse::<u64>().unwrap_or(1) * 86_400;
    }
    // Try raw seconds.
    s.parse::<u64>().unwrap_or(3_600)
}
