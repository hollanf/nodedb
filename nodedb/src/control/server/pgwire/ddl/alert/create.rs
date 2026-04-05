//! `CREATE ALERT` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::alert::types::AlertDef;

use super::super::super::types::{require_admin, sqlstate_error};
use super::ALERT_RULES_CRDT_COLLECTION;
use super::parse::parse_create_alert;

pub fn create_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create alerts")?;

    let parsed = parse_create_alert(sql)?;
    let tenant_id = identity.tenant_id.as_u32();

    // Validate collection exists.
    if let Some(catalog) = state.credentials.catalog()
        && catalog
            .get_collection(tenant_id, &parsed.collection)
            .ok()
            .flatten()
            .is_none()
    {
        return Err(sqlstate_error(
            "42P01",
            &format!("collection '{}' does not exist", parsed.collection),
        ));
    }

    // Check for duplicate alert name.
    if state.alert_registry.get(tenant_id, &parsed.name).is_some() {
        return Err(sqlstate_error(
            "42710",
            &format!("alert '{}' already exists", parsed.name),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock error"))?
        .as_secs();

    let def = AlertDef {
        tenant_id,
        name: parsed.name.clone(),
        collection: parsed.collection.clone(),
        where_filter: parsed.where_filter,
        condition: parsed.condition,
        group_by: parsed.group_by,
        window_ms: parsed.window_ms,
        fire_after: parsed.fire_after,
        recover_after: parsed.recover_after,
        severity: parsed.severity,
        notify_targets: parsed.notify_targets,
        enabled: true,
        owner: identity.username.clone(),
        created_at: now,
    };

    // Persist to catalog.
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_alert_rule(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    // Emit CRDT sync delta for Lite visibility.
    {
        let delta_payload = zerompk::to_msgpack_vec(&def).unwrap_or_default();
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            collection: ALERT_RULES_CRDT_COLLECTION.into(),
            document_id: def.name.clone(),
            payload: delta_payload,
            op: crate::event::crdt_sync::types::DeltaOp::Upsert,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(tenant_id, delta);
    }

    // Register in memory.
    state.alert_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE ALERT {}", parsed.name),
    );

    tracing::info!(
        name = parsed.name,
        collection = parsed.collection,
        "alert rule created"
    );

    Ok(vec![Response::Execution(Tag::new("CREATE ALERT"))])
}
