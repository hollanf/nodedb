//! BEFORE trigger firing logic.
//!
//! BEFORE triggers fire synchronously in the Control Plane BEFORE the row
//! mutation is dispatched to the Data Plane. They can:
//! - Validate and reject the DML via RAISE EXCEPTION
//! - Modify the NEW row (for INSERT/UPDATE) before it reaches storage
//! - Execute side-effect DML (dispatched through normal plan+SPSC path)
//!
//! BEFORE triggers are ALWAYS synchronous — there is no ASYNC or DEFERRED variant.

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::security::catalog::trigger_types::TriggerTiming;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::fire_common::{
    check_cascade_depth, fire_before_triggers_with_mutation, fire_triggers, map_to_hashmap,
};
use super::registry::DmlEvent;

/// Fire BEFORE ROW triggers for an INSERT operation.
///
/// Returns the (possibly modified) NEW fields. The caller MUST use the returned
/// fields for the actual PointPut dispatch, not the original input — a BEFORE
/// trigger may have normalized or enriched the row.
///
/// If a trigger raises an exception, the error propagates and the INSERT is aborted.
pub async fn fire_before_insert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    new_fields: &serde_json::Map<String, serde_json::Value>,
    cascade_depth: u32,
) -> crate::Result<serde_json::Map<String, serde_json::Value>> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Insert);

    let before_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .collect();

    if before_triggers.is_empty() {
        return Ok(new_fields.clone());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_insert(collection, map_to_hashmap(new_fields));

    let result = fire_before_triggers_with_mutation(
        state,
        identity,
        tenant_id,
        collection,
        &before_triggers,
        &bindings,
        cascade_depth,
        Some(new_fields.clone()),
    )
    .await?;

    // Return the (possibly mutated) NEW fields. If None somehow, return original.
    Ok(result.unwrap_or_else(|| new_fields.clone()))
}

/// Fire BEFORE ROW triggers for an UPDATE operation.
///
/// Returns the (possibly modified) NEW fields. `old_fields` is the row before
/// the update (read-only in the trigger body as OLD.*).
///
/// If a trigger raises an exception, the UPDATE is aborted.
pub async fn fire_before_update(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    old_fields: &serde_json::Map<String, serde_json::Value>,
    new_fields: &serde_json::Map<String, serde_json::Value>,
    cascade_depth: u32,
) -> crate::Result<serde_json::Map<String, serde_json::Value>> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Update);

    let before_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .collect();

    if before_triggers.is_empty() {
        return Ok(new_fields.clone());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_update(
        collection,
        map_to_hashmap(old_fields),
        map_to_hashmap(new_fields),
    );

    let result = fire_before_triggers_with_mutation(
        state,
        identity,
        tenant_id,
        collection,
        &before_triggers,
        &bindings,
        cascade_depth,
        Some(new_fields.clone()),
    )
    .await?;

    Ok(result.unwrap_or_else(|| new_fields.clone()))
}

/// Fire BEFORE ROW triggers for a DELETE operation.
///
/// BEFORE DELETE triggers cannot modify the row (there is no NEW). They can
/// only validate and reject via RAISE EXCEPTION, or execute side-effect DML.
///
/// If a trigger raises an exception, the DELETE is aborted.
pub async fn fire_before_delete(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    old_fields: &serde_json::Map<String, serde_json::Value>,
    cascade_depth: u32,
) -> crate::Result<()> {
    let triggers =
        state
            .trigger_registry
            .get_matching(tenant_id.as_u32(), collection, DmlEvent::Delete);

    let before_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .collect();

    if before_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::before_delete(collection, map_to_hashmap(old_fields));

    fire_triggers(
        state,
        identity,
        tenant_id,
        collection,
        &before_triggers,
        &bindings,
        cascade_depth,
    )
    .await
}
