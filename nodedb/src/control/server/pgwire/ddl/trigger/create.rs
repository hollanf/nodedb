//! `CREATE TRIGGER` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::trigger_types::{
    TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity, TriggerTiming,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `CREATE [OR REPLACE] TRIGGER ...` from typed AST fields.
#[allow(clippy::too_many_arguments)]
pub fn create_trigger(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    or_replace: bool,
    execution_mode: &str,
    name: &str,
    timing: &str,
    events_insert: bool,
    events_update: bool,
    events_delete: bool,
    collection: &str,
    granularity: &str,
    when_condition: Option<&str>,
    priority: i32,
    security: &str,
    body_sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create triggers")?;

    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    // Check for existing trigger.
    if !or_replace && let Ok(Some(_)) = catalog.get_trigger(tenant_id, name) {
        return Err(sqlstate_error(
            "42710",
            &format!("trigger '{name}' already exists"),
        ));
    }

    // Validate the trigger body parses as procedural SQL.
    crate::control::planner::procedural::parse_block(body_sql)
        .map_err(|e| sqlstate_error("42601", &format!("trigger body parse error: {e}")))?;

    let execution_mode_enum = parse_execution_mode(execution_mode);
    let timing_enum = parse_timing(timing);
    let granularity_enum = parse_granularity(granularity);
    let security_enum = parse_security(security);

    if execution_mode_enum == TriggerExecutionMode::Sync {
        tracing::info!(
            trigger = %name,
            collection = %collection,
            "SYNC trigger created — trigger body DML must target same vShard"
        );
    }
    if execution_mode_enum == TriggerExecutionMode::Async {
        tracing::debug!(
            trigger = %name,
            collection = %collection,
            "ASYNC trigger: side effects are eventually consistent"
        );
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock before UNIX epoch"))?
        .as_secs();

    let batch_mode = crate::control::trigger::batch::classify::classify_trigger_body(body_sql);

    let events = TriggerEvents {
        on_insert: events_insert,
        on_update: events_update,
        on_delete: events_delete,
    };

    let stored = crate::control::security::catalog::trigger_types::StoredTrigger {
        tenant_id,
        name: name.to_string(),
        collection: collection.to_string(),
        timing: timing_enum,
        events,
        granularity: granularity_enum,
        when_condition: when_condition.map(|s| s.to_string()),
        body_sql: body_sql.to_string(),
        priority,
        enabled: true,
        execution_mode: execution_mode_enum,
        security: security_enum,
        batch_mode,
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutTrigger(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .put_trigger(&stored)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        state.trigger_registry.register(stored.clone());
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "CREATE TRIGGER {} {} {} ON {}",
            stored.name,
            stored.timing.as_str(),
            stored.events.display(),
            stored.collection
        ),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TRIGGER"))])
}

fn parse_execution_mode(s: &str) -> TriggerExecutionMode {
    match s.to_uppercase().as_str() {
        "SYNC" => TriggerExecutionMode::Sync,
        "DEFERRED" => TriggerExecutionMode::Deferred,
        _ => TriggerExecutionMode::Async,
    }
}

fn parse_timing(s: &str) -> TriggerTiming {
    match s.to_uppercase().as_str() {
        "BEFORE" => TriggerTiming::Before,
        "INSTEAD OF" => TriggerTiming::InsteadOf,
        _ => TriggerTiming::After,
    }
}

fn parse_granularity(s: &str) -> TriggerGranularity {
    if s.to_uppercase() == "STATEMENT" {
        TriggerGranularity::Statement
    } else {
        TriggerGranularity::Row
    }
}

fn parse_security(s: &str) -> TriggerSecurity {
    if s.to_uppercase() == "DEFINER" {
        TriggerSecurity::Definer
    } else {
        TriggerSecurity::Invoker
    }
}
