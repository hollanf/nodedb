//! `ALTER SEQUENCE` handler — RESTART + FORMAT paths.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `ALTER SEQUENCE <name> RESTART [WITH <value>] | FORMAT '<template>'`.
///
/// `name`, `action`, and `with_value` come from the typed
/// [`NodedbStatement::AlterSequence`] variant.
pub fn alter_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    action: &str,
    with_value: Option<&str>,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();

    if !state.sequence_registry.exists(tenant_id, name) {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("sequence \"{name}\" does not exist"),
        ))));
    }

    match action.to_uppercase().as_str() {
        "RESTART" => alter_restart(state, tenant_id, name, with_value),
        "FORMAT" => alter_format(state, tenant_id, name, with_value),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "ALTER SEQUENCE supports: RESTART [WITH value], FORMAT 'template'".to_owned(),
        )))),
    }
}

/// `ALTER SEQUENCE <name> RESTART [WITH <value>]`
fn alter_restart(
    state: &SharedState,
    tenant_id: u64,
    name: &str,
    with_value: Option<&str>,
) -> PgWireResult<Vec<Response>> {
    let restart_value = if let Some(v) = with_value.and_then(|s| s.parse::<i64>().ok()) {
        v
    } else {
        state
            .sequence_registry
            .get_def(tenant_id, name)
            .map(|d| d.start_value)
            .unwrap_or(1)
    };

    // RESTART touches the sequence *state* (current counter),
    // not the definition. Propose a `PutSequenceState` entry so
    // every node's in-memory registry converges on the new
    // counter value.
    let def = state
        .sequence_registry
        .get_def(tenant_id, name)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("sequence \"{name}\" does not exist"),
            )))
        })?;
    let new_state = crate::control::security::catalog::sequence_types::SequenceState {
        tenant_id,
        name: name.to_string(),
        current_value: restart_value,
        is_called: false,
        epoch: def.epoch,
        period_key: String::new(),
    };
    let entry = crate::control::catalog_entry::CatalogEntry::PutSequenceState(Box::new(new_state));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                e.to_string(),
            )))
        })?;
    if log_index == 0 {
        state
            .sequence_registry
            .restart(tenant_id, name, restart_value)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22023".to_owned(),
                    e.to_string(),
                )))
            })?;
        if let Some(catalog) = state.credentials.catalog() {
            state.sequence_registry.persist_all(catalog);
        }
    }

    Ok(vec![Response::Execution(Tag::new("ALTER SEQUENCE"))])
}

/// `ALTER SEQUENCE <name> FORMAT '<template>'`
fn alter_format(
    state: &SharedState,
    tenant_id: u64,
    name: &str,
    with_value: Option<&str>,
) -> PgWireResult<Vec<Response>> {
    let Some(raw) = with_value else {
        return Ok(vec![Response::Execution(Tag::new("ALTER SEQUENCE"))]);
    };
    let raw = raw.trim_matches('\'').trim_matches('"');
    let tokens = crate::control::sequence::format::parse_format_template(raw).map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            format!("invalid FORMAT: {e}"),
        )))
    })?;

    // FORMAT alters the stored *definition*, not the counter —
    // ship the whole updated `StoredSequence` through `PutSequence`
    // and let every node's applier replace it in redb + registry.
    if let Some(mut def) = state.sequence_registry.get_def(tenant_id, name) {
        def.format_template = Some(tokens);
        let entry = crate::control::catalog_entry::CatalogEntry::PutSequence(Box::new(def.clone()));
        let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    e.to_string(),
                )))
            })?;
        if log_index == 0 {
            if let Some(catalog) = state.credentials.catalog() {
                let _ = catalog.put_sequence(&def);
            }
            let _ = state.sequence_registry.remove(tenant_id, name);
            let _ = state.sequence_registry.create(def);
        }
    }
    Ok(vec![Response::Execution(Tag::new("ALTER SEQUENCE"))])
}
