//! `ALTER SCHEDULE` DDL handler.
//!
//! Supports: ENABLE, DISABLE, SET CRON 'expr'.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::scheduler::cron::CronExpr;

/// Handle `ALTER SCHEDULE <name> ENABLE | DISABLE | SET CRON '<expr>'`.
///
/// `name`, `action`, and `cron_expr` come from the typed
/// [`NodedbStatement::AlterSchedule`] variant.
pub fn alter_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    action: &str,
    cron_expr: Option<&str>,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    // Look up the schedule in the registry.
    let mut def = state
        .schedule_registry
        .get(tenant_id, name)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!("schedule \"{name}\" does not exist"),
            )))
        })?;

    match action {
        "ENABLE" => {
            def.enabled = true;
        }
        "DISABLE" => {
            def.enabled = false;
        }
        "SET" => {
            let new_cron = cron_expr.ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "ALTER SCHEDULE SET CRON requires a quoted cron expression".to_owned(),
                )))
            })?;

            CronExpr::parse(new_cron).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22023".to_owned(),
                    format!("invalid cron expression: {e}"),
                )))
            })?;

            def.cron_expr = new_cron.to_string();
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "ALTER SCHEDULE supports: ENABLE, DISABLE, SET CRON 'expr'".to_owned(),
            ))));
        }
    }

    // Persist updated definition.
    if let Some(catalog) = state.credentials.catalog() {
        catalog.put_schedule(&def).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("failed to persist schedule: {e}"),
            )))
        })?;
    }

    // Update in-memory registry.
    state.schedule_registry.update(def);

    Ok(vec![Response::Execution(Tag::new("ALTER SCHEDULE"))])
}
