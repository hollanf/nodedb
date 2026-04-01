//! `ALTER SCHEDULE` DDL handler.
//!
//! Supports: ENABLE, DISABLE, SET CRON 'expr'.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::scheduler::cron::CronExpr;

/// Handle `ALTER SCHEDULE name ENABLE | DISABLE | SET CRON 'expr'`.
pub fn alter_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // ALTER SCHEDULE name ...
    let name = parts
        .get(2)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "ALTER SCHEDULE requires a name".to_owned(),
            )))
        })?
        .to_lowercase();

    let upper: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();

    // Look up the schedule in the registry.
    let mut def = state
        .schedule_registry
        .get(tenant_id, &name)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42704".to_owned(),
                format!("schedule \"{name}\" does not exist"),
            )))
        })?;

    // Determine the alteration.
    let action = upper.get(3).map(|s| s.as_str()).unwrap_or("");

    match action {
        "ENABLE" => {
            def.enabled = true;
        }
        "DISABLE" => {
            def.enabled = false;
        }
        "SET" => {
            // SET CRON 'new_expr'
            let set_target = upper.get(4).map(|s| s.as_str()).unwrap_or("");
            if set_target != "CRON" {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "ALTER SCHEDULE SET supports: CRON".to_owned(),
                ))));
            }
            // Extract the quoted cron expression from the raw SQL.
            let new_cron = extract_quoted_cron(sql).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "ALTER SCHEDULE SET CRON requires a quoted cron expression".to_owned(),
                )))
            })?;

            CronExpr::parse(&new_cron).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22023".to_owned(),
                    format!("invalid cron expression: {e}"),
                )))
            })?;

            def.cron_expr = new_cron;
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

/// Extract the content between single quotes after "CRON" in the SQL.
fn extract_quoted_cron(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let cron_idx = upper.find("CRON")?;
    let rest = &sql[cron_idx + 4..];
    let start = rest.find('\'')?;
    let end = rest[start + 1..].find('\'')?;
    Some(rest[start + 1..start + 1 + end].to_string())
}
