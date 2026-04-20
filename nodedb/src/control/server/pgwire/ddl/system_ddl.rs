//! `ALTER SYSTEM SET <field> = <value>` handler.
//!
//! System-level settings that change at runtime without a node
//! restart. The live cell lives on `SharedState::retention_settings`;
//! the collection-GC sweeper reads it on every tick.
//!
//! Currently supported fields:
//! - `deactivated_collection_retention_days` (u32)

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

pub fn alter_system(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can ALTER SYSTEM",
        ));
    }

    // Expected token shape:
    //   [0]=ALTER  [1]=SYSTEM  [2]=SET  [3]=<field>  [4]==  [5]=<value>
    // or without the `=` token:
    //   [0]=ALTER  [1]=SYSTEM  [2]=SET  [3]=<field>  [4]=<value>
    if parts.len() < 5 || !parts[2].eq_ignore_ascii_case("SET") {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER SYSTEM SET <field> = <value>",
        ));
    }

    let field = parts[3].trim_end_matches(';').to_lowercase();
    let value_idx = if parts.len() > 5 && parts[4] == "=" {
        5
    } else {
        4
    };
    if value_idx >= parts.len() {
        return Err(sqlstate_error("42601", "expected value after field name"));
    }
    let raw_value = parts[value_idx].trim_end_matches(';');

    match field.as_str() {
        "deactivated_collection_retention_days" => {
            let days: u32 = raw_value.parse().map_err(|_| {
                sqlstate_error(
                    "42601",
                    "deactivated_collection_retention_days must be a non-negative integer",
                )
            })?;
            let mut w = state
                .retention_settings
                .write()
                .map_err(|_| sqlstate_error("58000", "retention settings lock poisoned"))?;
            w.deactivated_collection_retention_days = days;
            drop(w);
            state.audit_record(
                AuditEvent::AdminAction,
                None,
                &identity.username,
                &format!("altered system: set {field} = {days}"),
            );
            Ok(vec![Response::Execution(Tag::new("ALTER SYSTEM"))])
        }
        other => Err(sqlstate_error(
            "42601",
            &format!(
                "unknown ALTER SYSTEM field: {other}. Valid: deactivated_collection_retention_days"
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    //! Integration-style coverage lives in
    //! `tests/collection_gc_retention.rs` — the ALTER SYSTEM path is
    //! exercised end-to-end there (propose → sweeper picks up new
    //! window on the next tick). The in-file suite would need a full
    //! `SharedState` fixture which is heavier than necessary for the
    //! parsing logic.
}
