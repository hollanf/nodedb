//! `ALTER CHANGE STREAM` DDL handler.
//!
//! Syntax:
//! ```sql
//! ALTER CHANGE STREAM <name> ENABLE
//! ALTER CHANGE STREAM <name> DISABLE
//! ALTER CHANGE STREAM <name> SUSPEND
//! ALTER CHANGE STREAM <name> RESUME
//! ```
//!
//! ENABLE/DISABLE/SUSPEND/RESUME require a `paused` field on
//! `ChangeStreamDef` which is not yet present; those actions return
//! SQLSTATE 0A000 until that field is added.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `ALTER CHANGE STREAM <name> <action>`.
///
/// Typed entry point called from the AST router.
pub fn alter_change_stream(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    action: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "alter change streams")?;

    match action {
        "ENABLE" | "DISABLE" | "SUSPEND" | "RESUME" | "PAUSE" => Err(sqlstate_error(
            "0A000",
            &format!(
                "ALTER CHANGE STREAM {name} {action} is not yet supported; \
                     stream pause/resume requires a schema migration to add the \
                     'paused' field to ChangeStreamDef"
            ),
        )),
        _ => Err(sqlstate_error(
            "42601",
            &format!(
                "unknown ALTER CHANGE STREAM action '{action}'; \
                 expected ENABLE, DISABLE, SUSPEND, or RESUME"
            ),
        )),
    }
}
