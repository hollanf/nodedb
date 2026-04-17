//! Tenant backup / restore DDL.
//!
//! Backup bytes flow over the pgwire COPY framing — the database
//! never opens an fd against a caller-named filesystem path. The
//! client process places bytes on disk under the operator's UID.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

const FEATURE_NOT_SUPPORTED: &str = "0A000";

fn require_superuser(identity: &AuthenticatedIdentity, action: &str) -> PgWireResult<()> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            &format!("permission denied: only superuser can {action}"),
        ));
    }
    Ok(())
}

/// `COPY (BACKUP TENANT <id>) TO STDOUT`
pub async fn backup_tenant(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_superuser(identity, "backup tenants")?;
    Err(sqlstate_error(
        FEATURE_NOT_SUPPORTED,
        "use `COPY (BACKUP TENANT <id>) TO STDOUT` and stream the wire \
         output to a file on the client",
    ))
}

/// `COPY tenant_restore(<id>) FROM STDIN`
pub async fn restore_tenant(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_superuser(identity, "restore tenants")?;
    Err(sqlstate_error(
        FEATURE_NOT_SUPPORTED,
        "use `COPY tenant_restore(<id>) FROM STDIN` and stream backup \
         bytes from the client",
    ))
}

/// `COPY tenant_restore(<id>) FROM STDIN DRY RUN`
pub fn restore_tenant_dry_run(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_superuser(identity, "validate restores")?;
    Err(sqlstate_error(
        FEATURE_NOT_SUPPORTED,
        "use `COPY tenant_restore(<id>) FROM STDIN DRY RUN` and stream \
         backup bytes from the client",
    ))
}
