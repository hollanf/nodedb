//! Bulk data import.
//!
//! Document bytes flow over the pgwire COPY framing — the client
//! reads the file under the operator's UID and streams content to
//! the server.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

/// `COPY <collection> FROM STDIN [WITH (FORMAT csv|json|ndjson)]`
pub async fn copy_from(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    Err(sqlstate_error(
        "0A000",
        "use `COPY <collection> FROM STDIN [WITH (FORMAT csv|json|ndjson)]` \
         and stream the file from the client",
    ))
}
