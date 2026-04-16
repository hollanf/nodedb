//! `SHOW SCHEMA VERSION` — current descriptor version visible on
//! this node.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

/// SHOW SCHEMA VERSION — report the current descriptor version
/// counter and per-collection metadata if available.
pub fn show_schema_version(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view schema version",
        ));
    }

    let schema = Arc::new(vec![text_field("property"), text_field("value")]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    let version = state.schema_version.current();
    encoder.encode_field(&"schema_version")?;
    encoder.encode_field(&version.to_string())?;
    rows.push(Ok(encoder.take_row()));

    let applied_index = {
        let cache = state
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.applied_index
    };
    encoder.encode_field(&"metadata_applied_index")?;
    encoder.encode_field(&applied_index.to_string())?;
    rows.push(Ok(encoder.take_row()));

    encoder.encode_field(&"node_id")?;
    encoder.encode_field(&state.node_id.to_string())?;
    rows.push(Ok(encoder.take_row()));

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
