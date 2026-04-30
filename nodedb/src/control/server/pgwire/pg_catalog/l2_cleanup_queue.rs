//! `_system.l2_cleanup_queue` virtual view.
//!
//! Surfaces the post-purge object-store delete backlog. Rows come
//! from `SystemCatalog::load_l2_cleanup_queue()`; the table is
//! populated on hard-delete and drained by the L2 cleanup worker as
//! `DELETE` calls succeed. Each row corresponds to one collection
//! whose L2 bytes are still owed to the object store.
//!
//! Columns:
//! - `tenant_id` — numeric tenant id (int8).
//! - `name` — collection name (text).
//! - `purge_lsn` — WAL LSN at which the hard-delete committed (int8).
//! - `enqueued_at_ns` — Unix-epoch wall-clock nanoseconds when the
//!   entry was queued (int8).
//! - `bytes_pending` — best-effort estimate of L2 bytes still to
//!   delete, 0 if unknown (int8).
//! - `last_error` — last error the worker observed, empty when
//!   healthy (text).
//! - `attempts` — number of delete attempts this entry has survived
//!   (int4).
//!
//! Visibility: tenant_admin and superuser see every tenant's entries;
//! regular users see only their own tenant.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::pgwire::types::{int4_field, int8_field, text_field};
use crate::control::state::SharedState;

/// Row generator for `_system.l2_cleanup_queue`.
pub fn l2_cleanup_queue(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("tenant_id"),
        text_field("name"),
        int8_field("purge_lsn"),
        int8_field("enqueued_at_ns"),
        int8_field("bytes_pending"),
        text_field("last_error"),
        int4_field("attempts"),
    ]);

    let Some(catalog) = state.credentials.catalog() else {
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(Vec::<Result<_, pgwire::error::PgWireError>>::new()),
        ))]);
    };

    let queue = catalog
        .load_l2_cleanup_queue()
        .map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;

    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);
    let caller_tenant = identity.tenant_id.as_u64();

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());
    for e in &queue {
        if !is_admin && e.tenant_id != caller_tenant {
            continue;
        }
        encoder.encode_field(&(e.tenant_id as i64))?;
        encoder.encode_field(&e.name.as_str())?;
        encoder.encode_field(&(e.purge_lsn as i64))?;
        encoder.encode_field(&(e.enqueued_at_ns as i64))?;
        encoder.encode_field(&(e.bytes_pending as i64))?;
        encoder.encode_field(&e.last_error.as_str())?;
        encoder.encode_field(&(e.attempts as i32))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
