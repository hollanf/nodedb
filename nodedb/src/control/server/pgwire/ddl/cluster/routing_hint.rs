//! `SHOW ROUTING` — expose the vshard → leaseholder → node address
//! mapping so smart clients can cache it and route writes directly
//! to the leaseholder, skipping the gateway hop.
//!
//! Result columns: `vshard_id`, `group_id`, `leaseholder_node_id`,
//! `leaseholder_addr`.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// SHOW ROUTING — full vshard → leaseholder → address table.
///
/// Any authenticated user may call this (smart-client libs need it).
pub fn show_routing(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let routing = match &state.cluster_routing {
        Some(r) => r,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let schema = Arc::new(vec![
        int8_field("vshard_id"),
        int8_field("group_id"),
        int8_field("leaseholder_node_id"),
        text_field("leaseholder_addr"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    let rt = routing.read().unwrap_or_else(|p| p.into_inner());
    let topo_guard = state
        .cluster_topology
        .as_ref()
        .map(|t| t.read().unwrap_or_else(|p| p.into_inner()));

    for vshard_id in 0..nodedb_cluster::routing::VSHARD_COUNT {
        let group_id = rt.group_for_vshard(vshard_id).unwrap_or(0);
        let leader = rt.group_info(group_id).map(|info| info.leader).unwrap_or(0);
        let addr = topo_guard
            .as_ref()
            .and_then(|topo| topo.get_node(leader))
            .map(|n| n.addr.clone())
            .unwrap_or_default();

        encoder.encode_field(&(vshard_id as i64))?;
        encoder.encode_field(&(group_id as i64))?;
        encoder.encode_field(&(leader as i64))?;
        encoder.encode_field(&addr)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
