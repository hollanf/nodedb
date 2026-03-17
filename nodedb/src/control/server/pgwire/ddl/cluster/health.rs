//! Peer health DDL commands: SHOW PEER HEALTH.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};
use super::topology::node_state_str;

fn encode_err(e: pgwire::error::PgWireError) -> pgwire::error::PgWireError {
    e
}

/// SHOW PEER HEALTH — circuit breaker state for all known peers.
///
/// Superuser only.
pub fn show_peer_health(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view peer health",
        ));
    }

    let transport = match &state.cluster_transport {
        Some(t) => t,
        None => {
            return Err(sqlstate_error(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let topo = match &state.cluster_topology {
        Some(t) => t,
        None => {
            return Err(sqlstate_error("55000", "cluster topology not available"));
        }
    };

    let topo = topo.read().unwrap_or_else(|p| p.into_inner());
    let cb = transport.circuit_breaker();

    let schema = Arc::new(vec![
        int8_field("node_id"),
        text_field("address"),
        text_field("node_state"),
        text_field("circuit"),
        int8_field("failures"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    let mut nodes: Vec<_> = topo
        .all_nodes()
        .filter(|n| n.node_id != state.node_id)
        .collect();
    nodes.sort_by_key(|n| n.node_id);

    for node in nodes {
        encoder
            .encode_field(&(node.node_id as i64))
            .map_err(encode_err)?;
        encoder.encode_field(&node.addr).map_err(encode_err)?;
        let state_str = node_state_str(node.state);
        encoder.encode_field(&state_str).map_err(encode_err)?;
        let circuit = cb.state(node.node_id);
        let circuit_str = match circuit {
            nodedb_cluster::circuit_breaker::CircuitState::Closed => "closed",
            nodedb_cluster::circuit_breaker::CircuitState::Open => "OPEN",
            nodedb_cluster::circuit_breaker::CircuitState::HalfOpen => "half-open",
        };
        encoder.encode_field(&circuit_str).map_err(encode_err)?;
        encoder
            .encode_field(&(cb.failure_count(node.node_id) as i64))
            .map_err(encode_err)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
