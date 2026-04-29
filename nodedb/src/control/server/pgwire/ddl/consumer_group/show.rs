//! `SHOW CONSUMER GROUPS ON <stream>` and `SHOW PARTITIONS ON <stream>` handlers.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

/// Handle `SHOW CONSUMER GROUPS ON <stream>`
pub fn show_consumer_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // parts: ["SHOW", "CONSUMER", "GROUPS", "ON", "<stream>"]
    if parts.len() < 5 || !parts[3].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "expected SHOW CONSUMER GROUPS ON <stream>",
        ));
    }

    let stream_name = parts[4].to_lowercase();
    let tenant_id = identity.tenant_id.as_u32();

    let schema = Arc::new(vec![
        text_field("group_name"),
        text_field("stream"),
        text_field("committed_partitions"),
        text_field("owner"),
    ]);

    let groups = state
        .group_registry
        .list_for_stream(tenant_id, &stream_name);

    let mut rows = Vec::new();
    for g in &groups {
        let offsets = state
            .offset_store
            .get_all_offsets(tenant_id, &stream_name, &g.name);
        let committed_count = offsets.len();

        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&g.name);
        let _ = encoder.encode_field(&g.stream_name);
        let _ = encoder.encode_field(&committed_count.to_string());
        let _ = encoder.encode_field(&g.owner);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Handle `SHOW PARTITIONS ON <stream>`
///
/// Lists all vShard partitions that have events in the stream's buffer,
/// with earliest/latest LSN for each partition.
pub fn show_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // parts: ["SHOW", "PARTITIONS", "ON", "<stream>"]
    if parts.len() < 4 || !parts[2].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "expected SHOW PARTITIONS ON <stream>",
        ));
    }

    let stream_name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id.as_u32();

    // Get the stream's buffer from the CdcRouter.
    let buffer = state.cdc_router.get_buffer(tenant_id, &stream_name);

    let schema = Arc::new(vec![
        text_field("partition_id"),
        text_field("earliest_lsn"),
        text_field("latest_lsn"),
        text_field("event_count"),
    ]);

    let mut rows = Vec::new();
    if let Some(buf) = buffer {
        // Scan the buffer and collect per-partition stats.
        let events = buf.read_from_lsn(0, usize::MAX);
        let mut partition_stats: std::collections::BTreeMap<u32, (u64, u64, usize)> =
            std::collections::BTreeMap::new();
        for e in &events {
            let entry = partition_stats
                .entry(e.partition)
                .or_insert((u64::MAX, 0, 0));
            entry.0 = entry.0.min(e.lsn);
            entry.1 = entry.1.max(e.lsn);
            entry.2 += 1;
        }
        for (pid, (earliest, latest, count)) in &partition_stats {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&pid.to_string());
            let _ = encoder.encode_field(&earliest.to_string());
            let _ = encoder.encode_field(&latest.to_string());
            let _ = encoder.encode_field(&count.to_string());
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
