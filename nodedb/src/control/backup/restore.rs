//! RESTORE TENANT orchestrator.
//!
//! Validates a backup envelope, merges all sections into a single
//! `TenantDataSnapshot`, then splits the merged snapshot into per-node
//! sub-snapshots according to the *current* cluster topology and
//! dispatches `MetaOp::RestoreTenantSnapshot` to each owning node.
//!
//! This is what makes the restore topology-drift safe: keys are
//! re-bucketed under the live `vshard_for_collection` mapping, not
//! the source-cluster mapping recorded in the envelope.
//!
//! Single-node mode is the degenerate case: routing absent → one
//! bucket, dispatched locally.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::routing::{VSHARD_COUNT, vshard_for_collection};
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc, TypedClusterError};
use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, EnvelopeError, VERSION_ENCRYPTED, VERSION_PLAIN,
    parse as parse_envelope, parse_encrypted as parse_envelope_encrypted,
};
use serde::Serialize;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{MetaOp, wire as plan_wire};
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{TenantDataSnapshot, TenantId, TraceId};

const NODE_RESTORE_TIMEOUT: Duration = Duration::from_secs(120);

/// Aggregate stats returned to the client at the end of a restore.
#[derive(Debug, Default, Clone, Serialize)]
pub struct RestoreStats {
    pub tenant_id: u64,
    pub dry_run: bool,
    pub sections: u16,
    pub source_vshard_count: u16,
    pub documents: usize,
    pub indexes: usize,
    pub edges: usize,
    pub vectors: usize,
    pub kv_tables: usize,
    pub crdt_state: usize,
    pub timeseries: usize,
    pub nodes_dispatched: usize,
    /// Entries whose key/collection name didn't parse — likely a
    /// corrupted snapshot. Each is logged at WARN with its key prefix.
    /// A non-zero count after a "successful" restore means the operator
    /// should investigate the source backup.
    pub malformed_keys: usize,
    /// Entries whose vshard owner couldn't be resolved at restore time
    /// (no current leader for the target vshard). Each is delivered to
    /// the local node — Raft replication then redistributes once the
    /// leader is back. Non-zero is informational, not necessarily an
    /// error.
    pub route_fallbacks: usize,
}

/// Restore a tenant from a fully-buffered backup envelope.
///
/// `tenant_id` is the request-scoped tenant ID; the envelope's
/// embedded `tenant_id` must match.
pub async fn restore_tenant(
    state: &Arc<SharedState>,
    tenant_id: u64,
    envelope_bytes: &[u8],
    dry_run: bool,
) -> Result<RestoreStats, Error> {
    // Detect envelope version from the 5th byte (version field in header).
    // Version 2 envelopes are encrypted; version 1 are plaintext.
    // If the byte slice is too short, `parse_envelope` / `parse_envelope_encrypted`
    // will return `Truncated` as expected.
    let envelope_version = envelope_bytes.get(4).copied().unwrap_or(0);
    let env = if envelope_version == VERSION_ENCRYPTED {
        match &state.backup_kek {
            Some(kek) => parse_envelope_encrypted(envelope_bytes, DEFAULT_MAX_TOTAL_BYTES, kek)
                .map_err(envelope_to_err)?,
            None => {
                return Err(Error::Internal {
                    detail: "restore: envelope is encrypted (version 2) but no backup KEK \
                             is configured; set [backup_encryption] in the server config"
                        .into(),
                });
            }
        }
    } else {
        // Version 1 (plaintext) or unknown — let parse surface the error.
        if envelope_version != VERSION_PLAIN && envelope_version != 0 {
            tracing::warn!(
                envelope_version,
                "restore: unrecognised envelope version; attempting plaintext parse"
            );
        }
        if envelope_version == VERSION_PLAIN && state.backup_kek.is_some() {
            tracing::warn!(
                "restore: envelope is unencrypted (version 1) but a backup KEK is configured; \
                 proceeding without decryption — re-backup to produce an encrypted envelope"
            );
        }
        parse_envelope(envelope_bytes, DEFAULT_MAX_TOTAL_BYTES).map_err(envelope_to_err)?
    };
    if env.meta.tenant_id != tenant_id {
        // Mismatch is a hard error — request is for a different tenant.
        return Err(Error::Internal {
            detail: format!(
                "backup tenant mismatch: envelope has {}, request is for {}",
                env.meta.tenant_id, tenant_id
            ),
        });
    }

    // Staleness gate. An envelope whose captured watermark is older
    // than any dispatch this destination cluster has already observed
    // for the same tenant would silently roll back newer committed
    // writes. Reject unconditionally today — a `FORCE` override is
    // the operator's escape hatch and is a separate DDL decision.
    // Envelopes with `snapshot_watermark == 0` come from a source
    // that did not capture a watermark (pre-gate envelopes); let them
    // through for compatibility — the gate only rejects real
    // ordered-against-ordered comparisons.
    if !dry_run && env.meta.snapshot_watermark != 0 {
        let current_high_water = state
            .tenant_write_hlc
            .lock()
            .ok()
            .and_then(|map| map.get(&tenant_id).copied())
            .unwrap_or(0);
        if env.meta.snapshot_watermark < current_high_water {
            return Err(Error::Internal {
                detail: format!(
                    "restore refused: envelope watermark {} is older than the \
                     destination cluster's last observed write-HLC {} for tenant \
                     {} — newer writes would be silently overwritten",
                    env.meta.snapshot_watermark, current_high_water, tenant_id
                ),
            });
        }
    }

    let mut stats = RestoreStats {
        tenant_id,
        dry_run,
        sections: env.sections.len() as u16,
        source_vshard_count: env.meta.source_vshard_count,
        ..Default::default()
    };

    // Apply catalog-row + source-tombstone metadata sections first
    // so the destination catalog has the soft-deleted rows (for
    // post-restore UNDROP) + purge barrier (so a resurrection of
    // source-side-purged rows is shadowed on replay).
    if !dry_run {
        apply_metadata_sections(state, tenant_id, &env);
    }

    // Merge every section into one snapshot.
    let merged = merge_sections(&env.sections)?;
    stats.documents = merged.documents.len();
    stats.indexes = merged.indexes.len();
    stats.edges = merged.edges.len();
    stats.vectors = merged.vectors.len();
    stats.kv_tables = merged.kv_tables.len();
    stats.crdt_state = merged.crdt_state.len();
    stats.timeseries = merged.timeseries.len();

    // Warn-and-audit for any collection in the snapshot that has been
    // hard-deleted on the destination cluster since the snapshot was
    // captured. Restore still proceeds (operator intent wins — the
    // tombstone set is tenant-local state and was not captured in the
    // backup), but the operator needs to know they just resurrected
    // data that had been purged.
    warn_on_tombstoned_restores(state, tenant_id, &merged, env.meta.snapshot_watermark);

    if dry_run {
        return Ok(stats);
    }

    // Split per current topology, dispatch each.
    let SplitOutput {
        buckets,
        malformed_keys,
        route_fallbacks,
    } = split_by_current_topology(state, tenant_id, merged);
    stats.nodes_dispatched = buckets.len();
    stats.malformed_keys = malformed_keys;
    stats.route_fallbacks = route_fallbacks;
    if malformed_keys > 0 {
        tracing::warn!(
            tenant_id,
            count = malformed_keys,
            "restore: snapshot contained keys that did not parse — possible corruption"
        );
    }
    if route_fallbacks > 0 {
        tracing::warn!(
            tenant_id,
            count = route_fallbacks,
            "restore: routed some entries to local node because no current leader was visible"
        );
    }
    // Dispatch the per-node sub-snapshots. The local bucket runs first
    // on the SPSC bridge (its handler mutates this node's engine state
    // and must not race with concurrent reads on this node). Remote
    // buckets then fan out in parallel via QUIC RPC — they touch
    // independent state machines and don't interact with each other,
    // so a sequential `for ... .await?` would have turned N × rpc
    // timeouts into N × that wall time when a peer is unreachable.
    // Each remote dispatch's error already carries the failing node
    // id, so first-error-wins is preserved by reducing the result
    // vector.
    let mut local_plan: Option<PhysicalPlan> = None;
    let mut remote_futs = Vec::with_capacity(buckets.len());
    for (node_id, sub) in buckets {
        let payload = zerompk::to_msgpack_vec(&sub).map_err(|e| Error::Internal {
            detail: format!("restore: snapshot encode failed: {e}"),
        })?;
        let plan = PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
            tenant_id,
            snapshot: payload,
        });
        if is_self(state, node_id) {
            local_plan = Some(plan);
        } else {
            let state = state.clone();
            remote_futs
                .push(async move { dispatch_remote(&state, node_id, tenant_id, plan).await });
        }
    }
    if let Some(plan) = local_plan {
        sync_dispatch::dispatch_async(
            state,
            TenantId::new(tenant_id),
            "__system",
            plan,
            NODE_RESTORE_TIMEOUT,
        )
        .await?;
    }
    let results = futures::future::join_all(remote_futs).await;
    if let Some(first_err) = results.into_iter().find_map(Result::err) {
        return Err(first_err);
    }

    Ok(stats)
}

/// Walk the merged snapshot, extract the distinct collection names
/// present across every engine section, and for each one that has a
/// live tombstone on the destination cluster emit a loud warning +
/// audit event. Best-effort: catalog-open failures silently skip the
/// check (the main restore path still runs its own catalog-open).
fn warn_on_tombstoned_restores(
    state: &Arc<SharedState>,
    tenant_id: u64,
    merged: &TenantDataSnapshot,
    snapshot_watermark: u64,
) {
    let Some(catalog) = state.credentials.catalog() else {
        return;
    };
    let Ok(tombstones) = catalog.load_wal_tombstones() else {
        return;
    };
    if tombstones.is_empty() {
        return;
    }

    // Keys across the snapshot all start with `{tenant_id}:{collection}`
    // (with either `:` or `\0` as the next separator depending on
    // engine). Extract the second segment for each key and dedupe.
    let mut names = std::collections::BTreeSet::new();
    let sections: [&[(String, Vec<u8>)]; 6] = [
        &merged.documents,
        &merged.indexes,
        &merged.vectors,
        &merged.kv_tables,
        &merged.timeseries,
        &merged.edges,
    ];
    for section in sections {
        for (key, _) in section {
            if let Some(name) = collection_from_key(key) {
                names.insert(name.to_string());
            }
        }
    }

    for name in &names {
        let Some(purge_lsn) = tombstones.purge_lsn(tenant_id, name) else {
            continue;
        };
        // Any snapshot whose watermark is below the purge_lsn is
        // pre-purge. A 0 watermark (envelope had no watermark) is
        // treated as "older than any purge" — flag it too.
        if snapshot_watermark != 0 && snapshot_watermark >= purge_lsn {
            continue;
        }
        tracing::warn!(
            tenant_id,
            collection = %name,
            purge_lsn,
            snapshot_watermark,
            "RESTORE: bringing back a collection that was hard-deleted on this cluster — \
             operator intent wins, but the data predates the purge"
        );
        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(TenantId::new(tenant_id)),
            "__restore",
            &format!(
                "restore resurrected tombstoned collection '{name}' \
                 (purge_lsn={purge_lsn}, snapshot_watermark={snapshot_watermark})"
            ),
        );
    }
}

/// Parse `{tid}:{collection}(:|\0)...` → `collection`.
fn collection_from_key(key: &str) -> Option<&str> {
    let tail = key.split_once(':')?.1;
    tail.split([':', '\0']).next()
}

#[cfg(test)]
mod collection_key_tests {
    use super::collection_from_key;

    #[test]
    fn extracts_collection_with_colon_separator() {
        assert_eq!(collection_from_key("1:users:doc-1"), Some("users"));
    }

    #[test]
    fn extracts_collection_with_null_separator() {
        // Edge-key shape: `{tid}:{src}\x00{label}\x00{tid}:{dst}`
        assert_eq!(collection_from_key("1:src\0label\0"), Some("src"));
    }

    #[test]
    fn vector_and_kv_key_shapes() {
        // These engines use `{tid}:{collection}` with no trailing part.
        assert_eq!(collection_from_key("1:events"), Some("events"));
    }

    #[test]
    fn no_tenant_prefix_returns_none() {
        assert_eq!(collection_from_key("no_colon"), None);
    }
}

fn merge_sections(
    sections: &[nodedb_types::backup_envelope::Section],
) -> Result<TenantDataSnapshot, Error> {
    let mut merged = TenantDataSnapshot::default();
    for section in sections {
        // Metadata sections carry catalog rows + source-side
        // tombstones, not tenant engine data. Skip them here; they're
        // applied separately in `apply_metadata_sections`.
        if is_metadata_section(section) {
            continue;
        }
        // Decoder errors must NOT echo deserializer context to the client.
        let snap: TenantDataSnapshot =
            zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                detail: "invalid backup format: section payload is not a tenant snapshot".into(),
            })?;
        merged.documents.extend(snap.documents);
        merged.indexes.extend(snap.indexes);
        merged.edges.extend(snap.edges);
        merged.vectors.extend(snap.vectors);
        merged.kv_tables.extend(snap.kv_tables);
        merged.crdt_state.extend(snap.crdt_state);
        merged.timeseries.extend(snap.timeseries);
    }
    Ok(merged)
}

fn is_metadata_section(section: &nodedb_types::backup_envelope::Section) -> bool {
    matches!(
        section.origin_node_id,
        nodedb_types::backup_envelope::SECTION_ORIGIN_CATALOG_ROWS
            | nodedb_types::backup_envelope::SECTION_ORIGIN_SOURCE_TOMBSTONES
    )
}

/// Apply catalog-row and source-tombstone sections to the destination
/// catalog. Runs BEFORE the data-section restore so the catalog rows
/// are present when the data dispatch re-hydrates per-engine state,
/// and the tombstone barrier is in place when replay runs.
pub(super) fn apply_metadata_sections(
    state: &Arc<SharedState>,
    tenant_id: u64,
    env: &nodedb_types::backup_envelope::Envelope,
) {
    use nodedb_types::backup_envelope::{
        SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_SOURCE_TOMBSTONES, SourceTombstoneEntry,
        StoredCollectionBlob,
    };
    let Some(catalog) = state.credentials.catalog() else {
        return;
    };

    for section in &env.sections {
        match section.origin_node_id {
            SECTION_ORIGIN_CATALOG_ROWS => {
                let Ok(blobs) = zerompk::from_msgpack::<Vec<StoredCollectionBlob>>(&section.body)
                else {
                    tracing::warn!(
                        tenant_id,
                        "restore: catalog-rows section failed to decode — skipping"
                    );
                    continue;
                };
                for blob in blobs {
                    let Ok(coll) = zerompk::from_msgpack::<
                        crate::control::security::catalog::StoredCollection,
                    >(&blob.bytes) else {
                        tracing::warn!(
                            tenant_id,
                            name = %blob.name,
                            "restore: catalog row failed to decode — skipping"
                        );
                        continue;
                    };
                    // Only re-insert if the destination doesn't already
                    // have a newer descriptor — the destination's state
                    // wins for freshness. Best-effort on error.
                    if let Err(e) = catalog.put_collection(&coll) {
                        tracing::warn!(
                            tenant_id,
                            name = %blob.name,
                            error = %e,
                            "restore: catalog put_collection failed"
                        );
                    }
                }
            }
            SECTION_ORIGIN_SOURCE_TOMBSTONES => {
                let Ok(tombs) = zerompk::from_msgpack::<Vec<SourceTombstoneEntry>>(&section.body)
                else {
                    tracing::warn!(
                        tenant_id,
                        "restore: source-tombstones section failed to decode — skipping"
                    );
                    continue;
                };
                for t in tombs {
                    if let Err(e) =
                        catalog.record_wal_tombstone(tenant_id, &t.collection, t.purge_lsn)
                    {
                        tracing::warn!(
                            tenant_id,
                            collection = %t.collection,
                            purge_lsn = t.purge_lsn,
                            error = %e,
                            "restore: record_wal_tombstone failed"
                        );
                    }
                }
            }
            _ => {} // Data section — handled elsewhere.
        }
    }
}

/// Bucketed output from `split_by_current_topology`.
struct SplitOutput {
    buckets: BTreeMap<u64, TenantDataSnapshot>,
    /// Entries whose key/collection couldn't be parsed (corrupt snapshot).
    malformed_keys: usize,
    /// Entries whose target vshard had no current leader; routed to self
    /// as a fallback (Raft replication redistributes once a leader exists).
    route_fallbacks: usize,
}

/// Routing decision for a single entry.
enum RouteOutcome {
    /// Resolved cleanly to this node.
    Routed(u64),
    /// Key/collection parse failed — corrupt snapshot signal.
    Malformed,
    /// Parsed fine but no current leader — fallback to self.
    NoLeader,
}

/// Bucket the merged snapshot per current vshard ownership.
///
/// Replicated-by-design data (graph edges, CRDT state) goes to every
/// owning node — those engines tolerate (and rely on) replicated state.
fn split_by_current_topology(
    state: &SharedState,
    tenant_id: u64,
    merged: TenantDataSnapshot,
) -> SplitOutput {
    let routing = state.cluster_routing.as_ref().map(|r| r.read().unwrap());
    let single_node = routing.is_none() || state.cluster_transport.is_none();

    if single_node {
        // Degenerate case: everything to self.
        let mut out = BTreeMap::new();
        out.insert(state.node_id, merged);
        return SplitOutput {
            buckets: out,
            malformed_keys: 0,
            route_fallbacks: 0,
        };
    }
    let routing = routing.unwrap();

    // Pre-compute every owning node so we can broadcast the by-design
    // replicated engines without missing one.
    let mut all_owners = BTreeMap::<u64, TenantDataSnapshot>::new();
    for vshard in 0..VSHARD_COUNT {
        if let Ok(node) = routing.leader_for_vshard(vshard)
            && node != 0
        {
            all_owners.entry(node).or_default();
        }
    }
    if all_owners.is_empty() {
        all_owners.insert(state.node_id, TenantDataSnapshot::default());
    }

    let route_collection = |coll: &str| -> RouteOutcome {
        let v = vshard_for_collection(coll);
        match routing.leader_for_vshard(v) {
            Ok(leader) if leader != 0 => RouteOutcome::Routed(leader),
            _ => RouteOutcome::NoLeader,
        }
    };
    let route_key = |key: &str| -> RouteOutcome {
        match extract_collection(key, tenant_id) {
            Some(coll) => route_collection(coll),
            None => RouteOutcome::Malformed,
        }
    };

    let mut malformed = 0usize;
    let mut fallbacks = 0usize;
    let mut resolve = |outcome: RouteOutcome, key: Option<&str>| -> u64 {
        match outcome {
            RouteOutcome::Routed(node) => node,
            RouteOutcome::Malformed => {
                malformed += 1;
                if let Some(k) = key {
                    let prefix: String = k.chars().take(64).collect();
                    tracing::warn!(tenant_id, key_prefix = %prefix, "restore: malformed key");
                }
                state.node_id
            }
            RouteOutcome::NoLeader => {
                fallbacks += 1;
                state.node_id
            }
        }
    };

    for entry in merged.documents {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().documents.push(entry);
    }
    for entry in merged.indexes {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().indexes.push(entry);
    }
    for entry in merged.vectors {
        // vector key prefix is "{tid}:{collection}".
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().vectors.push(entry);
    }
    for entry in merged.kv_tables {
        // kv collection name is the key (no tenant prefix here).
        let node = resolve(route_collection(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().kv_tables.push(entry);
    }
    for entry in merged.timeseries {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().timeseries.push(entry);
    }

    // Replicated-by-design: every owning node gets a copy.
    for entry in &merged.edges {
        for snap in all_owners.values_mut() {
            snap.edges.push(entry.clone());
        }
    }
    for entry in &merged.crdt_state {
        for snap in all_owners.values_mut() {
            snap.crdt_state.push(entry.clone());
        }
    }

    SplitOutput {
        buckets: all_owners,
        malformed_keys: malformed,
        route_fallbacks: fallbacks,
    }
}

fn extract_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let prefix_owned = format!("{tenant_id}:");
    let after = key.strip_prefix(prefix_owned.as_str())?;
    let coll = after.split(['\0', ':']).next()?;
    if coll.is_empty() { None } else { Some(coll) }
}

fn is_self(state: &SharedState, node_id: u64) -> bool {
    node_id == state.node_id || node_id == 0 || state.cluster_transport.is_none()
}

async fn dispatch_remote(
    state: &Arc<SharedState>,
    node_id: u64,
    tenant_id: u64,
    plan: PhysicalPlan,
) -> Result<(), Error> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| Error::Internal {
            detail: format!("restore: cluster_transport unavailable but node {node_id} is remote"),
        })?;
    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("restore: plan encode failed: {e}"),
    })?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        deadline_remaining_ms: NODE_RESTORE_TIMEOUT.as_millis() as u64,
        trace_id: TraceId::generate().0,
        descriptor_versions: Vec::new(),
    });
    let resp = transport
        .send_rpc(node_id, req)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("restore RPC to node {node_id} failed: {e}"),
        })?;
    match resp {
        RaftRpc::ExecuteResponse(ExecuteResponse { success: true, .. }) => Ok(()),
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(err), ..
        }) => Err(map_typed_error(err, node_id)),
        RaftRpc::ExecuteResponse(_) => Err(Error::Internal {
            detail: format!("restore: empty error response from node {node_id}"),
        }),
        other => Err(Error::Internal {
            detail: format!(
                "restore: unexpected RPC response variant from node {node_id}: {other:?}"
            ),
        }),
    }
}

fn map_typed_error(err: TypedClusterError, node_id: u64) -> Error {
    match err {
        TypedClusterError::Internal { message, .. } => Error::Internal {
            detail: format!("restore node {node_id}: {message}"),
        },
        TypedClusterError::DeadlineExceeded { elapsed_ms } => Error::Internal {
            detail: format!("restore node {node_id}: deadline exceeded after {elapsed_ms}ms"),
        },
        TypedClusterError::NotLeader { .. } => Error::Internal {
            detail: format!("restore node {node_id}: routed to non-leader"),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::Internal {
            detail: format!(
                "restore node {node_id}: descriptor mismatch on collection {collection}"
            ),
        },
    }
}

/// Map envelope-level errors to a generic `Error::Internal` with a
/// scrubbed message — never echoes deserializer context to the client.
fn envelope_to_err(e: EnvelopeError) -> Error {
    let msg = match e {
        EnvelopeError::TenantMismatch { expected, actual } => {
            format!("backup tenant mismatch: expected {expected}, got {actual}")
        }
        EnvelopeError::OverSizeTotal { cap } => format!("backup exceeds size cap of {cap} bytes"),
        EnvelopeError::OverSizeSection { cap } => {
            format!("backup section exceeds size cap of {cap} bytes")
        }
        EnvelopeError::UnsupportedVersion(v) => format!("unsupported backup version: {v}"),
        // Everything else — bad magic, crc mismatch, truncation — gets the
        // same generic message so attackers learn nothing about format.
        _ => "invalid backup format".to_string(),
    };
    Error::Internal { detail: msg }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_collection_strips_prefix() {
        assert_eq!(extract_collection("7:users:doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users\u{0}doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users", 7), Some("users"));
        assert_eq!(extract_collection("8:users:doc1", 7), None); // wrong tenant
        assert_eq!(extract_collection("7:", 7), None);
    }
}
