//! Calvin dispatch classification and routing for cross-shard writes.
//!
//! This module is the single chokepoint for deciding whether a set of
//! [`PhysicalTask`]s should be dispatched via:
//!
//! - The single-shard fast path (existing path, no Calvin involvement).
//! - Calvin static dispatch (all write keys known upfront).
//! - Calvin dependent-read dispatch (OLLP) (write keys depend on a pre-read).
//! - Best-effort non-atomic dispatch (each vshard independently, no atomicity).
//!
//! # Note on predicate_class
//!
//! The ideal implementation of `predicate_class` would serialize the `Filter`
//! AST via zerompk and normalize bound parameter values to their type tags.
//! However, `nodedb_sql::types::Filter` does not derive `zerompk::ToMessagePack`
//! or `zerompk::FromMessagePack`. As a declared fallback, `predicate_class`
//! accepts the canonical SQL text string (post-parse-canonicalization) and
//! normalizes numeric and string literals to their type tags before hashing.
//! This is a degraded path relative to AST-level hashing — see the
//! stop-and-report note in the checklist.

use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_cluster::calvin::sequencer::inbox::Inbox;
use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
use nodedb_types::TenantId;

use crate::Error;
use crate::bridge::physical_plan::{
    DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::planner::physical::PhysicalTask;
use crate::control::server::pgwire::session::TransactionState;
use crate::control::server::pgwire::session::cross_shard_mode::CrossShardTxnMode;
use crate::types::VShardId;
use crate::util::fnv1a_hash;

// ── DispatchClass ─────────────────────────────────────────────────────────────

/// Classification of a task set by the number of distinct write vShards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchClass {
    /// All write tasks target one vshard (or there are no writes).
    SingleShard { vshard: VShardId },
    /// Write tasks span two or more vShards — requires Calvin or best-effort.
    /// `BTreeSet` mandatory for determinism contract.
    MultiShard { vshards: BTreeSet<u32> },
}

// ── DispatchOutcome ───────────────────────────────────────────────────────────

/// Outcome returned by `dispatch_calvin_or_fast`.
#[derive(Debug)]
pub enum DispatchOutcome {
    /// Dispatched via the single-shard fast path.
    SingleShard,
    /// Submitted to the Calvin sequencer (static path).
    CalvinStatic { inbox_seq: u64 },
    /// OLLP dependent-read path submitted successfully.
    CalvinDependent { inbox_seq: u64 },
    /// Best-effort non-atomic: each vshard dispatched independently.
    BestEffortNonAtomic,
}

// ── is_write_plan ─────────────────────────────────────────────────────────────

/// Returns `true` if the plan is a write operation.
///
/// Centralizing this avoids scattered `match` arms when new write variants
/// are added. Reads, scans, and query operators return `false`.
pub fn is_write_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        // Document writes
        PhysicalPlan::Document(op) => matches!(
            op,
            DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::BatchInsert { .. }
                | DocumentOp::InsertSelect { .. }
                | DocumentOp::Upsert { .. }
                | DocumentOp::BulkUpdate { .. }
                | DocumentOp::BulkDelete { .. }
        ),
        // KV writes
        PhysicalPlan::Kv(op) => matches!(
            op,
            KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Delete { .. }
                | KvOp::BatchPut { .. }
        ),
        // Vector writes
        PhysicalPlan::Vector(op) => matches!(
            op,
            VectorOp::Insert { .. }
                | VectorOp::BatchInsert { .. }
                | VectorOp::Delete { .. }
                | VectorOp::SparseInsert { .. }
                | VectorOp::SparseDelete { .. }
                | VectorOp::MultiVectorInsert { .. }
        ),
        // Graph writes
        PhysicalPlan::Graph(op) => {
            matches!(op, GraphOp::EdgePut { .. } | GraphOp::EdgeDelete { .. })
        }
        // Timeseries writes
        PhysicalPlan::Timeseries(op) => matches!(op, TimeseriesOp::Ingest { .. }),
        // Columnar writes
        PhysicalPlan::Columnar(op) => {
            use crate::bridge::physical_plan::ColumnarOp;
            matches!(op, ColumnarOp::Insert { .. })
        }
        // CRDT writes
        PhysicalPlan::Crdt(op) => {
            use crate::bridge::physical_plan::CrdtOp;
            matches!(op, CrdtOp::ListInsert { .. } | CrdtOp::ListDelete { .. })
        }
        // Array writes
        PhysicalPlan::Array(op) => {
            use crate::bridge::physical_plan::ArrayOp;
            matches!(
                op,
                ArrayOp::Put { .. } | ArrayOp::Delete { .. } | ArrayOp::Flush { .. }
            )
        }
        // Everything else: reads, scans, queries, meta, spatial, text
        PhysicalPlan::Spatial(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_) => false,
    }
}

// ── classify_dispatch ─────────────────────────────────────────────────────────

/// Classify the dispatch class of a task slice by collecting the unique set of
/// write vShards.
///
/// 0 or 1 unique write vShards → `SingleShard`.
/// 2+ unique write vShards → `MultiShard` with the full `BTreeSet<u32>`.
pub fn classify_dispatch(tasks: &[PhysicalTask]) -> DispatchClass {
    let mut vshards: BTreeSet<u32> = BTreeSet::new();
    let mut last_vshard = None;

    for task in tasks {
        if is_write_plan(&task.plan) {
            let id = task.vshard_id.as_u32();
            vshards.insert(id);
            last_vshard = Some(task.vshard_id);
        }
    }

    match vshards.len() {
        0 => DispatchClass::SingleShard {
            vshard: tasks
                .first()
                .map(|t| t.vshard_id)
                .unwrap_or(VShardId::new(0)),
        },
        1 => DispatchClass::SingleShard {
            vshard: last_vshard.unwrap(),
        },
        _ => DispatchClass::MultiShard { vshards },
    }
}

// ── predicate_class ───────────────────────────────────────────────────────────

/// Compute a stable hash for a predicate class.
///
/// **Degraded path note**: `Filter` is not zerompk-encodable. This function
/// accepts the canonical SQL text representation of the filter and normalizes
/// numeric and string literals to their type tags before hashing. Two queries
/// with the same predicate shape but different bound values will produce the
/// same `predicate_class`. Example: `WHERE balance > 1000` and
/// `WHERE balance > 9999` both normalize to `WHERE balance > i64`.
///
/// The collection name is mixed in so predicates on different collections
/// don't collide.
pub fn predicate_class(canonical_filter_sql: &str, collection: &str) -> u64 {
    let normalized = normalize_predicate_text(canonical_filter_sql);
    let mut buf = Vec::with_capacity(collection.len() + normalized.len() + 1);
    buf.extend_from_slice(collection.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(normalized.as_bytes());
    fnv1a_hash(&buf)
}

/// Normalize a SQL text predicate by replacing literal values with type tags.
///
/// - Integer/float literals → `i64` or `f64`
/// - Quoted string literals → `str`
/// - Preserves operators, field names, and keywords
fn normalize_predicate_text(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Quoted string literal
        if c == '\'' {
            out.push_str("str");
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    i += 1;
                    // Handle escaped quote ''
                    if i < chars.len() && chars[i] == '\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Numeric literal (integer or float)
        if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let mut is_float = false;
            i += 1; // skip leading digit or minus
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                if chars[i] == '.' {
                    is_float = true;
                }
                i += 1;
            }
            if is_float {
                out.push_str("f64");
            } else {
                out.push_str("i64");
            }
            continue;
        }

        out.push(c);
        i += 1;
    }

    out
}

// ── build_static_tx_class ────────────────────────────────────────────────────

/// Build a `TxClass` from a static write task slice.
///
/// Extracts `(collection, surrogate)` pairs from each write task to build
/// `EngineKeySet`s, constructs the `ReadWriteSet`, msgpack-encodes plans into
/// `Vec<u8>`, and calls `TxClass::new`.
///
/// Returns `Err(SequencerUnavailable)` if msgpack encoding of plans fails.
pub fn build_static_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
) -> crate::Result<TxClass> {
    use std::collections::HashMap;

    // Collect surrogates per collection for write tasks.
    let mut doc_surrogates: HashMap<String, Vec<u32>> = HashMap::new();

    for task in tasks {
        if !is_write_plan(&task.plan) {
            continue;
        }
        let collection = collection_name_from_plan(&task.plan);
        let surrogate = surrogate_from_plan(&task.plan);
        doc_surrogates
            .entry(collection)
            .or_default()
            .push(surrogate);
    }

    // Build write set — one EngineKeySet per collection, sorted for
    // determinism.
    let mut write_sets: Vec<EngineKeySet> = doc_surrogates
        .into_iter()
        .map(|(collection, surrogates)| EngineKeySet::Document {
            collection,
            surrogates: SortedVec::new(surrogates),
        })
        .collect();
    // Sort by collection name for determinism.
    write_sets.sort_by(|a, b| a.collection().cmp(b.collection()));

    let write_set = ReadWriteSet::new(write_sets);
    let read_set = ReadWriteSet::new(vec![]);

    // Encode all plans as msgpack bytes.
    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin TxClass: {e}"),
    })?;

    TxClass::new(read_set, write_set, plans_bytes, tenant_id, None).map_err(|e| Error::BadRequest {
        detail: format!("invalid TxClass: {e}"),
    })
}

/// Extract the collection name from a write plan.
fn collection_name_from_plan(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { collection, .. }
            | DocumentOp::PointInsert { collection, .. }
            | DocumentOp::PointDelete { collection, .. }
            | DocumentOp::PointUpdate { collection, .. }
            | DocumentOp::BatchInsert { collection, .. }
            | DocumentOp::Upsert { collection, .. }
            | DocumentOp::BulkUpdate { collection, .. }
            | DocumentOp::BulkDelete { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Kv(
            KvOp::Put { collection, .. }
            | KvOp::Insert { collection, .. }
            | KvOp::InsertIfAbsent { collection, .. }
            | KvOp::InsertOnConflictUpdate { collection, .. }
            | KvOp::Delete { collection, .. }
            | KvOp::BatchPut { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Vector(
            VectorOp::Insert { collection, .. }
            | VectorOp::BatchInsert { collection, .. }
            | VectorOp::Delete { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Graph(
            GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => collection.clone(),
        _ => String::new(),
    }
}

/// Extract a surrogate from a write plan (returns 0 when unavailable).
fn surrogate_from_plan(plan: &PhysicalPlan) -> u32 {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { surrogate, .. }
            | DocumentOp::PointInsert { surrogate, .. }
            | DocumentOp::PointDelete { surrogate, .. }
            | DocumentOp::PointUpdate { surrogate, .. },
        ) => surrogate.as_u32(),
        _ => 0,
    }
}

// ── dispatch_calvin_or_fast ───────────────────────────────────────────────────

/// Route a set of tasks to the appropriate dispatch path.
///
/// Decision tree:
/// 1. `InBlock` + `MultiShard` → `Err(CrossShardInExplicitTransaction)`.
/// 2. `MultiShard` + `Strict` + no inbox → `Err(SequencerUnavailable)`.
/// 3. `MultiShard` + `Strict` → Calvin static path via inbox.
/// 4. `MultiShard` + `BestEffortNonAtomic` → independent per-vshard dispatch.
/// 5. `SingleShard` → existing single-shard fast path.
///
/// The single-shard and best-effort paths are modeled here as outcomes only —
/// the caller is responsible for the actual Data Plane dispatch, since this
/// module lives in the Control Plane and has no direct Data Plane handle.
pub async fn dispatch_calvin_or_fast(
    tasks: &[PhysicalTask],
    mode: CrossShardTxnMode,
    tx_state: TransactionState,
    inbox: Option<&Inbox>,
    _orchestrator: Option<&Arc<OllpOrchestrator>>,
    tenant_id: TenantId,
) -> crate::Result<DispatchOutcome> {
    let class = classify_dispatch(tasks);

    match &class {
        DispatchClass::MultiShard { .. } => {
            // Reject cross-shard writes inside explicit transaction blocks.
            if tx_state == TransactionState::InBlock {
                return Err(Error::CrossShardInExplicitTransaction);
            }

            match mode {
                CrossShardTxnMode::Strict => {
                    let inbox = inbox.ok_or(Error::SequencerUnavailable)?;
                    let tx_class = build_static_tx_class(tasks, tenant_id)?;
                    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
                        detail: format!("Calvin sequencer rejected transaction: {e}"),
                    })?;
                    Ok(DispatchOutcome::CalvinStatic { inbox_seq })
                }
                CrossShardTxnMode::BestEffortNonAtomic => Ok(DispatchOutcome::BestEffortNonAtomic),
            }
        }
        DispatchClass::SingleShard { .. } => Ok(DispatchOutcome::SingleShard),
    }
}

// ── dispatch_dependent_read ───────────────────────────────────────────────────

/// Outer retry loop for OLLP dependent-read Calvin transactions.
///
/// Calls the orchestrator's `submit_with_retry`, which runs a single attempt.
/// On `OllpError`, retries by calling `orchestrator.on_retry_required` then
/// re-submitting, up to `ollp_max_retries`.
pub async fn dispatch_dependent_read(
    orchestrator: &OllpOrchestrator,
    inbox: &Inbox,
    predicate_class_hash: u64,
    tenant_id: TenantId,
    tx_builder: impl Fn() -> crate::Result<TxClass>,
    ollp_max_retries: u8,
) -> crate::Result<u64> {
    use crate::control::cluster::calvin::executor::ollp::error::OllpError;

    let mut retry_count: u32 = 0;

    loop {
        let result = orchestrator
            .submit_with_retry(inbox, predicate_class_hash, tenant_id, || {
                tx_builder().map_err(|_e| {
                    nodedb_cluster::error::CalvinError::Sequencer(
                        nodedb_cluster::calvin::sequencer::error::SequencerError::Unavailable,
                    )
                })
            })
            .await;

        match result {
            Ok(inbox_seq) => return Ok(inbox_seq),
            Err(OllpError::CircuitOpen { .. })
            | Err(OllpError::Sequencer(_))
            | Err(OllpError::Exhausted { .. })
            | Err(OllpError::TenantBudgetExceeded { .. }) => {
                if retry_count >= ollp_max_retries as u32 {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries,
                    });
                }
                orchestrator
                    .on_retry_required(predicate_class_hash, retry_count)
                    .await;
                retry_count += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::physical_plan::{DocumentOp, PhysicalPlan};
    use crate::control::planner::physical::{PhysicalTask, PostSetOp};
    use crate::types::{TenantId, VShardId};

    fn doc_insert_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: format!("col_{vshard}"),
                document_id: "id1".to_owned(),
                surrogate: nodedb_types::Surrogate::new(1),
                value: vec![],
                if_absent: false,
            }),
            post_set_op: PostSetOp::None,
        }
    }

    fn scan_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Document(DocumentOp::Scan {
                collection: format!("col_{vshard}"),
                filters: vec![],
                limit: 0,
                offset: 0,
                sort_keys: vec![],
                distinct: false,
                projection: vec![],
                computed_columns: vec![],
                window_functions: vec![],
                system_as_of_ms: None,
                valid_at_ms: None,
                prefilter: None,
            }),
            post_set_op: PostSetOp::None,
        }
    }

    #[test]
    fn is_write_plan_classifies_correctly() {
        let write = doc_insert_task(0).plan;
        let read = scan_task(0).plan;
        assert!(is_write_plan(&write));
        assert!(!is_write_plan(&read));
    }

    #[test]
    fn classify_dispatch_single_shard() {
        let tasks = vec![doc_insert_task(5), doc_insert_task(5)];
        let class = classify_dispatch(&tasks);
        assert!(matches!(
            class,
            DispatchClass::SingleShard { vshard } if vshard.as_u32() == 5
        ));
    }

    #[test]
    fn classify_dispatch_multi_shard_returns_btreeset() {
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let class = classify_dispatch(&tasks);
        match class {
            DispatchClass::MultiShard { vshards } => {
                let v: Vec<u32> = vshards.into_iter().collect();
                assert_eq!(v, vec![3, 7]);
            }
            _ => panic!("expected MultiShard"),
        }
    }

    #[test]
    fn classify_dispatch_zero_writes_is_single_shard() {
        let tasks = vec![scan_task(3), scan_task(7)];
        let class = classify_dispatch(&tasks);
        assert!(matches!(class, DispatchClass::SingleShard { .. }));
    }

    #[test]
    fn predicate_class_byte_stable_across_runs() {
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE balance > 1000", "accounts");
        assert_eq!(h1, h2);
    }

    #[test]
    fn predicate_class_normalizes_bound_parameters() {
        // WHERE balance > 1000 and WHERE balance > 9999 must produce the same class.
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE balance > 9999", "accounts");
        assert_eq!(
            h1, h2,
            "different numeric literals should normalize to the same predicate class"
        );
    }

    #[test]
    fn dispatch_inblock_multi_shard_rejects() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
            let result = dispatch_calvin_or_fast(
                &tasks,
                CrossShardTxnMode::Strict,
                TransactionState::InBlock,
                None,
                None,
                TenantId::new(1),
            )
            .await;
            assert!(
                matches!(result, Err(Error::CrossShardInExplicitTransaction)),
                "expected CrossShardInExplicitTransaction, got {result:?}"
            );
        });
    }

    #[test]
    fn dispatch_no_inbox_returns_sequencer_unavailable() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
            let result = dispatch_calvin_or_fast(
                &tasks,
                CrossShardTxnMode::Strict,
                TransactionState::Idle,
                None, // no inbox
                None,
                TenantId::new(1),
            )
            .await;
            assert!(
                matches!(result, Err(Error::SequencerUnavailable)),
                "expected SequencerUnavailable, got {result:?}"
            );
        });
    }

    #[test]
    fn dispatch_best_effort_skips_inbox() {
        use nodedb_cluster::calvin::sequencer::config::SequencerConfig;
        use nodedb_cluster::calvin::sequencer::inbox::new_inbox;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (inbox, mut rx) = new_inbox(16, &SequencerConfig::default());
            let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
            let result = dispatch_calvin_or_fast(
                &tasks,
                CrossShardTxnMode::BestEffortNonAtomic,
                TransactionState::Idle,
                Some(&inbox),
                None,
                TenantId::new(1),
            )
            .await;
            // Must succeed with BestEffortNonAtomic outcome.
            assert!(
                matches!(result, Ok(DispatchOutcome::BestEffortNonAtomic)),
                "expected BestEffortNonAtomic, got {result:?}"
            );
            // Inbox must be empty — best-effort path must NOT call submit.
            let mut out = Vec::new();
            let drained = rx.drain_into_capped(&mut out, 10, usize::MAX);
            assert_eq!(drained, 0, "inbox should not have been called");
        });
    }
}
