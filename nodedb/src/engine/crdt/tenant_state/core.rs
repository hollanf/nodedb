//! TenantCrdtEngine core: construction, state access, delta apply, DLQ, row purge.

use loro::LoroValue;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::pre_validate::{self, PreValidationResult};
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{ProposedChange, Validator};

use crate::types::TenantId;

/// Per-tenant CRDT engine state.
pub struct TenantCrdtEngine {
    pub(super) tenant_id: TenantId,

    /// Leader's committed CRDT state for this tenant.
    pub(super) state: CrdtState,

    /// Constraint validator with DLQ and policy registry.
    pub(crate) validator: Validator,
}

impl TenantCrdtEngine {
    /// Create a new engine for a tenant with the given peer ID and constraints.
    pub fn new(
        tenant_id: TenantId,
        peer_id: u64,
        constraints: ConstraintSet,
    ) -> crate::Result<Self> {
        Ok(Self {
            tenant_id,
            state: CrdtState::new(peer_id).map_err(crate::Error::Crdt)?,
            validator: Validator::new(constraints, 1000),
        })
    }

    /// Get the peer ID for this CRDT engine.
    pub fn peer_id(&self) -> u64 {
        self.state.peer_id()
    }

    /// Access the underlying CrdtState (for advanced operations like list ops).
    pub fn state(&self) -> &CrdtState {
        &self.state
    }

    /// Export the full CRDT state as binary bytes (for snapshot transfer).
    pub fn export_snapshot_bytes(&self) -> crate::Result<Vec<u8>> {
        self.state.export_snapshot().map_err(crate::Error::Crdt)
    }

    /// Read a document's CRDT state, returning the raw snapshot bytes.
    pub fn read_snapshot(&self, collection: &str, row_id: &str) -> crate::Result<Option<Vec<u8>>> {
        if self.state.row_exists(collection, row_id) {
            Ok(Some(
                self.state.export_snapshot().map_err(crate::Error::Crdt)?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Read a single row's fields as a `LoroValue`.
    ///
    /// Returns the deep value of the row (all nested containers resolved),
    /// or `None` if the row does not exist.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        self.state.read_row(collection, row_id)
    }

    /// Pre-validate a proposed change (fast-reject before Raft).
    pub fn pre_validate(&self, change: &ProposedChange) -> PreValidationResult {
        pre_validate::pre_validate(&self.validator, &self.state, change)
    }

    /// Import a full CRDT snapshot (for snapshot restore).
    pub fn import_snapshot_bytes(&self, bytes: &[u8]) -> crate::Result<()> {
        self.state.import(bytes).map_err(crate::Error::Crdt)
    }

    /// Apply a validated delta from Raft commit.
    ///
    /// This is called AFTER Raft consensus — the delta has been committed
    /// to the Raft log and now needs to be applied to the local state.
    pub fn apply_committed_delta(&self, delta: &[u8]) -> crate::Result<()> {
        self.state.import(delta).map_err(crate::Error::Crdt)
    }

    /// Validate and attempt to apply a delta from a peer.
    ///
    /// If constraints are violated, the delta is routed to the DLQ.
    /// Returns `Ok(())` on success, or the constraint violation error.
    ///
    /// For bitemporal collections, `_ts_system` is always stamped with the
    /// receiving node's clock, overwriting any value the sender supplied.
    /// This keeps system-time receiver-authoritative so convergence does
    /// not depend on clock agreement between peers.
    pub fn validate_and_apply(
        &mut self,
        peer_id: u64,
        auth: nodedb_crdt::CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> crate::Result<()> {
        self.validator
            .validate_or_reject(&self.state, peer_id, auth, change, delta_bytes)
            .map_err(crate::Error::Crdt)?;

        let is_bitemporal = self.validator.is_bitemporal(&change.collection);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut fields: Vec<(&str, LoroValue)> = change
            .fields
            .iter()
            .filter(|(k, _)| !(is_bitemporal && k == "_ts_system"))
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();

        if is_bitemporal {
            fields.push(("_ts_system", LoroValue::I64(now_ms)));
            self.state
                .upsert_versioned(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        } else {
            self.state
                .upsert(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        }
    }

    /// Drop archived bitemporal versions older than `cutoff_system_ms`
    /// for the given collection. The live row is never touched. Called
    /// from the Data Plane purge handler.
    pub fn purge_history_before(
        &self,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        self.state
            .purge_history_before(collection, cutoff_system_ms)
            .map_err(crate::Error::Crdt)
    }

    /// Set the conflict-resolution policy for a collection from a typed
    /// `CollectionPolicy`. The JSON-accepting variant in `policy.rs` is the
    /// DDL-facing path; this one is for in-process callers (tests, engine
    /// setup).
    pub fn set_collection_policy_typed(
        &mut self,
        collection: &str,
        policy: nodedb_crdt::policy::CollectionPolicy,
    ) {
        self.validator.policies_mut().set(collection, policy);
    }

    /// Register a collection as bitemporal on this tenant's validator.
    ///
    /// Bitemporal collections get (a) UNIQUE constraints scoped to live
    /// rows only and (b) receiver-stamped `_ts_system` on apply.
    pub fn mark_bitemporal(&mut self, collection: impl Into<String>) {
        self.validator.mark_bitemporal(collection);
    }

    /// Is the named collection bitemporal?
    pub fn is_bitemporal(&self, collection: &str) -> bool {
        self.validator.is_bitemporal(collection)
    }

    /// Number of entries in the dead-letter queue.
    pub fn dlq_len(&self) -> usize {
        self.validator.dlq().len()
    }

    /// Purge all CRDT state for a single collection.
    ///
    /// Three things happen:
    /// 1. Every row in the loro map for this collection is cleared.
    /// 2. The collection's conflict-resolution policy is removed from
    ///    the policy registry.
    /// 3. Any dead-letter entries (rejected deltas) scoped to this
    ///    collection are dropped — otherwise a re-created collection
    ///    of the same name would inherit unrelated rejected deltas.
    ///
    /// Returns the number of CRDT rows removed. Idempotent.
    pub fn purge_collection(&mut self, collection: &str) -> crate::Result<usize> {
        let removed = self
            .state
            .clear_collection(collection)
            .map_err(crate::Error::Crdt)?;
        self.validator.policies_mut().remove(collection);
        let dlq_dropped = self
            .validator
            .dlq_mut()
            .purge_collection(self.tenant_id.as_u32(), collection);
        if dlq_dropped > 0 {
            tracing::debug!(
                tenant = self.tenant_id.as_u32(),
                collection,
                dlq_dropped,
                "crdt: dropped DLQ entries scoped to purged collection"
            );
        }
        Ok(removed)
    }

    /// Check if a row exists in a collection.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.state.row_exists(collection, row_id)
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
}
