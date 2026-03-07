use loro::LoroValue;

use synapsedb_crdt::constraint::ConstraintSet;
use synapsedb_crdt::pre_validate::{self, PreValidationResult};
use synapsedb_crdt::state::CrdtState;
use synapsedb_crdt::validator::{ProposedChange, Validator};

use crate::types::TenantId;

/// Per-tenant CRDT engine state.
///
/// Manages the loro-backed CRDT state, constraint validation, and dead-letter
/// queue for a single tenant. Lives on the Data Plane (one per tenant per core).
pub struct TenantCrdtEngine {
    tenant_id: TenantId,

    /// Leader's committed CRDT state for this tenant.
    state: CrdtState,

    /// Constraint validator with DLQ.
    validator: Validator,
}

impl TenantCrdtEngine {
    /// Create a new engine for a tenant with the given peer ID and constraints.
    pub fn new(tenant_id: TenantId, peer_id: u64, constraints: ConstraintSet) -> Self {
        Self {
            tenant_id,
            state: CrdtState::new(peer_id),
            validator: Validator::new(constraints, 1000),
        }
    }

    /// Read a document's CRDT state, returning the raw snapshot bytes.
    pub fn read_snapshot(&self, collection: &str, row_id: &str) -> Option<Vec<u8>> {
        if self.state.row_exists(collection, row_id) {
            Some(self.state.export_snapshot())
        } else {
            None
        }
    }

    /// Pre-validate a proposed change (fast-reject before Raft).
    pub fn pre_validate(&self, change: &ProposedChange) -> PreValidationResult {
        pre_validate::pre_validate(&self.validator, &self.state, change)
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
    pub fn validate_and_apply(
        &mut self,
        peer_id: u64,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> crate::Result<()> {
        self.validator
            .validate_or_reject(&self.state, peer_id, change, delta_bytes)
            .map_err(crate::Error::Crdt)?;

        // Validation passed — apply to state.
        // In production this would apply the delta bytes, but for now
        // we upsert the fields directly since we have the ProposedChange.
        let fields: Vec<(&str, LoroValue)> = change
            .fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();

        self.state
            .upsert(&change.collection, &change.row_id, &fields)
            .map_err(crate::Error::Crdt)
    }

    /// Number of entries in the dead-letter queue.
    pub fn dlq_len(&self) -> usize {
        self.validator.dlq().len()
    }

    /// Check if a row exists in a collection.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.state.row_exists(collection, row_id)
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_constraints() -> ConstraintSet {
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        cs.add_not_null("users_name_nn", "users", "name");
        cs
    }

    #[test]
    fn valid_write_applies() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints());

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };

        engine
            .validate_and_apply(1, &change, b"delta".to_vec())
            .unwrap();

        assert!(engine.row_exists("users", "u1"));
        assert_eq!(engine.dlq_len(), 0);
    }

    #[test]
    fn constraint_violation_routes_to_dlq() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints());

        // Missing "name" field violates NOT NULL.
        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        let err = engine
            .validate_and_apply(42, &change, b"delta".to_vec())
            .unwrap_err();

        assert!(matches!(err, crate::Error::Crdt(_)));
        assert_eq!(engine.dlq_len(), 1);
    }

    #[test]
    fn pre_validate_fast_rejects() {
        let engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints());

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        match engine.pre_validate(&change) {
            PreValidationResult::FastReject { constraint, .. } => {
                assert_eq!(constraint, "users_name_nn");
            }
            _ => panic!("expected fast reject"),
        }
    }

    #[test]
    fn unique_violation_after_first_write() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints());

        let first = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };
        engine
            .validate_and_apply(1, &first, b"d1".to_vec())
            .unwrap();

        // Second write with same email should fail.
        let second = ProposedChange {
            collection: "users".into(),
            row_id: "u2".into(),
            fields: vec![
                ("name".into(), LoroValue::String("Bob".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };
        assert!(
            engine
                .validate_and_apply(2, &second, b"d2".to_vec())
                .is_err()
        );
        assert_eq!(engine.dlq_len(), 1);
    }
}
