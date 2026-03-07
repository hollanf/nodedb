//! Constraint validator — checks deltas against committed state.
//!
//! This is the core of the CRDT/SQL bridge. When a delta arrives from a peer,
//! the validator checks all applicable constraints against the leader's
//! committed state. If any constraint is violated, the delta is rejected
//! with a compensation hint.

use crate::constraint::{Constraint, ConstraintKind, ConstraintSet};
use crate::dead_letter::{CompensationHint, DeadLetterQueue};
use crate::error::{CrdtError, Result};
use crate::state::CrdtState;

use loro::LoroValue;

/// Outcome of validating a proposed change against constraints.
#[derive(Debug)]
pub enum ValidationOutcome {
    /// All constraints satisfied — safe to commit.
    Accepted,
    /// One or more constraints violated — delta rejected.
    Rejected(Vec<Violation>),
}

/// A single constraint violation.
#[derive(Debug, Clone)]
pub struct Violation {
    /// The constraint that was violated.
    pub constraint_name: String,
    /// Human-readable reason.
    pub reason: String,
    /// Suggested fix.
    pub hint: CompensationHint,
}

/// A proposed row change to validate.
#[derive(Debug, Clone)]
pub struct ProposedChange {
    /// Target collection.
    pub collection: String,
    /// Row ID being inserted/updated.
    pub row_id: String,
    /// Field values being set.
    pub fields: Vec<(String, LoroValue)>,
}

/// The constraint validator.
///
/// Validates proposed changes against a set of constraints and the current
/// committed state. Rejected changes are routed to a dead-letter queue.
pub struct Validator {
    constraints: ConstraintSet,
    dlq: DeadLetterQueue,
}

impl Validator {
    pub fn new(constraints: ConstraintSet, dlq_capacity: usize) -> Self {
        Self {
            constraints,
            dlq: DeadLetterQueue::new(dlq_capacity),
        }
    }

    /// Validate a proposed change against all applicable constraints.
    ///
    /// Returns `Accepted` if all constraints pass, or `Rejected` with
    /// detailed violation information.
    pub fn validate(&self, state: &CrdtState, change: &ProposedChange) -> ValidationOutcome {
        let constraints = self.constraints.for_collection(&change.collection);
        let mut violations = Vec::new();

        for constraint in constraints {
            if let Some(violation) = self.check_constraint(state, change, constraint) {
                violations.push(violation);
            }
        }

        if violations.is_empty() {
            ValidationOutcome::Accepted
        } else {
            ValidationOutcome::Rejected(violations)
        }
    }

    /// Validate and, if rejected, automatically enqueue to the DLQ.
    ///
    /// Returns `Ok(())` if accepted, or `Err` if rejected (delta is in the DLQ).
    pub fn validate_or_reject(
        &mut self,
        state: &CrdtState,
        peer_id: u64,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> Result<()> {
        match self.validate(state, change) {
            ValidationOutcome::Accepted => Ok(()),
            ValidationOutcome::Rejected(violations) => {
                // Enqueue the first violation to the DLQ.
                let v = &violations[0];
                let constraint = self
                    .constraints
                    .all()
                    .iter()
                    .find(|c| c.name == v.constraint_name)
                    .cloned()
                    .unwrap_or_else(|| crate::constraint::Constraint {
                        name: v.constraint_name.clone(),
                        collection: change.collection.clone(),
                        field: String::new(),
                        kind: ConstraintKind::NotNull,
                    });

                self.dlq.enqueue(
                    peer_id,
                    delta_bytes,
                    &constraint,
                    v.reason.clone(),
                    v.hint.clone(),
                )?;

                Err(CrdtError::ConstraintViolation {
                    constraint: v.constraint_name.clone(),
                    collection: change.collection.clone(),
                    detail: v.reason.clone(),
                })
            }
        }
    }

    /// Access the dead-letter queue.
    pub fn dlq(&self) -> &DeadLetterQueue {
        &self.dlq
    }

    /// Mutable access to the DLQ (for dequeue/retry).
    pub fn dlq_mut(&mut self) -> &mut DeadLetterQueue {
        &mut self.dlq
    }

    fn check_constraint(
        &self,
        state: &CrdtState,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        match &constraint.kind {
            ConstraintKind::Unique => self.check_unique(state, change, constraint),
            ConstraintKind::ForeignKey {
                ref_collection,
                ref_key,
            } => self.check_foreign_key(state, change, constraint, ref_collection, ref_key),
            ConstraintKind::NotNull => self.check_not_null(change, constraint),
            ConstraintKind::Check { .. } => {
                // Custom checks are application-defined; we can't evaluate them
                // generically. They'd be registered as closures in a real impl.
                None
            }
        }
    }

    fn check_unique(
        &self,
        state: &CrdtState,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        let value = &field_value.1;

        // Check if this value already exists in another row.
        if state.field_value_exists(&change.collection, &constraint.field, value) {
            let value_str = format!("{:?}", value);
            Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!(
                    "value {} for field `{}` already exists in `{}`",
                    value_str, constraint.field, constraint.collection
                ),
                hint: CompensationHint::RetryWithDifferentValue {
                    field: constraint.field.clone(),
                    conflicting_value: value_str.clone(),
                    suggestion: format!("{value_str}-dedup"),
                },
            })
        } else {
            None
        }
    }

    fn check_foreign_key(
        &self,
        state: &CrdtState,
        change: &ProposedChange,
        constraint: &Constraint,
        ref_collection: &str,
        ref_key: &str,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field)?;

        // The FK value should reference an existing row_id in the ref collection.
        let ref_id = match &field_value.1 {
            LoroValue::String(s) => s.to_string(),
            LoroValue::I64(n) => n.to_string(),
            other => format!("{:?}", other),
        };

        if !state.row_exists(ref_collection, &ref_id) {
            Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!(
                    "foreign key `{}` references `{}.{}` = `{}` which does not exist",
                    constraint.field, ref_collection, ref_key, ref_id
                ),
                hint: CompensationHint::CreateReferencedRow {
                    ref_collection: ref_collection.to_string(),
                    ref_key: ref_key.to_string(),
                    missing_value: ref_id,
                },
            })
        } else {
            None
        }
    }

    fn check_not_null(
        &self,
        change: &ProposedChange,
        constraint: &Constraint,
    ) -> Option<Violation> {
        let field_value = change.fields.iter().find(|(f, _)| f == &constraint.field);

        match field_value {
            None => Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!("field `{}` is required but not provided", constraint.field),
                hint: CompensationHint::ProvideRequiredField {
                    field: constraint.field.clone(),
                },
            }),
            Some((_, LoroValue::Null)) => Some(Violation {
                constraint_name: constraint.name.clone(),
                reason: format!("field `{}` must not be null", constraint.field),
                hint: CompensationHint::ProvideRequiredField {
                    field: constraint.field.clone(),
                },
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (CrdtState, ConstraintSet) {
        let state = CrdtState::new(1);
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        cs.add_not_null("users_name_nn", "users", "name");
        cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
        (state, cs)
    }

    #[test]
    fn valid_insert_accepted() {
        let (state, cs) = setup();
        let validator = Validator::new(cs, 100);

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

        assert!(matches!(
            validator.validate(&state, &change),
            ValidationOutcome::Accepted
        ));
    }

    #[test]
    fn not_null_violation() {
        let (_state, cs) = setup();
        let validator = Validator::new(cs, 100);
        let state = CrdtState::new(1);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![
                // Missing "name" field — violates NOT NULL.
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };

        match validator.validate(&state, &change) {
            ValidationOutcome::Rejected(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].constraint_name, "users_name_nn");
                assert!(matches!(
                    v[0].hint,
                    CompensationHint::ProvideRequiredField { .. }
                ));
            }
            _ => panic!("expected rejection"),
        }
    }

    #[test]
    fn foreign_key_violation() {
        let (state, cs) = setup();
        let validator = Validator::new(cs, 100);

        // No users exist — FK to users.id should fail.
        let change = ProposedChange {
            collection: "posts".into(),
            row_id: "p1".into(),
            fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
        };

        match validator.validate(&state, &change) {
            ValidationOutcome::Rejected(v) => {
                assert_eq!(v[0].constraint_name, "posts_author_fk");
                assert!(matches!(
                    v[0].hint,
                    CompensationHint::CreateReferencedRow { .. }
                ));
            }
            _ => panic!("expected FK violation"),
        }
    }

    #[test]
    fn foreign_key_passes_when_parent_exists() {
        let (state, cs) = setup();
        let validator = Validator::new(cs, 100);

        // Create the referenced user first.
        state
            .upsert(
                "users",
                "u1",
                &[
                    ("name", LoroValue::String("Alice".into())),
                    ("email", LoroValue::String("a@b.com".into())),
                ],
            )
            .unwrap();

        let change = ProposedChange {
            collection: "posts".into(),
            row_id: "p1".into(),
            fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
        };

        assert!(matches!(
            validator.validate(&state, &change),
            ValidationOutcome::Accepted
        ));
    }

    #[test]
    fn validate_or_reject_enqueues_to_dlq() {
        let (state, cs) = setup();
        let mut validator = Validator::new(cs, 100);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![
                // Missing "name" — violates NOT NULL.
                ("email".into(), LoroValue::String("a@b.com".into())),
            ],
        };

        let err = validator
            .validate_or_reject(&state, 42, &change, b"delta-bytes".to_vec())
            .unwrap_err();

        assert!(matches!(err, CrdtError::ConstraintViolation { .. }));
        assert_eq!(validator.dlq().len(), 1);

        let dl = validator.dlq().peek().unwrap();
        assert_eq!(dl.peer_id, 42);
        assert_eq!(dl.violated_constraint, "users_name_nn");
    }
}
