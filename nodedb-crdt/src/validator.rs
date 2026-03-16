//! Constraint validator — checks deltas against committed state.
//!
//! This is the core of the CRDT/SQL bridge. When a delta arrives from a peer,
//! the validator checks all applicable constraints against the leader's
//! committed state. If any constraint is violated, the delta is rejected
//! with a compensation hint.

use crate::constraint::{Constraint, ConstraintKind, ConstraintSet};
use crate::dead_letter::{CompensationHint, DeadLetterQueue};
use crate::deferred::DeferredQueue;
use crate::error::{CrdtError, Result};
use crate::policy::{ConflictPolicy, PolicyRegistry, PolicyResolution, ResolvedAction};
use crate::state::CrdtState;

use loro::LoroValue;
use std::collections::HashMap;

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
/// committed state. Violations are resolved via declarative policies:
/// - AUTO_RESOLVED — policy handles it (e.g., LAST_WRITER_WINS)
/// - DEFERRED — queued for exponential backoff retry (CASCADE_DEFER)
/// - WEBHOOK_REQUIRED — caller must POST to webhook for decision
/// - ESCALATE — route to dead-letter queue (fallback)
pub struct Validator {
    constraints: ConstraintSet,
    dlq: DeadLetterQueue,
    policies: PolicyRegistry,
    deferred: DeferredQueue,
    /// Monotonic suffix counter: (collection, field) -> next suffix number
    suffix_counter: HashMap<(String, String), u64>,
}

impl Validator {
    /// Create a new validator with default (ephemeral) policies.
    pub fn new(constraints: ConstraintSet, dlq_capacity: usize) -> Self {
        Self::new_with_policies(constraints, dlq_capacity, PolicyRegistry::new(), 1000)
    }

    /// Create a new validator with custom policies and deferred queue.
    pub fn new_with_policies(
        constraints: ConstraintSet,
        dlq_capacity: usize,
        policies: PolicyRegistry,
        deferred_capacity: usize,
    ) -> Self {
        Self {
            constraints,
            dlq: DeadLetterQueue::new(dlq_capacity),
            policies,
            deferred: DeferredQueue::new(deferred_capacity),
            suffix_counter: HashMap::new(),
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

    /// Validate and apply declarative policy resolution.
    ///
    /// This is the modern API. It uses validate_with_policy internally.
    /// For accepted changes, returns Ok(()).
    /// For violations, applies policy and:
    /// - If AutoResolved: returns Ok(())
    /// - If Deferred/Webhook/Escalate: returns appropriate error
    pub fn validate_or_reject(
        &mut self,
        state: &CrdtState,
        peer_id: u64,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> Result<()> {
        // Use a dummy HLC timestamp (caller should upgrade to validate_with_policy for proper semantics)
        let hlc_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        match self.validate_with_policy(state, peer_id, change, delta_bytes, hlc_timestamp)? {
            PolicyResolution::AutoResolved(_) => Ok(()),
            PolicyResolution::Deferred { .. } => {
                // Violation was deferred for retry; return error to signal this
                // The deferred entry was already enqueued by validate_with_policy
                let violations = match self.validate(state, change) {
                    ValidationOutcome::Rejected(v) => v,
                    _ => vec![],
                };
                if !violations.is_empty() {
                    let v = &violations[0];
                    Err(CrdtError::ConstraintViolation {
                        constraint: v.constraint_name.clone(),
                        collection: change.collection.clone(),
                        detail: format!("{} (deferred for retry)", v.reason),
                    })
                } else {
                    Ok(())
                }
            }
            PolicyResolution::WebhookRequired { .. } => {
                // Webhook decision required; return error
                let violations = match self.validate(state, change) {
                    ValidationOutcome::Rejected(v) => v,
                    _ => vec![],
                };
                if !violations.is_empty() {
                    let v = &violations[0];
                    Err(CrdtError::ConstraintViolation {
                        constraint: v.constraint_name.clone(),
                        collection: change.collection.clone(),
                        detail: format!("{} (webhook required)", v.reason),
                    })
                } else {
                    Ok(())
                }
            }
            PolicyResolution::Escalate => {
                // Already enqueued to DLQ by validate_with_policy
                let violations = match self.validate(state, change) {
                    ValidationOutcome::Rejected(v) => v,
                    _ => vec![],
                };
                if !violations.is_empty() {
                    let v = &violations[0];
                    Err(CrdtError::ConstraintViolation {
                        constraint: v.constraint_name.clone(),
                        collection: change.collection.clone(),
                        detail: v.reason.clone(),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Validate with declarative policy resolution.
    ///
    /// This is the new core validation method. It attempts to resolve violations
    /// via policy before falling back to the DLQ.
    ///
    /// # Arguments
    ///
    /// * `state` — current CRDT state
    /// * `peer_id` — source peer ID
    /// * `change` — proposed change
    /// * `delta_bytes` — raw delta bytes
    /// * `hlc_timestamp` — Hybrid Logical Clock timestamp of the incoming write
    ///
    /// Returns:
    /// - `Ok(PolicyResolution::AutoResolved(_))` if the policy auto-fixed the violation
    /// - `Ok(PolicyResolution::Deferred { .. })` if deferred for retry (entry already enqueued)
    /// - `Ok(PolicyResolution::WebhookRequired { .. })` if webhook call needed (caller's responsibility)
    /// - `Ok(PolicyResolution::Escalate)` if escalating to DLQ (entry already enqueued)
    /// - `Err(_)` if an internal error occurred
    pub fn validate_with_policy(
        &mut self,
        state: &CrdtState,
        peer_id: u64,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
        hlc_timestamp: u64,
    ) -> Result<PolicyResolution> {
        match self.validate(state, change) {
            ValidationOutcome::Accepted => {
                // No violation; return synthetic "auto-resolved" to maintain API consistency
                Ok(PolicyResolution::AutoResolved(
                    ResolvedAction::OverwriteExisting,
                ))
            }
            ValidationOutcome::Rejected(violations) => {
                // Exactly one violation per constraint (current design)
                let v = &violations[0];
                let constraint = self
                    .constraints
                    .all()
                    .iter()
                    .find(|c| c.name == v.constraint_name)
                    .cloned()
                    .unwrap_or_else(|| Constraint {
                        name: v.constraint_name.clone(),
                        collection: change.collection.clone(),
                        field: String::new(),
                        kind: ConstraintKind::NotNull,
                    });

                let policy = self.policies.get_owned(&change.collection);
                let policy_for_kind = policy.for_kind(&constraint.kind);

                // Attempt policy resolution
                match policy_for_kind {
                    ConflictPolicy::LastWriterWins => {
                        // LWW: incoming write always wins; log audit entry
                        tracing::info!(
                            constraint = %v.constraint_name,
                            collection = %change.collection,
                            timestamp = hlc_timestamp,
                            reason = %v.reason,
                            "resolved via LAST_WRITER_WINS"
                        );
                        Ok(PolicyResolution::AutoResolved(
                            ResolvedAction::OverwriteExisting,
                        ))
                    }

                    ConflictPolicy::RenameSuffix => {
                        // Auto-rename conflicting field
                        let counter_key = (change.collection.clone(), constraint.field.clone());
                        let suffix = self.suffix_counter.entry(counter_key).or_insert(0);
                        *suffix += 1;
                        let new_value = format!(
                            "{}_{}",
                            change
                                .fields
                                .iter()
                                .find(|(f, _)| f == &constraint.field)
                                .map(|(_, v)| format!("{:?}", v))
                                .unwrap_or_else(|| "unknown".to_string()),
                            suffix
                        );

                        tracing::info!(
                            constraint = %v.constraint_name,
                            field = %constraint.field,
                            new_value = %new_value,
                            "resolved via RENAME_APPEND_SUFFIX"
                        );

                        Ok(PolicyResolution::AutoResolved(
                            ResolvedAction::RenamedField {
                                field: constraint.field.clone(),
                                new_value,
                            },
                        ))
                    }

                    ConflictPolicy::CascadeDefer {
                        max_retries,
                        ttl_secs,
                    } => {
                        // Enqueue for exponential backoff retry
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        let base_ms = 500u64;
                        let first_retry_after_ms = base_ms;

                        let id = self.deferred.enqueue(
                            peer_id,
                            delta_bytes,
                            change.collection.clone(),
                            constraint.name.clone(),
                            0,
                            *max_retries,
                            now_ms,
                            first_retry_after_ms,
                            *ttl_secs,
                        );

                        tracing::info!(
                            constraint = %v.constraint_name,
                            deferred_id = id,
                            reason = %v.reason,
                            "resolved via CASCADE_DEFER (queued for retry)"
                        );

                        Ok(PolicyResolution::Deferred {
                            retry_after_ms: first_retry_after_ms,
                            attempt: 0,
                        })
                    }

                    ConflictPolicy::Custom {
                        webhook_url,
                        timeout_secs,
                    } => {
                        // Webhook decision required
                        tracing::info!(
                            constraint = %v.constraint_name,
                            webhook_url = %webhook_url,
                            "escalated to webhook"
                        );

                        Ok(PolicyResolution::WebhookRequired {
                            webhook_url: webhook_url.clone(),
                            timeout_secs: *timeout_secs,
                        })
                    }

                    ConflictPolicy::EscalateToDlq => {
                        // Explicit fallback to DLQ
                        self.dlq.enqueue(
                            peer_id,
                            delta_bytes,
                            &constraint,
                            v.reason.clone(),
                            v.hint.clone(),
                        )?;

                        tracing::info!(
                            constraint = %v.constraint_name,
                            collection = %change.collection,
                            "escalated to DLQ"
                        );

                        Ok(PolicyResolution::Escalate)
                    }
                }
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

    /// Access the policy registry.
    pub fn policies(&self) -> &PolicyRegistry {
        &self.policies
    }

    /// Mutable access to the policy registry.
    pub fn policies_mut(&mut self) -> &mut PolicyRegistry {
        &mut self.policies
    }

    /// Access the deferred queue.
    pub fn deferred(&self) -> &DeferredQueue {
        &self.deferred
    }

    /// Mutable access to the deferred queue.
    pub fn deferred_mut(&mut self) -> &mut DeferredQueue {
        &mut self.deferred
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
        // Create policies with strict mode (escalates to DLQ)
        let policies = crate::policy::PolicyRegistry::new();
        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        // Set a strict policy for users collection
        let strict_policy = crate::policy::CollectionPolicy::strict();
        validator.policies_mut().set("users", strict_policy);

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

    #[test]
    fn validate_with_policy_last_writer_wins() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        policies.set("users", crate::policy::CollectionPolicy::ephemeral());

        // Override to use explicit LWW
        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.not_null = crate::policy::ConflictPolicy::LastWriterWins;
        policies.set("users", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![
                // Missing "name" — should be resolved via LWW
                ("email".into(), LoroValue::String("a@b.com".into())),
            ],
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let resolution = validator
            .validate_with_policy(&state, 42, &change, b"delta".to_vec(), now)
            .unwrap();

        assert!(matches!(
            resolution,
            PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
        ));
    }

    #[test]
    fn validate_with_policy_rename_suffix() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.unique = crate::policy::ConflictPolicy::RenameSuffix;
        policies.set("users", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        // First user exists
        state
            .upsert(
                "users",
                "u1",
                &[
                    ("name", LoroValue::String("Alice".into())),
                    ("email", LoroValue::String("alice@example.com".into())),
                ],
            )
            .unwrap();

        // Second user tries same email — should be auto-renamed
        let change = ProposedChange {
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

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let resolution = validator
            .validate_with_policy(&state, 42, &change, b"delta".to_vec(), now)
            .unwrap();

        match resolution {
            PolicyResolution::AutoResolved(ResolvedAction::RenamedField { field, new_value }) => {
                assert_eq!(field, "email");
                assert!(new_value.contains("_1"));
            }
            _ => panic!("expected RenamedField resolution"),
        }
    }

    #[test]
    fn validate_with_policy_cascade_defer() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.foreign_key = crate::policy::ConflictPolicy::CascadeDefer {
            max_retries: 3,
            ttl_secs: 60,
        };
        policies.set("posts", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        // FK to non-existent user
        let change = ProposedChange {
            collection: "posts".into(),
            row_id: "p1".into(),
            fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let resolution = validator
            .validate_with_policy(&state, 42, &change, b"delta".to_vec(), now)
            .unwrap();

        match resolution {
            PolicyResolution::Deferred {
                retry_after_ms,
                attempt,
            } => {
                assert_eq!(attempt, 0);
                assert_eq!(retry_after_ms, 500); // base backoff
            }
            _ => panic!("expected Deferred resolution"),
        }

        // Verify entry was enqueued to deferred queue
        assert_eq!(validator.deferred().len(), 1);
    }

    #[test]
    fn validate_with_policy_escalate_to_dlq() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.not_null = crate::policy::ConflictPolicy::EscalateToDlq;
        policies.set("users", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let resolution = validator
            .validate_with_policy(&state, 42, &change, b"delta".to_vec(), now)
            .unwrap();

        assert!(matches!(resolution, PolicyResolution::Escalate));
        assert_eq!(validator.dlq().len(), 1);
    }
}
