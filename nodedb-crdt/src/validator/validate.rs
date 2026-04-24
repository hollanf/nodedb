//! Basic validate() + validate_or_reject() entry points.

use crate::CrdtAuthContext;
use crate::error::{CrdtError, Result};
use crate::policy::PolicyResolution;
use crate::state::CrdtState;

use super::core::Validator;
use super::types::{ProposedChange, ValidationOutcome};

impl Validator {
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
        auth: CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> Result<()> {
        // Check auth expiry: agents that accumulated deltas offline must
        // re-authenticate before syncing.
        if auth.auth_expires_at > 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if now_ms > auth.auth_expires_at {
                return Err(CrdtError::AuthExpired {
                    user_id: auth.user_id,
                    expired_at: auth.auth_expires_at,
                });
            }
        }

        // Verify delta signature if present (non-zero).
        if auth.delta_signature != [0u8; 32]
            && let Some(ref verifier) = self.delta_verifier
        {
            verifier.verify(auth.user_id, &delta_bytes, &auth.delta_signature)?;
        }

        let hlc_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        match self.validate_with_policy(state, peer_id, auth, change, delta_bytes, hlc_timestamp)? {
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
}
