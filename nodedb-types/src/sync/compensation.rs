//! Typed compensation hints for rejected sync deltas.
//!
//! When the Origin rejects a CRDT delta (constraint violation, RLS, rate limit),
//! it sends a `CompensationHint` back to the edge client. The edge uses this
//! to roll back optimistic local state and notify the application with a
//! typed, actionable error — not a generic string.

use serde::{Deserialize, Serialize};

/// Typed compensation hint sent from Origin to edge when a delta is rejected.
///
/// The edge's `CompensationHandler` receives this and can programmatically
/// decide how to react (prompt user, auto-retry with suffix, silently merge).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum CompensationHint {
    /// UNIQUE constraint violated — another device wrote the same value first.
    UniqueViolation {
        /// The field that has the UNIQUE constraint (e.g., "username").
        field: String,
        /// The conflicting value that was already taken.
        conflicting_value: String,
    },

    /// Foreign key reference missing — the referenced entity doesn't exist.
    ForeignKeyMissing {
        /// The ID that was referenced but not found.
        referenced_id: String,
    },

    /// Permission denied — the user doesn't have write access.
    /// No details are leaked (security: the edge is untrusted).
    PermissionDenied,

    /// Rate limit exceeded — try again later.
    RateLimited {
        /// Suggested delay before retrying (milliseconds).
        retry_after_ms: u64,
    },

    /// Schema violation — the delta doesn't conform to the collection schema.
    SchemaViolation {
        /// Which field failed validation.
        field: String,
        /// Human-readable reason.
        reason: String,
    },

    /// Custom application-defined constraint violation.
    Custom {
        /// Constraint name.
        constraint: String,
        /// Typed payload for the application to interpret.
        detail: String,
    },
}

impl CompensationHint {
    /// Returns a short, machine-readable code for the hint type.
    pub fn code(&self) -> &'static str {
        match self {
            Self::UniqueViolation { .. } => "UNIQUE_VIOLATION",
            Self::ForeignKeyMissing { .. } => "FK_MISSING",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::RateLimited { .. } => "RATE_LIMITED",
            Self::SchemaViolation { .. } => "SCHEMA_VIOLATION",
            Self::Custom { .. } => "CUSTOM",
        }
    }
}

impl std::fmt::Display for CompensationHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UniqueViolation {
                field,
                conflicting_value,
            } => write!(
                f,
                "UNIQUE({field}): value '{conflicting_value}' already exists"
            ),
            Self::ForeignKeyMissing { referenced_id } => {
                write!(f, "FK_MISSING: referenced ID '{referenced_id}' not found")
            }
            Self::PermissionDenied => write!(f, "PERMISSION_DENIED"),
            Self::RateLimited { retry_after_ms } => {
                write!(f, "RATE_LIMITED: retry after {retry_after_ms}ms")
            }
            Self::SchemaViolation { field, reason } => {
                write!(f, "SCHEMA({field}): {reason}")
            }
            Self::Custom {
                constraint, detail, ..
            } => write!(f, "CUSTOM({constraint}): {detail}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compensation_codes() {
        assert_eq!(
            CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into()
            }
            .code(),
            "UNIQUE_VIOLATION"
        );
        assert_eq!(
            CompensationHint::PermissionDenied.code(),
            "PERMISSION_DENIED"
        );
        assert_eq!(
            CompensationHint::RateLimited {
                retry_after_ms: 5000
            }
            .code(),
            "RATE_LIMITED"
        );
    }

    #[test]
    fn compensation_display() {
        let hint = CompensationHint::UniqueViolation {
            field: "username".into(),
            conflicting_value: "alice".into(),
        };
        assert!(hint.to_string().contains("alice"));
        assert!(hint.to_string().contains("username"));
    }

    #[test]
    fn msgpack_roundtrip() {
        let hint = CompensationHint::ForeignKeyMissing {
            referenced_id: "user-42".into(),
        };
        let bytes = rmp_serde::to_vec_named(&hint).unwrap();
        let decoded: CompensationHint = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(hint, decoded);
    }
}
