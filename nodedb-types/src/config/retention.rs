//! Cross-engine bitemporal retention configuration.
//!
//! Bitemporal collections split "retention" along two independent axes:
//!
//! - `data_retain_ms` — how long the *current* logical row stays queryable
//!   in live reads (the "as of now" view). Superseded by normal
//!   engine-specific retention when the collection is not bitemporal.
//! - `audit_retain_ms` — how long *superseded versions* of a row are
//!   preserved for historical / audit-time queries (the "as of then" view).
//!
//! The minimum audit retention is a floor enforced by policy — some
//! deployments (regulated, GDPR-on-paper, SOC2) require audit history to
//! survive beyond the default so operators can't silently configure it
//! below the compliance floor.
//!
//! Lives in `nodedb-types` because multiple engines (Columnar, Array,
//! EdgeStore, DocumentStrict) compose it into their engine-specific
//! config. Engine code consults the two axes separately: data purge
//! deletes the live row when `data_retain_ms` elapses; audit purge
//! deletes superseded versions when `audit_retain_ms` elapses.

use serde::{Deserialize, Serialize};

/// Error validating a [`BitemporalRetention`].
#[derive(Debug, thiserror::Error)]
#[error("bitemporal retention: {field} — {reason}")]
pub struct RetentionValidationError {
    pub field: String,
    pub reason: String,
}

/// Per-collection bitemporal retention policy.
///
/// Two independent axes: data (live rows) and audit (superseded
/// versions). Zero on either axis means "retain forever".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BitemporalRetention {
    /// Retention for currently-live rows (versions where
    /// `_ts_valid_until == MAX`). `0` = retain forever.
    pub data_retain_ms: u64,

    /// Retention for superseded versions (versions where
    /// `_ts_valid_until < MAX`). `0` = retain forever.
    pub audit_retain_ms: u64,

    /// Policy floor for `audit_retain_ms`. Operators cannot configure
    /// `audit_retain_ms` below this value. `0` = no floor.
    pub minimum_audit_retain_ms: u64,
}

impl BitemporalRetention {
    /// Retain everything forever. Sensible conservative default for
    /// bitemporal collections where operator intent is unstated.
    pub const fn retain_forever() -> Self {
        Self {
            data_retain_ms: 0,
            audit_retain_ms: 0,
            minimum_audit_retain_ms: 0,
        }
    }

    /// Validate axis consistency and policy floor.
    pub fn validate(&self) -> Result<(), RetentionValidationError> {
        if self.minimum_audit_retain_ms > 0
            && self.audit_retain_ms > 0
            && self.audit_retain_ms < self.minimum_audit_retain_ms
        {
            return Err(RetentionValidationError {
                field: "audit_retain_ms".into(),
                reason: format!(
                    "{} is below minimum_audit_retain_ms ({})",
                    self.audit_retain_ms, self.minimum_audit_retain_ms
                ),
            });
        }
        Ok(())
    }
}

impl Default for BitemporalRetention {
    fn default() -> Self {
        Self::retain_forever()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_forever_validates() {
        assert!(BitemporalRetention::retain_forever().validate().is_ok());
    }

    #[test]
    fn audit_below_floor_rejected() {
        let r = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: 60_000,
            minimum_audit_retain_ms: 120_000,
        };
        let err = r.validate().expect_err("must reject");
        assert_eq!(err.field, "audit_retain_ms");
    }

    #[test]
    fn audit_at_or_above_floor_ok() {
        let r = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: 120_000,
            minimum_audit_retain_ms: 120_000,
        };
        assert!(r.validate().is_ok());
    }

    #[test]
    fn zero_audit_ignores_floor() {
        // audit_retain_ms == 0 means "retain forever" — floor does not apply.
        let r = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: 0,
            minimum_audit_retain_ms: 120_000,
        };
        assert!(r.validate().is_ok());
    }

    #[test]
    fn data_and_audit_are_independent() {
        let r = BitemporalRetention {
            data_retain_ms: 30_000,
            audit_retain_ms: 300_000,
            minimum_audit_retain_ms: 0,
        };
        assert!(r.validate().is_ok());
    }
}
