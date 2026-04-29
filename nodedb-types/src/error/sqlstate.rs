//! Standard PostgreSQL SQLSTATE code constants.
//!
//! A single source of truth for every five-character SQLSTATE string used
//! across the codebase. Grouping follows the PostgreSQL documentation
//! appendix (Class 00–XX).  All constants are `&'static str` so they compose
//! directly with `pgwire::error::ErrorInfo` without any allocation.
//!
//! Add new codes here when a new error path needs one; never inline a literal
//! elsewhere — a typo in a SQLSTATE string is undetectable at compile time.

// ── Class 00 — Successful Completion ────────────────────────────────────────

/// `00000` — `successful_completion`
pub const SUCCESS: &str = "00000";

// ── Class 01 — Warning ───────────────────────────────────────────────────────

/// `01000` — `warning` (generic warning)
pub const WARNING: &str = "01000";

// ── Class 02 — No Data ───────────────────────────────────────────────────────

/// `02000` — `no_data` (document / row not found)
pub const NO_DATA: &str = "02000";

// ── Class 0A — Feature Not Supported ────────────────────────────────────────

/// `0A000` — `feature_not_supported`
pub const FEATURE_NOT_SUPPORTED: &str = "0A000";

// ── Class 22 — Data Exception ────────────────────────────────────────────────

/// `22003` — `numeric_value_out_of_range`
pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";

// ── Class 23 — Integrity Constraint Violation ────────────────────────────────

/// `23000` — `integrity_constraint_violation` (generic)
pub const INTEGRITY_CONSTRAINT_VIOLATION: &str = "23000";

/// `23502` — `not_null_violation`
pub const NOT_NULL_VIOLATION: &str = "23502";

/// `23503` — `foreign_key_violation` (dangling-edge rejection)
pub const FOREIGN_KEY_VIOLATION: &str = "23503";

/// `23505` — `unique_violation`
pub const UNIQUE_VIOLATION: &str = "23505";

/// `23514` — `check_violation`
pub const CHECK_VIOLATION: &str = "23514";

/// `23601` — NodeDB extension: append-only write rejected.
pub const APPEND_ONLY_VIOLATION: &str = "23601";

/// `23602` — NodeDB extension: balance constraint violated.
pub const BALANCE_VIOLATION: &str = "23602";

/// `23603` — NodeDB extension: period lock; writes rejected.
pub const PERIOD_LOCKED: &str = "23603";

/// `23604` — NodeDB extension: state-transition constraint violated.
pub const STATE_TRANSITION_VIOLATION: &str = "23604";

/// `23605` — NodeDB extension: transition-check constraint violated.
pub const TRANSITION_CHECK_VIOLATION: &str = "23605";

/// `23606` — NodeDB extension: retention policy blocks deletion.
pub const RETENTION_VIOLATION: &str = "23606";

/// `23607` — NodeDB extension: legal hold blocks deletion.
pub const LEGAL_HOLD_ACTIVE: &str = "23607";

/// `23608` — NodeDB extension: type-guard constraint violated.
pub const TYPE_GUARD_VIOLATION: &str = "23608";

// ── Class 28 — Invalid Authorization Specification ───────────────────────────

/// `28000` — `invalid_authorization_specification` (no valid credentials)
pub const INVALID_AUTHORIZATION: &str = "28000";

// ── Class 40 — Transaction Rollback ──────────────────────────────────────────

/// `40001` — `serialization_failure` (write conflict; client should retry)
pub const SERIALIZATION_FAILURE: &str = "40001";

// ── Class 42 — Syntax Error or Access Rule Violation ─────────────────────────

/// `42501` — `insufficient_privilege`
pub const INSUFFICIENT_PRIVILEGE: &str = "42501";

/// `42601` — `syntax_error`
pub const SYNTAX_ERROR: &str = "42601";

/// `42846` — `cannot_coerce`
pub const CANNOT_COERCE: &str = "42846";

/// `42P01` — `undefined_table` (collection not found)
pub const UNDEFINED_TABLE: &str = "42P01";

// ── Class 53 — Insufficient Resources ────────────────────────────────────────

/// `53200` — `out_of_memory`
pub const OUT_OF_MEMORY: &str = "53200";

/// `53300` — `too_many_connections` (closest match for rate-limit denial)
pub const TOO_MANY_CONNECTIONS: &str = "53300";

/// `53400` — `configuration_limit_exceeded` (quota exceeded)
pub const CONFIGURATION_LIMIT_EXCEEDED: &str = "53400";

// ── Class 54 — Program Limit Exceeded ────────────────────────────────────────

/// `54000` — `program_limit_exceeded` (generic over-cap)
pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";

/// `54001` — `statement_too_complex` (fan-out / rate limit exceeded)
pub const STATEMENT_TOO_COMPLEX: &str = "54001";

// ── Class 55 — Object Not In Prerequisite State ──────────────────────────────

/// `55P03` — `lock_not_available` (no cluster leader)
pub const LOCK_NOT_AVAILABLE: &str = "55P03";

// ── Class 57 — Operator Intervention ─────────────────────────────────────────

/// `57014` — `query_canceled` (deadline exceeded)
pub const QUERY_CANCELED: &str = "57014";

/// `57P03` — `cannot_connect_now` (collection is draining)
pub const CANNOT_CONNECT_NOW: &str = "57P03";

/// `57P04` — `database_dropped` (not-leader redirect; client should retry elsewhere)
pub const DATABASE_DROPPED: &str = "57P04";

// ── Class XX — Internal Error ────────────────────────────────────────────────

/// `XX000` — `internal_error`
pub const INTERNAL_ERROR: &str = "XX000";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_codes_are_five_chars() {
        let codes = [
            SUCCESS,
            WARNING,
            NO_DATA,
            FEATURE_NOT_SUPPORTED,
            NUMERIC_VALUE_OUT_OF_RANGE,
            INTEGRITY_CONSTRAINT_VIOLATION,
            NOT_NULL_VIOLATION,
            FOREIGN_KEY_VIOLATION,
            UNIQUE_VIOLATION,
            CHECK_VIOLATION,
            APPEND_ONLY_VIOLATION,
            BALANCE_VIOLATION,
            PERIOD_LOCKED,
            STATE_TRANSITION_VIOLATION,
            TRANSITION_CHECK_VIOLATION,
            RETENTION_VIOLATION,
            LEGAL_HOLD_ACTIVE,
            TYPE_GUARD_VIOLATION,
            INVALID_AUTHORIZATION,
            SERIALIZATION_FAILURE,
            INSUFFICIENT_PRIVILEGE,
            SYNTAX_ERROR,
            CANNOT_COERCE,
            UNDEFINED_TABLE,
            OUT_OF_MEMORY,
            TOO_MANY_CONNECTIONS,
            CONFIGURATION_LIMIT_EXCEEDED,
            PROGRAM_LIMIT_EXCEEDED,
            STATEMENT_TOO_COMPLEX,
            LOCK_NOT_AVAILABLE,
            QUERY_CANCELED,
            CANNOT_CONNECT_NOW,
            DATABASE_DROPPED,
            INTERNAL_ERROR,
        ];
        for code in &codes {
            assert_eq!(
                code.len(),
                5,
                "SQLSTATE '{code}' must be exactly 5 characters"
            );
        }
    }

    #[test]
    fn spot_check_well_known_codes() {
        assert_eq!(UNIQUE_VIOLATION, "23505");
        assert_eq!(UNDEFINED_TABLE, "42P01");
        assert_eq!(INSUFFICIENT_PRIVILEGE, "42501");
        assert_eq!(QUERY_CANCELED, "57014");
        assert_eq!(INTERNAL_ERROR, "XX000");
        assert_eq!(FEATURE_NOT_SUPPORTED, "0A000");
    }
}
