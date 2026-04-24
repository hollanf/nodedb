//! Bitemporal scope types threaded through the SQL planner.
//!
//! A `TemporalScope` captures the user-facing `FOR SYSTEM_TIME AS OF <ms>` /
//! `FOR VALID_TIME CONTAINS <ms>` / `FOR VALID_TIME FROM <ms> TO <ms>`
//! qualifiers. It is extracted from raw SQL by
//! [`crate::parser::preprocess::temporal`] before sqlparser-rs sees the
//! statement, threaded through `plan_sql` into each `SqlPlan::Scan`, and
//! finally honored by the engine's Data Plane handler.
//!
//! All timestamps are **milliseconds since Unix epoch**. The edge store
//! converts to HLC ordinal via `nodedb_types::ms_to_ordinal_upper`. A
//! `TemporalScope::default()` value is equivalent to "current state"
//! (no temporal qualifier) — every scan site must construct one even when
//! the query is not temporal.

/// Valid-time qualifier attached to a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidTime {
    /// No `FOR VALID_TIME` clause — every version qualifies.
    #[default]
    Any,
    /// `FOR VALID_TIME CONTAINS <ms>` — version must satisfy
    /// `valid_from_ms <= ms < valid_until_ms`.
    At(i64),
    /// `FOR VALID_TIME FROM <lo_ms> TO <hi_ms>` — version's valid
    /// interval must overlap `[lo_ms, hi_ms)`.
    Range(i64, i64),
}

/// Combined bitemporal qualifier for a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TemporalScope {
    /// `FOR SYSTEM_TIME AS OF <ms>` cutoff, milliseconds. `None` means
    /// current (latest) system-time version.
    pub system_as_of_ms: Option<i64>,
    /// Valid-time qualifier.
    pub valid_time: ValidTime,
}

impl TemporalScope {
    /// True when either temporal axis is qualified.
    pub fn is_temporal(&self) -> bool {
        self.system_as_of_ms.is_some() || !matches!(self.valid_time, ValidTime::Any)
    }
}
