//! Canonical wire timestamp types for NodeDB.
//!
//! All wire-level timestamps use these two concepts:
//!
//! - **Wall-clock instants** — signed 64-bit milliseconds since the Unix epoch
//!   (UTC). Negative values represent dates before 1970-01-01. Type alias:
//!   [`WallMs`].
//! - **Durations / TTLs** — unsigned 64-bit milliseconds. Always non-negative.
//!   Type alias: [`DurMs`].
//!
//! Using these aliases in field declarations makes the intended semantics
//! self-documenting and ensures a single canonical width per concept.

use std::time::{SystemTime, UNIX_EPOCH};

/// A wall-clock instant expressed as signed milliseconds since the Unix epoch
/// (UTC). Negative values represent dates before 1970-01-01.
///
/// This is the canonical wire type for every "when did this happen?" timestamp.
pub type WallMs = i64;

/// A duration or TTL expressed as unsigned milliseconds. Always ≥ 0.
///
/// This is the canonical wire type for every "how long?" field.
pub type DurMs = u64;

/// Return the current wall-clock time as [`WallMs`] (milliseconds since the
/// Unix epoch, UTC).
///
/// Returns `i64::MAX` on the extremely unlikely event that the system clock
/// is before the Unix epoch or overflows — callers that care about that edge
/// case should use `SystemTime::now()` directly.
pub fn current_wall_ms() -> WallMs {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_wall_ms_is_positive_and_reasonable() {
        let ms = current_wall_ms();
        // Must be after 2024-01-01 (1704067200000 ms)
        assert!(ms > 1_704_067_200_000, "wall ms too small: {ms}");
        // Must be before year 2100 (4102444800000 ms)
        assert!(ms < 4_102_444_800_000, "wall ms too large: {ms}");
    }

    #[test]
    fn wall_ms_is_i64_alias() {
        let _x: WallMs = -1_i64; // must compile — negative is valid
        let _y: DurMs = 0_u64;
    }
}
