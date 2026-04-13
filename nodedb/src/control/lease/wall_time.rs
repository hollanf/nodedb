//! Wall-clock helper shared by every lease code path that needs a
//! "real time now" reference — the renewal loop, the drain tracker
//! check, and the drain TTL computation. See `lease::renewal::tick`
//! for the rationale on why these paths use wall clock directly
//! instead of `HlcClock::peek`.

/// Wall-clock nanoseconds since Unix epoch.
pub(crate) fn wall_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
