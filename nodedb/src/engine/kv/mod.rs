pub mod engine;
pub mod entry;
pub mod expiry_wheel;
mod hash_helpers;
pub mod hash_table;
pub mod scan;

pub use engine::KvEngine;

/// Get current wall-clock time in milliseconds since Unix epoch.
///
/// Used by KV engine handlers and the core loop for TTL calculations.
/// Returns 0 on clock failure (extremely rare, only on broken systems).
pub fn current_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
