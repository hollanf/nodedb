//! NodeDB version and compatibility information.
//!
//! Follows semver: MAJOR.MINOR.PATCH.
//! - MAJOR changes MAY break wire/disk compatibility.
//! - MINOR changes MUST be backward compatible within the same MAJOR.
//! - Rolling upgrades are supported for N-1 within the same MAJOR.

/// Current NodeDB version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Wire format version. Re-exported from `nodedb_types::wire_version`,
/// which is the single source of truth shared with `nodedb-cluster`
/// and any other crate that stamps or interprets the value.
///
/// Readers MUST reject messages with wire_version > their own. Readers
/// SHOULD accept messages with wire_version == their own or one less
/// (N-1).
pub use nodedb_types::wire_version::{MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION};

/// Wire version assigned to legacy clients that send wire_version == 0.
/// Must always equal MIN_WIRE_FORMAT_VERSION to prevent silent upgrades
/// across breaking changes.
pub const LEGACY_CLIENT_WIRE_VERSION: u16 = MIN_WIRE_FORMAT_VERSION;

/// Returns a comma-separated list of non-default features compiled into this build.
///
/// Returns an empty string when only default features are active.
pub fn features_str() -> &'static str {
    // Each feature flag is checked at compile time. The slice is built once
    // and leaked so we return a `&'static str` without any allocation at
    // runtime beyond the first call.
    use std::sync::OnceLock;
    static CACHE: OnceLock<&'static str> = OnceLock::new();
    CACHE.get_or_init(|| {
        let mut parts: Vec<&'static str> = Vec::new();
        if cfg!(feature = "promql") {
            parts.push("promql");
        }
        if cfg!(feature = "otel") {
            parts.push("otel");
        }
        if cfg!(feature = "grafana") {
            parts.push("grafana");
        }
        if cfg!(feature = "monitoring") {
            parts.push("monitoring");
        }
        if cfg!(feature = "kafka") {
            parts.push("kafka");
        }
        if parts.is_empty() {
            return "";
        }
        // Join into a permanent string.
        let joined = parts.join(",");
        Box::leak(joined.into_boxed_str())
    })
}

/// Returns the hostname of the current machine.
///
/// Uses `libc::gethostname`. Returns `"unknown"` on any error (buffer too small,
/// non-UTF-8, or syscall failure). Never panics.
pub fn hostname() -> String {
    // POSIX guarantees HOST_NAME_MAX + 1 bytes is sufficient (typically 256).
    // We use 256 to be safe across all platforms.
    let mut buf = vec![0u8; 256];
    let result = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if result != 0 {
        return "unknown".to_owned();
    }
    // Find the null terminator.
    let nul_pos = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8(buf[..nul_pos].to_vec()).unwrap_or_else(|_| "unknown".to_owned())
}

/// Check if a remote node's wire version is compatible.
///
/// Returns `Ok(())` if compatible, `Err(reason)` if not.
pub fn check_wire_compatibility(remote_version: u16) -> crate::Result<()> {
    if remote_version > WIRE_FORMAT_VERSION {
        return Err(crate::Error::VersionCompat {
            detail: format!(
                "remote wire version {remote_version} is newer than local {WIRE_FORMAT_VERSION}; upgrade this node"
            ),
        });
    }
    if remote_version < MIN_WIRE_FORMAT_VERSION {
        return Err(crate::Error::VersionCompat {
            detail: format!(
                "remote wire version {remote_version} is too old (minimum {MIN_WIRE_FORMAT_VERSION}); upgrade the remote node"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_version_compatible() {
        assert!(check_wire_compatibility(WIRE_FORMAT_VERSION).is_ok());
    }

    #[test]
    fn newer_version_rejected() {
        assert!(check_wire_compatibility(WIRE_FORMAT_VERSION + 1).is_err());
    }

    #[test]
    fn version_string_valid() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn git_commit_env_present() {
        let commit = env!("NODEDB_GIT_COMMIT");
        assert!(!commit.is_empty(), "NODEDB_GIT_COMMIT must not be empty");
    }

    #[test]
    fn build_date_env_format() {
        let date = env!("NODEDB_BUILD_DATE");
        assert_eq!(
            date.len(),
            10,
            "NODEDB_BUILD_DATE must be YYYY-MM-DD (10 chars)"
        );
        let bytes = date.as_bytes();
        assert_eq!(bytes[4], b'-', "position 4 must be '-'");
        assert_eq!(bytes[7], b'-', "position 7 must be '-'");
        for (i, &b) in bytes.iter().enumerate() {
            if i == 4 || i == 7 {
                continue;
            }
            assert!(
                b.is_ascii_digit(),
                "position {i} must be a digit, got '{}'",
                b as char
            );
        }
    }

    #[test]
    fn features_str_returns_known_set() {
        const KNOWN: &[&str] = &["promql", "otel", "grafana", "monitoring", "kafka"];
        let s = features_str();
        if s.is_empty() {
            return;
        }
        for token in s.split(',') {
            assert!(
                KNOWN.contains(&token),
                "features_str() returned unknown token: '{token}'"
            );
        }
    }

    #[test]
    fn hostname_is_non_empty() {
        let h = hostname();
        assert!(
            !h.is_empty(),
            "hostname() must never return an empty string"
        );
    }

    #[test]
    fn tracing_span_u64_field_captured() {
        // Validates that entering a span with a u64 field compiles and does
        // not panic — the same pattern used for `node_id` in main.rs.
        //
        // `entered()` consumes the span, so we use `enter()` (borrows) here
        // to retain the span handle for the subsequent `record` call.
        let span = tracing::info_span!("test_service", node_id = 0u64);
        let _guard = span.enter();
        // Record an updated value — mirrors the late-record call for cluster mode.
        span.record("node_id", 42u64);
        // If we reach here without panic the pattern is valid.
    }
}
