//! Minimal feature-gated fail-point framework for crash-injection tests.
//!
//! When the `failpoints` Cargo feature is OFF (the default and what release
//! builds compile with), `fail_point!(name)` expands to nothing — zero
//! runtime cost.
//!
//! When the feature is ON, each invocation looks up `name` in a process-wide
//! registry. If a `FailAction` has been installed for that name, the action
//! fires:
//!   - `Panic`: panic immediately with a message naming the fail point.
//!   - `Sleep(d)`: sleep for `d`, useful for race-condition tests.
//!
//! The framework is deliberately tiny — no fail-rs dep, no parsing of env
//! vars, no list of probabilities. Tests install actions explicitly via
//! `set` / `clear` and must clean up in their own teardown (or use
//! `FailGuard` for RAII cleanup).

#[cfg(feature = "failpoints")]
mod imp {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    /// Action a fail point performs when triggered.
    #[derive(Debug, Clone)]
    pub enum FailAction {
        /// Panic with a message that names the fail-point.
        Panic,
        /// Sleep for the given duration; execution then continues normally.
        Sleep(Duration),
    }

    static REGISTRY: LazyLock<Mutex<HashMap<String, FailAction>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// Install an action for a named fail point. Subsequent
    /// `fail_point!(name)` invocations will fire the action.
    pub fn set(name: &str, action: FailAction) {
        REGISTRY
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(name.to_string(), action);
    }

    /// Remove any installed action for the named fail point.
    pub fn clear(name: &str) {
        REGISTRY
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name);
    }

    /// Look up the installed action for a fail point (None if not set).
    pub fn lookup(name: &str) -> Option<FailAction> {
        REGISTRY
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .cloned()
    }

    /// Evaluate the installed action for a fail point. Used by the
    /// `fail_point!` macro; not intended to be called directly.
    pub fn eval(name: &str) {
        if let Some(action) = lookup(name) {
            match action {
                FailAction::Panic => panic!("fail_point fired: {name}"),
                FailAction::Sleep(d) => std::thread::sleep(d),
            }
        }
    }

    /// RAII guard that clears a fail point on drop. Use to keep tests
    /// from leaking installed actions across cases.
    pub struct FailGuard {
        name: String,
    }

    impl FailGuard {
        pub fn install(name: &str, action: FailAction) -> Self {
            set(name, action);
            Self {
                name: name.to_string(),
            }
        }
    }

    impl Drop for FailGuard {
        fn drop(&mut self) {
            clear(&self.name);
        }
    }
}

#[cfg(feature = "failpoints")]
pub use imp::{FailAction, FailGuard, clear, eval, lookup, set};

/// Inject a fail point. Expands to nothing without the `failpoints` feature.
///
/// Usage in production code:
///   `fail_point!("transaction_batch::between_subapply");`
///
/// Tests opt in by enabling the feature and installing actions:
///   `fail_point::set("transaction_batch::between_subapply",
///                    fail_point::FailAction::Panic);`
#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {
        #[cfg(feature = "failpoints")]
        $crate::fail_point::eval($name);
    };
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use super::*;

    #[test]
    fn unset_fail_point_is_noop() {
        eval("nodedb::test::unset");
    }

    #[test]
    #[should_panic(expected = "fail_point fired: nodedb::test::panic_target")]
    fn set_panic_fires() {
        let _g = FailGuard::install("nodedb::test::panic_target", FailAction::Panic);
        eval("nodedb::test::panic_target");
    }

    #[test]
    fn fail_guard_clears_on_drop() {
        {
            let _g = FailGuard::install("nodedb::test::guard_clear", FailAction::Panic);
            assert!(lookup("nodedb::test::guard_clear").is_some());
        }
        assert!(lookup("nodedb::test::guard_clear").is_none());
    }
}
