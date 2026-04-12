//! Cluster lifecycle state tracking for observability.
//!
//! `ClusterLifecycleTracker` is the single source of truth for "what
//! phase is this node's cluster init in right now". It is owned by
//! the main binary, passed by reference into `start_cluster` and
//! friends, and read by:
//!
//! - The `/cluster/status` HTTP endpoint.
//! - The `nodedb_cluster_state` Prometheus gauge.
//! - systemd via `readiness::notify_status` so `systemctl status`
//!   surfaces a live phase string during the seconds-to-minutes
//!   window when the cluster is still forming.
//! - INFO-level structured logs — every transition calls `info!` with
//!   the previous state, the new state, and the reason so a post-
//!   mortem on a flaky deploy can be done from `journalctl` alone.
//!
//! Transitions are validated only in the loose sense that every
//! transition goes through a typed method on the tracker. There is no
//! strict state machine — a cluster can legitimately go
//! `Joining{3} → Failed{"timeout"} → Joining{0} → Ready{3}` if the
//! operator restarts with `force_bootstrap`, so we allow any → any.

use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::readiness;

/// Discrete phase of this node's cluster init.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ClusterLifecycleState {
    /// Tracker just created, no phase decision yet.
    Starting,
    /// Catalog already marked as bootstrapped — loading from disk.
    Restarting,
    /// This node is the elected bootstrapper — creating a fresh cluster.
    Bootstrapping,
    /// Joining an existing cluster. `attempt` counts from 0.
    Joining {
        /// Current join attempt (0-indexed). See
        /// `bootstrap::join::next_backoff` for the backoff schedule.
        attempt: u32,
    },
    /// Cluster init finished successfully. `nodes` is the number of
    /// members the node observed at the moment of transition.
    Ready {
        /// Number of peers in topology when the transition occurred.
        nodes: usize,
    },
    /// Cluster init failed terminally. `reason` is a short
    /// human-readable description; see `journalctl` for the full
    /// context.
    Failed {
        /// One-line reason for the failure.
        reason: String,
    },
}

impl ClusterLifecycleState {
    /// Short label used in the `state=` dimension of the
    /// `nodedb_cluster_state` Prometheus gauge. Stable across restarts
    /// so dashboards don't break.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Restarting => "restarting",
            Self::Bootstrapping => "bootstrapping",
            Self::Joining { .. } => "joining",
            Self::Ready { .. } => "ready",
            Self::Failed { .. } => "failed",
        }
    }

    /// `true` only in the `Ready` variant. Used by readiness probes.
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    /// Every label this enum can produce. Used by the metrics
    /// endpoint to emit a one-hot gauge over the full state space.
    pub fn all_labels() -> &'static [&'static str] {
        &[
            "starting",
            "restarting",
            "bootstrapping",
            "joining",
            "ready",
            "failed",
        ]
    }
}

/// Thread-safe container for the current `ClusterLifecycleState`.
///
/// Wraps `Arc<RwLock<...>>` so a single tracker can be shared
/// between `start_cluster`, the main binary, and HTTP / metrics
/// readers without any cloning beyond the cheap `Arc` bump.
///
/// Every transition method:
///
/// 1. Takes the write lock.
/// 2. Swaps the stored state.
/// 3. Drops the lock.
/// 4. Emits an `info!` event with `prev`, `new`, and any relevant
///    context fields.
/// 5. Calls `readiness::notify_status(...)` so `systemctl status`
///    shows the new phase without any polling.
#[derive(Debug, Clone)]
pub struct ClusterLifecycleTracker {
    inner: Arc<RwLock<ClusterLifecycleState>>,
}

impl ClusterLifecycleTracker {
    /// Create a fresh tracker in `Starting` state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ClusterLifecycleState::Starting)),
        }
    }

    /// Read the current state. Returns a clone — callers never hold
    /// the lock across an await or a long loop.
    pub fn current(&self) -> ClusterLifecycleState {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// `true` iff the tracker has reached `Ready`.
    pub fn is_ready(&self) -> bool {
        self.current().is_ready()
    }

    pub fn to_restarting(&self) {
        self.transition(ClusterLifecycleState::Restarting, "restart");
    }

    pub fn to_bootstrapping(&self) {
        self.transition(
            ClusterLifecycleState::Bootstrapping,
            "bootstrapping new cluster",
        );
    }

    pub fn to_joining(&self, attempt: u32) {
        let detail = format!("joining cluster (attempt {attempt})");
        self.transition(ClusterLifecycleState::Joining { attempt }, &detail);
    }

    pub fn to_ready(&self, nodes: usize) {
        let detail = format!("ready ({nodes} nodes)");
        self.transition(ClusterLifecycleState::Ready { nodes }, &detail);
    }

    pub fn to_failed(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let detail = format!("failed: {reason}");
        self.transition(ClusterLifecycleState::Failed { reason }, &detail);
    }

    /// Shared implementation: swap the state, log at INFO, push the
    /// status string to systemd.
    fn transition(&self, new: ClusterLifecycleState, human: &str) {
        let prev = {
            let mut guard = self.inner.write().unwrap_or_else(|p| p.into_inner());
            std::mem::replace(&mut *guard, new.clone())
        };
        info!(
            prev = prev.label(),
            new = new.label(),
            detail = human,
            "cluster lifecycle transition"
        );
        readiness::notify_status(human);
    }
}

impl Default for ClusterLifecycleTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_starting() {
        let t = ClusterLifecycleTracker::new();
        assert_eq!(t.current(), ClusterLifecycleState::Starting);
        assert!(!t.is_ready());
    }

    #[test]
    fn transition_sequence_logs_and_updates() {
        let t = ClusterLifecycleTracker::new();
        t.to_joining(0);
        assert_eq!(t.current(), ClusterLifecycleState::Joining { attempt: 0 });
        t.to_joining(1);
        assert_eq!(t.current(), ClusterLifecycleState::Joining { attempt: 1 });
        t.to_ready(3);
        assert_eq!(t.current(), ClusterLifecycleState::Ready { nodes: 3 });
        assert!(t.is_ready());
    }

    #[test]
    fn bootstrapping_then_ready() {
        let t = ClusterLifecycleTracker::new();
        t.to_bootstrapping();
        assert_eq!(t.current(), ClusterLifecycleState::Bootstrapping);
        t.to_ready(1);
        assert!(t.is_ready());
    }

    #[test]
    fn restarting_path() {
        let t = ClusterLifecycleTracker::new();
        t.to_restarting();
        assert_eq!(t.current(), ClusterLifecycleState::Restarting);
        t.to_ready(3);
        assert!(t.is_ready());
    }

    #[test]
    fn failed_is_not_terminal_by_contract() {
        // Operator recovery (e.g. `force_bootstrap` after a failed
        // join) is a real scenario, so the tracker allows any → any
        // transitions: `Failed → Ready` is legal and is the correct
        // behaviour here.
        let t = ClusterLifecycleTracker::new();
        t.to_joining(5);
        t.to_failed("timeout");
        assert!(matches!(t.current(), ClusterLifecycleState::Failed { .. }));
        t.to_ready(3);
        assert_eq!(t.current(), ClusterLifecycleState::Ready { nodes: 3 });
    }

    #[test]
    fn labels_are_stable() {
        assert_eq!(ClusterLifecycleState::Starting.label(), "starting");
        assert_eq!(ClusterLifecycleState::Restarting.label(), "restarting");
        assert_eq!(
            ClusterLifecycleState::Bootstrapping.label(),
            "bootstrapping"
        );
        assert_eq!(
            ClusterLifecycleState::Joining { attempt: 0 }.label(),
            "joining"
        );
        assert_eq!(ClusterLifecycleState::Ready { nodes: 3 }.label(), "ready");
        assert_eq!(
            ClusterLifecycleState::Failed { reason: "x".into() }.label(),
            "failed"
        );
    }

    #[test]
    fn all_labels_matches_variants() {
        // Every variant's label must be present in all_labels, so the
        // Prometheus one-hot gauge covers every state.
        for variant in [
            ClusterLifecycleState::Starting,
            ClusterLifecycleState::Restarting,
            ClusterLifecycleState::Bootstrapping,
            ClusterLifecycleState::Joining { attempt: 0 },
            ClusterLifecycleState::Ready { nodes: 0 },
            ClusterLifecycleState::Failed { reason: "x".into() },
        ] {
            assert!(
                ClusterLifecycleState::all_labels().contains(&variant.label()),
                "label {} missing from all_labels()",
                variant.label()
            );
        }
    }

    #[test]
    fn tracker_is_cheap_to_clone() {
        let a = ClusterLifecycleTracker::new();
        let b = a.clone();
        a.to_bootstrapping();
        // Both handles see the same state because they share an Arc.
        assert_eq!(b.current(), ClusterLifecycleState::Bootstrapping);
    }
}
