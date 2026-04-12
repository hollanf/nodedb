//! Cluster startup entry point: dispatches to bootstrap, join, or restart.
//!
//! The decision tree is deliberately small and delegates every
//! non-trivial choice to a dedicated module:
//!
//! - **restart** (`super::restart`) — if the catalog already reports
//!   this node as bootstrapped, we always take the restart path,
//!   regardless of `seed_nodes` or `force_bootstrap`. The catalog is
//!   the authoritative source of truth once it exists.
//! - **bootstrap** (`super::bootstrap_fn`) — taken when this node is
//!   the elected bootstrapper (lowest-addr seed), or when the operator
//!   forced it via `ClusterConfig::force_bootstrap`, or when no other
//!   seed is running. See [`super::probe::should_bootstrap`].
//! - **join** (`super::join`) — everything else. The join path owns
//!   its own retry-with-backoff loop and leader-redirect handling, so
//!   this dispatcher does not need to retry at this layer.

use crate::catalog::ClusterCatalog;
use crate::error::Result;
use crate::lifecycle_state::ClusterLifecycleTracker;
use crate::transport::NexarTransport;

use super::bootstrap_fn::bootstrap;
use super::config::{ClusterConfig, ClusterState};
use super::join::join;
use super::probe::should_bootstrap;
use super::restart::restart;

/// Start the cluster — bootstrap, join, or restart depending on state.
///
/// Returns the initialized cluster state ready for the Raft loop.
///
/// `lifecycle` is the caller-owned phase tracker. This function
/// transitions it to `Restarting` / `Bootstrapping` / `Joining` as
/// the dispatcher picks a branch, and to `Failed` on terminal error.
/// The caller is responsible for the final `Ready` transition once
/// listeners are up — see `nodedb::control::cluster::start_raft`.
pub async fn start_cluster(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
    lifecycle: &ClusterLifecycleTracker,
) -> Result<ClusterState> {
    // Authoritative catalog state wins — a previously bootstrapped
    // node always takes the restart path on boot.
    if catalog.is_bootstrapped()? {
        lifecycle.to_restarting();
        return restart(config, catalog, transport).inspect_err(|e| {
            lifecycle.to_failed(format!("restart failed: {e}"));
        });
    }

    // No existing state — decide bootstrap vs join.
    let is_seed = config.seed_nodes.contains(&config.listen_addr);

    if is_seed && should_bootstrap(config, transport).await {
        lifecycle.to_bootstrapping();
        bootstrap(config, catalog).inspect_err(|e| {
            lifecycle.to_failed(format!("bootstrap failed: {e}"));
        })
    } else {
        join(config, catalog, transport, lifecycle).await
    }
}
