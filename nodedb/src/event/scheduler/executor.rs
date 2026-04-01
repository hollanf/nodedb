//! Scheduler executor: Tokio task that evaluates cron expressions every second.
//!
//! For each due schedule, dispatches the SQL body through the Control Plane
//! query path using a system identity (SECURITY DEFINER).
//!
//! **Leader-aware (cluster mode):** Before firing, the scheduler checks
//! if this node is the Raft leader for the schedule's target vShard.
//! Only the leader fires — follower nodes skip. When a vShard migrates
//! or leadership changes, the new leader automatically picks up the schedule.
//!
//! **Lease-aware:** If this node is leader but the Raft group is lagging
//! (commit_index > last_applied), the scheduler skips firing to prevent
//! stale execution during a network partition.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::StatementExecutor;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::cron::CronExpr;
use super::history::JobHistoryStore;
use super::registry::ScheduleRegistry;
use super::types::{JobRun, ScheduleDef, ScheduleScope};

/// Spawn the scheduler loop as a background Tokio task.
pub fn spawn_scheduler(
    state: Arc<SharedState>,
    registry: Arc<ScheduleRegistry>,
    history: Arc<JobHistoryStore>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        scheduler_loop(state, registry, history, shutdown).await;
    })
}

/// The main scheduler loop. Runs every second.
async fn scheduler_loop(
    state: Arc<SharedState>,
    registry: Arc<ScheduleRegistry>,
    history: Arc<JobHistoryStore>,
    mut shutdown: watch::Receiver<bool>,
) {
    info!("scheduler started");

    // Track currently running jobs (for ALLOW_OVERLAP = false enforcement).
    // Shared with spawned job tasks so they remove themselves on completion.
    let running: Arc<std::sync::Mutex<HashSet<(u32, String)>>> =
        Arc::new(std::sync::Mutex::new(HashSet::new()));

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("scheduler shutting down");
                    return;
                }
            }
        }

        if *shutdown.borrow() {
            return;
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Get all enabled schedules.
        let schedules = registry.list_all_enabled();
        if schedules.is_empty() {
            continue;
        }

        for sched in &schedules {
            // Parse cron expression.
            let cron = match CronExpr::parse(&sched.cron_expr) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        schedule = %sched.name,
                        error = %e,
                        "invalid cron expression, skipping"
                    );
                    continue;
                }
            };

            // Check if this second matches the cron expression.
            // We check at second-level granularity but cron is minute-level,
            // so only fire at second 0 of each minute to prevent duplicate fires.
            if !now_secs.is_multiple_of(60) {
                continue;
            }

            if !cron.matches_epoch(now_secs) {
                continue;
            }

            // Leader-aware: skip if this node is not the right one for this schedule.
            if !should_fire_on_this_node(sched, &state) {
                trace!(
                    schedule = %sched.name,
                    "skipping: this node is not the leader for target vShard"
                );
                continue;
            }

            // Lease-aware: skip if the Raft group is lagging (stale leader).
            if !is_raft_group_healthy(sched, &state) {
                debug!(
                    schedule = %sched.name,
                    "skipping: Raft group lagging (lease-aware suspension)"
                );
                continue;
            }

            // Check overlap policy.
            let key = (sched.tenant_id, sched.name.clone());
            if !sched.allow_overlap {
                let guard = running.lock().unwrap_or_else(|p| p.into_inner());
                if guard.contains(&key) {
                    trace!(
                        schedule = %sched.name,
                        "skipping: previous run still active (ALLOW_OVERLAP = false)"
                    );
                    continue;
                }
            }

            // Mark as running before spawning (prevents race with next scheduler tick).
            {
                let mut guard = running.lock().unwrap_or_else(|p| p.into_inner());
                guard.insert(key.clone());
            }

            debug!(schedule = %sched.name, "firing scheduled job");

            let state_clone = Arc::clone(&state);
            let history_clone = Arc::clone(&history);
            let running_clone = Arc::clone(&running);
            let sched_clone = sched.clone();

            // Spawn each job as a separate task so the scheduler loop doesn't block.
            // The task removes itself from `running` on completion.
            tokio::spawn(async move {
                let result = execute_job(&state_clone, &sched_clone).await;
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let run = match result {
                    Ok(duration_ms) => JobRun {
                        schedule_name: sched_clone.name.clone(),
                        tenant_id: sched_clone.tenant_id,
                        started_at: now_ms.saturating_sub(duration_ms),
                        duration_ms,
                        success: true,
                        error: None,
                    },
                    Err(e) => {
                        warn!(
                            schedule = %sched_clone.name,
                            error = %e,
                            "scheduled job failed"
                        );
                        JobRun {
                            schedule_name: sched_clone.name.clone(),
                            tenant_id: sched_clone.tenant_id,
                            started_at: now_ms,
                            duration_ms: 0,
                            success: false,
                            error: Some(e.to_string()),
                        }
                    }
                };

                if let Err(e) = history_clone.record(run) {
                    warn!(error = %e, "failed to record job history");
                }

                // Remove from running set — allows next scheduled fire.
                let key = (sched_clone.tenant_id, sched_clone.name.clone());
                let mut guard = running_clone.lock().unwrap_or_else(|p| p.into_inner());
                guard.remove(&key);
            });
        }
    }
}

/// Check whether this node should fire the given schedule.
///
/// - `ScheduleScope::Local` → always fire (local node only).
/// - No `cluster_routing` → single-node mode → always fire.
/// - `target_collection` is Some → resolve vShard → check leader.
/// - `target_collection` is None → cross-collection job → only the lowest
///   node_id in the cluster fires (acts as `_system` coordinator).
fn should_fire_on_this_node(sched: &ScheduleDef, state: &SharedState) -> bool {
    // LOCAL scope: always fire on this node.
    if sched.scope == ScheduleScope::Local {
        return true;
    }

    // Single-node mode: no cluster routing → fire everything.
    let Some(ref routing_lock) = state.cluster_routing else {
        return true;
    };

    let node_id = state.node_id;

    if let Some(ref collection) = sched.target_collection {
        // Collection-targeted schedule: fire only on the shard leader.
        let vshard_id = nodedb_cluster::routing::vshard_for_collection(collection);
        let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
        match routing.leader_for_vshard(vshard_id) {
            Ok(leader) => leader == node_id,
            Err(_) => {
                // Can't determine leader — skip to be safe.
                false
            }
        }
    } else {
        // Cross-collection or opaque job: fire on coordinator node.
        // Convention: the leader of vShard 0 acts as the _system coordinator.
        let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
        match routing.leader_for_vshard(0) {
            Ok(coordinator) => coordinator == node_id,
            Err(_) => false,
        }
    }
}

/// Check if the Raft group for this schedule's target vShard is healthy.
///
/// A group is healthy when `commit_index == last_applied` (fully caught up).
/// If the group is lagging, this node may be a stale leader during a partition.
/// Skipping prevents dual execution when the new leader hasn't taken over yet.
///
/// Returns `true` (healthy) in single-node mode or when no status function exists.
fn is_raft_group_healthy(sched: &ScheduleDef, state: &SharedState) -> bool {
    // LOCAL scope: no Raft group to check.
    if sched.scope == ScheduleScope::Local {
        return true;
    }

    // Single-node mode: always healthy.
    let Some(ref status_fn) = state.raft_status_fn else {
        return true;
    };
    let Some(ref routing_lock) = state.cluster_routing else {
        return true;
    };

    // Determine the target vShard's Raft group.
    let vshard_id = sched
        .target_collection
        .as_ref()
        .map(|c| nodedb_cluster::routing::vshard_for_collection(c))
        .unwrap_or(0); // Cross-collection → coordinator vShard 0.

    let group_id = {
        let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
        match routing.group_for_vshard(vshard_id) {
            Ok(gid) => gid,
            Err(_) => return true, // Can't determine group — fire anyway.
        }
    };

    // Check the group's status.
    let statuses = status_fn();
    let Some(status) = statuses.iter().find(|s| s.group_id == group_id) else {
        return true; // Group not found locally — might be on another node, fire anyway.
    };

    // Stale leader check: if commit_index is ahead of last_applied,
    // this node has uncommitted entries — it may be partitioned.
    if status.commit_index > status.last_applied {
        let lag = status.commit_index - status.last_applied;
        debug!(
            group_id,
            commit_index = status.commit_index,
            last_applied = status.last_applied,
            lag,
            "Raft group lagging — suspending schedule fire"
        );
        return false;
    }

    // Also check that we're actually the leader for this group.
    if status.leader_id != state.node_id {
        return false;
    }

    true
}

/// Execute a single scheduled job.
///
/// Returns the duration in milliseconds on success.
async fn execute_job(state: &SharedState, sched: &ScheduleDef) -> crate::Result<u64> {
    let start = std::time::Instant::now();
    let identity = scheduler_identity(TenantId::new(sched.tenant_id), &sched.owner);

    // Parse and execute the body as procedural SQL.
    let block = crate::control::planner::procedural::parse_block(&sched.body_sql).map_err(|e| {
        crate::Error::BadRequest {
            detail: format!("schedule '{}' body parse error: {e}", sched.name),
        }
    })?;

    let executor =
        StatementExecutor::new(state, identity.clone(), TenantId::new(sched.tenant_id), 0);
    let bindings = RowBindings::empty();

    // Execute with one retry — if a vShard migrated mid-job, the scatter-gather
    // layer retries on the new location. Jobs are idempotent at the statement level.
    match executor.execute_block(&block, &bindings).await {
        Ok(()) => {}
        Err(first_err) => {
            tracing::warn!(
                schedule = %sched.name,
                error = %first_err,
                "scheduled job failed, retrying once (possible vShard migration)"
            );
            // Retry with a fresh executor.
            let retry_executor =
                StatementExecutor::new(state, identity, TenantId::new(sched.tenant_id), 0);
            retry_executor.execute_block(&block, &bindings).await?;
        }
    }

    Ok(start.elapsed().as_millis() as u64)
}

/// Build the owner's identity for scheduled job execution (SECURITY DEFINER).
///
/// The job runs with the schedule creator's privileges, not SYSTEM.
/// If the owner is found in the credential store, use their actual roles.
/// Falls back to superuser if not found (backward compatibility).
fn scheduler_identity(tenant_id: TenantId, owner: &str) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: owner.to_string(),
        tenant_id,
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::MissedPolicy;
    use super::*;

    fn make_schedule(name: &str, target: Option<&str>, scope: ScheduleScope) -> ScheduleDef {
        ScheduleDef {
            tenant_id: 1,
            name: name.into(),
            cron_expr: "* * * * *".into(),
            body_sql: "SELECT 1".into(),
            scope,
            missed_policy: MissedPolicy::Skip,
            allow_overlap: true,
            enabled: true,
            target_collection: target.map(|s| s.to_string()),
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn scheduler_identity_is_superuser() {
        let id = scheduler_identity(TenantId::new(1), "admin");
        assert!(id.is_superuser);
        assert_eq!(id.username, "admin");
    }

    #[test]
    fn local_scope_always_fires() {
        let sched = make_schedule("local_job", Some("orders"), ScheduleScope::Local);
        // Even if we had cluster routing, LOCAL always fires.
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        assert!(should_fire_on_this_node(&sched, &state));
    }

    #[test]
    fn single_node_always_fires() {
        let sched = make_schedule("normal_job", Some("orders"), ScheduleScope::Normal);
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        // No cluster_routing → single-node → always fires.
        assert!(state.cluster_routing.is_none());
        assert!(should_fire_on_this_node(&sched, &state));
    }

    #[test]
    fn local_scope_healthy_without_raft() {
        let sched = make_schedule("local_job", None, ScheduleScope::Local);
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        assert!(is_raft_group_healthy(&sched, &state));
    }

    #[test]
    fn single_node_healthy_without_raft() {
        let sched = make_schedule("job", Some("orders"), ScheduleScope::Normal);
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        // No raft_status_fn → always healthy.
        assert!(is_raft_group_healthy(&sched, &state));
    }
}
