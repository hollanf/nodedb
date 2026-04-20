//! Gateway plan-cache invalidation for DDL descriptor mutations.
//!
//! The gateway plan cache keys on `(sql_hash, ph_hash, GatewayVersionSet)`.
//! A `GatewayVersionSet` lists `(collection_name, descriptor_version)` pairs
//! extracted from the `PhysicalPlan` by `touched_collections`. A DDL entry
//! requires invalidation only if it changes the observable plan shape for
//! an already-cached plan.

use std::sync::Arc;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

/// Notify the gateway plan-cache invalidator after a DDL descriptor mutation.
///
/// Extracts the descriptor name and new version from the entry and calls
/// `PlanCacheInvalidator::invalidate`. This is best-effort: if the gateway
/// has not been constructed yet (`gateway_invalidator == None`) the call is
/// a no-op.
///
/// ## Invalidation decision table (exhaustive, no `_ => {}`)
///
/// | Entry kind                              | Invalidate? | Reason |
/// |-----------------------------------------|-------------|--------|
/// | PutCollection / DeactivateCollection    | ✅ yes      | collection schema baked into plan |
/// | PutSequence / DeleteSequence            | ❌ no       | sequences resolved at handler level (pgwire `transaction_cmds.rs`), not in PhysicalPlan |
/// | PutSequenceState                        | ❌ no       | runtime counter state, not plan shape |
/// | PutTrigger / DeleteTrigger              | ❌ no       | triggers dispatched by Event Plane post-execution; no trigger fields in any PhysicalPlan variant |
/// | PutFunction / DeleteFunction            | ❌ no       | functions looked up at eval time, not inlined |
/// | PutProcedure / DeleteProcedure          | ❌ no       | same as functions |
/// | PutSchedule / DeleteSchedule            | ❌ no       | scheduler runs independently |
/// | PutChangeStream / DeleteChangeStream    | ❌ no       | CDC Event Plane concern |
/// | PutUser / DeactivateUser                | ❌ no       | authz checked at exec time |
/// | PutRole / DeleteRole                    | ❌ no       | same |
/// | PutApiKey / RevokeApiKey                | ❌ no       | same |
/// | PutMaterializedView / DeleteMaterializedView | ❌ no  | MV definition is its own catalog object; write-path `materialized_sum_sources` is set at collection-register time via PutCollection, not updated by PutMaterializedView independently |
/// | PutTenant / DeleteTenant                | ❌ no       | tenant identity does not affect plan shape |
/// | PutRlsPolicy / DeleteRlsPolicy          | ❌ no       | `execute_sql` is only called from CDC path (no RLS injection via `inject_rls`); per-session pgwire cache has its own DDL invalidation |
/// | PutPermission / DeletePermission        | ❌ no       | permission checked at exec time |
/// | PutOwner / DeleteOwner                  | ❌ no       | ownership does not affect plan shape |
pub(crate) fn invalidate_gateway_cache_for_entry(entry: &CatalogEntry, shared: &Arc<SharedState>) {
    let Some(ref inv) = shared.gateway_invalidator else {
        return;
    };
    match entry {
        // ── Collection mutations that change the plan shape ──────────────────
        CatalogEntry::PutCollection(stored) => {
            inv.invalidate(&stored.name, stored.descriptor_version.max(1));
        }
        CatalogEntry::DeactivateCollection { name, .. } => {
            // Treat deactivation as version 0 (collection gone — any cached
            // plan for it is stale).
            inv.invalidate(name, 0);
        }
        CatalogEntry::PurgeCollection { name, .. } => {
            // Hard delete: same invalidation semantic as deactivate —
            // any cached plan for this name is stale.
            inv.invalidate(name, 0);
        }

        // ── Sequence: resolved at handler level, not baked into PhysicalPlan ─
        CatalogEntry::PutSequence(_) => {
            // no-op: sequences resolved in pgwire transaction_cmds.rs before
            // planning; StoredSequence never appears in a PhysicalPlan variant.
        }
        CatalogEntry::DeleteSequence { .. } => {
            // no-op: same reason as PutSequence.
        }
        CatalogEntry::PutSequenceState(_) => {
            // no-op: runtime counter state — the planner never reads seq state.
        }

        // ── Trigger: dispatched by Event Plane post-execution ────────────────
        CatalogEntry::PutTrigger(_) => {
            // no-op: triggers are AFTER-fire; no trigger field exists in any
            // PhysicalPlan variant; Event Plane reads the trigger registry
            // directly at fire time.
        }
        CatalogEntry::DeleteTrigger { .. } => {
            // no-op: same as PutTrigger.
        }

        // ── Function / Procedure: looked up at eval time, not inlined ────────
        CatalogEntry::PutFunction(_) => {
            // no-op: UDFs looked up in function_registry at eval time via
            // `wasm/` executor; never inlined into a PhysicalPlan.
        }
        CatalogEntry::DeleteFunction { .. } => {
            // no-op: same as PutFunction.
        }
        CatalogEntry::PutProcedure(_) => {
            // no-op: stored procedures parsed and executed at CALL time via
            // `procedural/executor`; body not baked into any PhysicalPlan.
        }
        CatalogEntry::DeleteProcedure { .. } => {
            // no-op: same as PutProcedure.
        }

        // ── Schedule: cron runs independently of the plan cache ──────────────
        CatalogEntry::PutSchedule(_) => {
            // no-op: ScheduleRegistry drives the scheduler loop; no plan shape
            // changes result from a new/updated schedule definition.
        }
        CatalogEntry::DeleteSchedule { .. } => {
            // no-op: same as PutSchedule.
        }

        // ── Change stream: CDC Event Plane concern ────────────────────────────
        CatalogEntry::PutChangeStream(_) => {
            // no-op: CDC stream definitions route WriteEvents in the Event
            // Plane; they do not alter how a collection's plan is constructed.
        }
        CatalogEntry::DeleteChangeStream { .. } => {
            // no-op: same as PutChangeStream.
        }

        // ── User / Role / ApiKey: authz checked at exec, not baked into plan ─
        CatalogEntry::PutUser(_) => {
            // no-op: user identity checked in credential store at exec time.
        }
        CatalogEntry::DeactivateUser { .. } => {
            // no-op: same as PutUser.
        }
        CatalogEntry::PutRole(_) => {
            // no-op: role membership checked at exec time via RoleStore.
        }
        CatalogEntry::DeleteRole { .. } => {
            // no-op: same as PutRole.
        }
        CatalogEntry::PutApiKey(_) => {
            // no-op: API key checked at connection/exec time via ApiKeyStore.
        }
        CatalogEntry::RevokeApiKey { .. } => {
            // no-op: same as PutApiKey.
        }

        // ── Materialized view: MV definition is a separate catalog object ────
        CatalogEntry::PutMaterializedView(_) => {
            // no-op: MaterializedView metadata is its own catalog object and
            // does not directly modify any PhysicalPlan. The `materialized_sum_sources`
            // field in DocumentOp::Register is set at collection-register time
            // (driven by PutCollection), not updated independently by
            // PutMaterializedView. Any schema change that would affect plans
            // cascades through PutCollection instead.
        }
        CatalogEntry::DeleteMaterializedView { .. } => {
            // no-op: same as PutMaterializedView.
        }

        // ── Tenant: identity does not affect plan shape ───────────────────────
        CatalogEntry::PutTenant(_) => {
            // no-op: tenant identity used for quota enforcement at exec time.
        }
        CatalogEntry::DeleteTenant { .. } => {
            // no-op: same as PutTenant.
        }

        // ── RLS policy: execute_sql callers (CDC) do not inject RLS ──────────
        CatalogEntry::PutRlsPolicy(_) => {
            // no-op: the gateway execute_sql path (CDC consume_remote) calls
            // plan_sql without RLS injection; per-session pgwire plan cache
            // has its own DDL-aware invalidation that handles RLS changes.
        }
        CatalogEntry::DeleteRlsPolicy { .. } => {
            // no-op: same as PutRlsPolicy.
        }

        // ── Permission / Owner: not baked into plan ───────────────────────────
        CatalogEntry::PutPermission(_) => {
            // no-op: permission grants checked at exec time via PermissionStore.
        }
        CatalogEntry::DeletePermission { .. } => {
            // no-op: same as PutPermission.
        }
        CatalogEntry::PutOwner(_) => {
            // no-op: ownership does not influence plan structure.
        }
        CatalogEntry::DeleteOwner { .. } => {
            // no-op: same as PutOwner.
        }
    }
}
