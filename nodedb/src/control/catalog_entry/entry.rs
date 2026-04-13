//! The `CatalogEntry` enum itself.
//!
//! Every variant corresponds to a single mutation on the host-side
//! `SystemCatalog` redb and/or an in-memory registry on
//! `SharedState`. Adding a variant forces every consumer to handle
//! it (the apply / post_apply / tests modules use exhaustive
//! matches).

use serde::{Deserialize, Serialize};

use crate::control::security::catalog::{
    StoredCollection, StoredMaterializedView,
    auth_types::{StoredApiKey, StoredRole, StoredUser},
    function_types::StoredFunction,
    procedure_types::StoredProcedure,
    sequence_types::{SequenceState, StoredSequence},
    trigger_types::StoredTrigger,
};
use crate::event::cdc::stream_def::ChangeStreamDef;
use crate::event::scheduler::types::ScheduleDef;

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum CatalogEntry {
    // ── Collection ─────────────────────────────────────────────────
    /// Upsert a collection record. Used by CREATE COLLECTION and by
    /// every ALTER COLLECTION path that ships a full updated record
    /// (strict schema changes, retention / legal_hold / LVC /
    /// append_only toggles, materialized_sum bindings).
    PutCollection(Box<StoredCollection>),
    /// Mark a collection as `is_active = false`. Record is
    /// preserved for audit + undrop.
    DeactivateCollection { tenant_id: u32, name: String },

    // ── Sequence ───────────────────────────────────────────────────
    /// Upsert a sequence record. Used by CREATE SEQUENCE and ALTER
    /// SEQUENCE FORMAT. Carries the full updated record so
    /// followers can apply the change without shipping a diff.
    PutSequence(Box<StoredSequence>),
    /// Delete a sequence record entirely. Used by DROP SEQUENCE and
    /// by the cascade path in DROP COLLECTION that removes implicit
    /// `{coll}_{field}_seq` sequences for SERIAL columns.
    DeleteSequence { tenant_id: u32, name: String },
    /// Upsert the runtime state of a sequence (current value,
    /// is_called, epoch, period_key). Used by ALTER SEQUENCE
    /// RESTART to propagate the new counter across nodes.
    PutSequenceState(Box<SequenceState>),

    // ── Trigger ────────────────────────────────────────────────────
    /// Upsert a trigger record. Used by CREATE [OR REPLACE] TRIGGER
    /// and by ALTER TRIGGER ENABLE/DISABLE paths that ship a full
    /// updated record.
    PutTrigger(Box<StoredTrigger>),
    /// Delete a trigger record.
    DeleteTrigger { tenant_id: u32, name: String },

    // ── Function ───────────────────────────────────────────────────
    /// Upsert a function record. Used by CREATE [OR REPLACE]
    /// FUNCTION. WASM binaries still live in their separate
    /// wasm-store redb table and are written directly on the
    /// proposing node; replicated wasm binary distribution is its
    /// own future batch.
    PutFunction(Box<StoredFunction>),
    /// Delete a function record.
    DeleteFunction { tenant_id: u32, name: String },

    // ── Procedure ──────────────────────────────────────────────────
    /// Upsert a stored procedure. Same body-cache invalidation
    /// pattern as `PutFunction` — the `block_cache` is cleared so
    /// the next CALL re-parses the new body.
    PutProcedure(Box<StoredProcedure>),
    /// Delete a stored procedure.
    DeleteProcedure { tenant_id: u32, name: String },

    // ── Schedule ───────────────────────────────────────────────────
    /// Upsert a scheduled-job definition. Post-apply syncs the
    /// in-memory `schedule_registry` so the cron executor on every
    /// node picks up the new / updated schedule immediately.
    PutSchedule(Box<ScheduleDef>),
    /// Delete a scheduled-job definition.
    DeleteSchedule { tenant_id: u32, name: String },

    // ── Change stream ──────────────────────────────────────────────
    /// Upsert a CDC change-stream definition. Post-apply syncs the
    /// in-memory `stream_registry` so the Event Plane starts
    /// buffering matching WriteEvents on every node.
    PutChangeStream(Box<ChangeStreamDef>),
    /// Delete a CDC change-stream definition + tear down its
    /// buffer via `cdc_router.remove_buffer`.
    DeleteChangeStream { tenant_id: u32, name: String },

    // ── User ───────────────────────────────────────────────────────
    /// Upsert a user record. The leader builds the full `StoredUser`
    /// (including Argon2 hash, SCRAM salt, and user_id) via
    /// `CredentialStore::prepare_user` before proposing — followers
    /// accept the pre-computed record verbatim and bump their local
    /// `next_user_id` counter to stay ahead of replicated IDs.
    PutUser(Box<StoredUser>),
    /// Soft-delete a user: flip `is_active = false` on every node's
    /// in-memory cache and redb record.
    DeactivateUser { username: String },

    // ── Role ───────────────────────────────────────────────────────
    /// Upsert a custom role. Built-in roles (Superuser/TenantAdmin/
    /// ReadWrite/ReadOnly/Monitor) never flow through this variant —
    /// they're hardcoded in `identity.rs`.
    PutRole(Box<StoredRole>),
    /// Delete a custom role. Does not cascade to grants that
    /// reference it (matching current local-only DROP semantics).
    DeleteRole { name: String },

    // ── ApiKey ─────────────────────────────────────────────────────
    /// Upsert an API key record. The leader builds the full
    /// `StoredApiKey` (including SHA-256 secret_hash) via
    /// `ApiKeyStore::prepare_key`; followers accept the pre-computed
    /// record verbatim. The plaintext secret NEVER enters raft —
    /// only the proposing client receives the token.
    PutApiKey(Box<StoredApiKey>),
    /// Revoke an API key — sets `is_revoked = true` in the cached
    /// record and re-writes the redb row. Preserves the record for
    /// audit trails.
    RevokeApiKey { key_id: String },

    // ── Materialized View ──────────────────────────────────────────
    /// Upsert a materialized view definition. The Data Plane
    /// refresh loop picks up the new definition on its next tick
    /// and starts materializing rows from source → target.
    PutMaterializedView(Box<StoredMaterializedView>),
    /// Delete a materialized view definition. The target
    /// collection is NOT deleted — operators drop it separately
    /// with `DROP COLLECTION` if desired.
    DeleteMaterializedView { tenant_id: u32, name: String },
}

impl CatalogEntry {
    /// Short, human-readable descriptor of this entry — used in
    /// trace / metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PutCollection(_) => "put_collection",
            Self::DeactivateCollection { .. } => "deactivate_collection",
            Self::PutSequence(_) => "put_sequence",
            Self::DeleteSequence { .. } => "delete_sequence",
            Self::PutSequenceState(_) => "put_sequence_state",
            Self::PutTrigger(_) => "put_trigger",
            Self::DeleteTrigger { .. } => "delete_trigger",
            Self::PutFunction(_) => "put_function",
            Self::DeleteFunction { .. } => "delete_function",
            Self::PutProcedure(_) => "put_procedure",
            Self::DeleteProcedure { .. } => "delete_procedure",
            Self::PutSchedule(_) => "put_schedule",
            Self::DeleteSchedule { .. } => "delete_schedule",
            Self::PutChangeStream(_) => "put_change_stream",
            Self::DeleteChangeStream { .. } => "delete_change_stream",
            Self::PutUser(_) => "put_user",
            Self::DeactivateUser { .. } => "deactivate_user",
            Self::PutRole(_) => "put_role",
            Self::DeleteRole { .. } => "delete_role",
            Self::PutApiKey(_) => "put_api_key",
            Self::RevokeApiKey { .. } => "revoke_api_key",
            Self::PutMaterializedView(_) => "put_materialized_view",
            Self::DeleteMaterializedView { .. } => "delete_materialized_view",
        }
    }
}
