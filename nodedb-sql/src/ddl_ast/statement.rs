//! The [`NodedbStatement`] enum — one variant per DDL command.

/// Typed representation of every NodeDB DDL statement.
///
/// Handlers receive a fully-parsed variant instead of raw `&[&str]`
/// parts, eliminating array-index panics and enabling exhaustive
/// match coverage for new DDL commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodedbStatement {
    // ── Collection lifecycle ─────────────────────────────────────
    CreateCollection {
        name: String,
        if_not_exists: bool,
        raw_sql: String,
    },
    DropCollection {
        name: String,
        if_exists: bool,
    },
    AlterCollection {
        name: String,
        raw_sql: String,
    },
    DescribeCollection {
        name: String,
    },
    ShowCollections,

    // ── Index ────────────────────────────────────────────────────
    CreateIndex {
        unique: bool,
        raw_sql: String,
    },
    DropIndex {
        name: String,
        collection: Option<String>,
        if_exists: bool,
    },
    ShowIndexes {
        collection: Option<String>,
    },
    Reindex {
        collection: String,
    },

    // ── Trigger ──────────────────────────────────────────────────
    CreateTrigger {
        or_replace: bool,
        deferred: bool,
        sync: bool,
        raw_sql: String,
    },
    DropTrigger {
        name: String,
        collection: String,
        if_exists: bool,
    },
    AlterTrigger {
        raw_sql: String,
    },
    ShowTriggers {
        collection: Option<String>,
    },

    // ── Schedule ─────────────────────────────────────────────────
    CreateSchedule {
        raw_sql: String,
    },
    DropSchedule {
        name: String,
        if_exists: bool,
    },
    AlterSchedule {
        raw_sql: String,
    },
    ShowSchedules,
    ShowScheduleHistory {
        name: String,
    },

    // ── Sequence ─────────────────────────────────────────────────
    CreateSequence {
        name: String,
        if_not_exists: bool,
        raw_sql: String,
    },
    DropSequence {
        name: String,
        if_exists: bool,
    },
    AlterSequence {
        raw_sql: String,
    },
    DescribeSequence {
        name: String,
    },
    ShowSequences,

    // ── Alert ────────────────────────────────────────────────────
    CreateAlert {
        raw_sql: String,
    },
    DropAlert {
        name: String,
        if_exists: bool,
    },
    AlterAlert {
        raw_sql: String,
    },
    ShowAlerts,
    ShowAlertStatus {
        name: String,
    },

    // ── Retention policy ─────────────────────────────────────────
    CreateRetentionPolicy {
        raw_sql: String,
    },
    DropRetentionPolicy {
        name: String,
        if_exists: bool,
    },
    AlterRetentionPolicy {
        raw_sql: String,
    },
    ShowRetentionPolicies,

    // ── Change stream ────────────────────────────────────────────
    CreateChangeStream {
        raw_sql: String,
    },
    DropChangeStream {
        name: String,
        if_exists: bool,
    },
    AlterChangeStream {
        raw_sql: String,
    },
    ShowChangeStreams,

    // ── Consumer group ───────────────────────────────────────────
    CreateConsumerGroup {
        raw_sql: String,
    },
    DropConsumerGroup {
        name: String,
        stream: String,
        if_exists: bool,
    },
    ShowConsumerGroups {
        stream: Option<String>,
    },

    // ── RLS policy ───────────────────────────────────────────────
    CreateRlsPolicy {
        raw_sql: String,
    },
    DropRlsPolicy {
        name: String,
        collection: String,
        if_exists: bool,
    },
    ShowRlsPolicies {
        collection: Option<String>,
    },

    // ── Materialized view ────────────────────────────────────────
    CreateMaterializedView {
        raw_sql: String,
    },
    DropMaterializedView {
        name: String,
        if_exists: bool,
    },
    ShowMaterializedViews,

    // ── Continuous aggregate ─────────────────────────────────────
    CreateContinuousAggregate {
        raw_sql: String,
    },
    DropContinuousAggregate {
        name: String,
        if_exists: bool,
    },
    ShowContinuousAggregates,

    // ── Backup / restore ─────────────────────────────────────────
    BackupTenant {
        raw_sql: String,
    },
    RestoreTenant {
        dry_run: bool,
        raw_sql: String,
    },

    // ── Cluster admin ────────────────────────────────────────────
    ShowNodes,
    ShowNode {
        node_id: String,
    },
    RemoveNode {
        node_id: String,
    },
    ShowCluster,
    ShowMigrations,
    ShowRanges,
    ShowRouting,
    ShowSchemaVersion,
    ShowPeerHealth,
    Rebalance,
    ShowRaftGroups,
    ShowRaftGroup {
        group_id: String,
    },
    AlterRaftGroup {
        raw_sql: String,
    },

    // ── Maintenance ──────────────────────────────────────────────
    Analyze {
        collection: Option<String>,
    },
    Compact {
        collection: String,
    },
    ShowStorage {
        collection: Option<String>,
    },
    ShowCompactionStatus,

    // ── User / auth / grant ──────────────────────────────────────
    CreateUser {
        raw_sql: String,
    },
    DropUser {
        username: String,
    },
    AlterUser {
        raw_sql: String,
    },
    ShowUsers,
    GrantRole {
        raw_sql: String,
    },
    RevokeRole {
        raw_sql: String,
    },
    GrantPermission {
        raw_sql: String,
    },
    RevokePermission {
        raw_sql: String,
    },
    ShowPermissions {
        collection: Option<String>,
    },
    ShowGrants {
        username: Option<String>,
    },

    // ── Miscellaneous ────────────────────────────────────────────
    ShowTenants,
    ShowAuditLog,
    ShowConstraints {
        collection: String,
    },
    ShowTypeGuards {
        collection: String,
    },

    /// Catch-all for DDL-like commands not yet promoted to their
    /// own variant. Preserves the raw SQL for the legacy dispatch
    /// path so new variants can be added incrementally without
    /// breaking existing handlers.
    Other {
        raw_sql: String,
    },
}
