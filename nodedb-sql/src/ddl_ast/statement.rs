//! The [`NodedbStatement`] enum — one variant per DDL command.

/// Typed representation of every NodeDB DDL statement.
///
/// Handlers receive a fully-parsed variant instead of raw `&[&str]`
/// parts, eliminating array-index panics and enabling exhaustive
/// match coverage for new DDL commands.
#[derive(Debug, Clone, PartialEq)]
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
        /// `DROP COLLECTION <n> PURGE` — skip the soft-delete step and
        /// go straight to `CatalogEntry::PurgeCollection`. Requires
        /// superuser or tenant_admin. Fails if dependents exist unless
        /// `cascade` is also set.
        purge: bool,
        /// `DROP COLLECTION <n> CASCADE` — recursively drop dependents
        /// (triggers, RLS, MVs sourcing the collection, change streams,
        /// schedules). Rejects on cascade cycles (32-deep limit).
        cascade: bool,
        /// `DROP COLLECTION <n> CASCADE FORCE` — only difference from
        /// `cascade`: also drops schedules whose SQL carries
        /// `references_unknown = true`. Requires superuser.
        cascade_force: bool,
    },
    /// `UNDROP COLLECTION <n>` — restore a soft-deleted collection
    /// whose retention window has not yet elapsed. Flips
    /// `StoredCollection.is_active` back to true via a new
    /// `PutCollection` catalog entry. Fails if the retention window
    /// has already expired (storage is already purged) or if an
    /// active collection of the same name exists.
    UndropCollection {
        name: String,
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

    // ── Graph DSL ─────────────────────────────────────────────────
    //
    // Typed variants replace substring-matched parsing in the
    // pgwire handlers. The `ddl_ast::graph_parse` module is the
    // single source of truth for graph-DSL syntax — quote- and
    // brace-aware, so node ids / labels / property values that
    // shadow DSL keywords cannot short-circuit extraction.
    GraphInsertEdge {
        collection: String,
        src: String,
        dst: String,
        label: String,
        properties: GraphProperties,
    },
    GraphDeleteEdge {
        collection: String,
        src: String,
        dst: String,
        label: String,
    },
    /// `GRAPH LABEL` / `GRAPH UNLABEL`.
    GraphSetLabels {
        node_id: String,
        labels: Vec<String>,
        /// `true` for `UNLABEL`, `false` for `LABEL`.
        remove: bool,
    },
    GraphTraverse {
        start: String,
        depth: usize,
        edge_label: Option<String>,
        direction: GraphDirection,
    },
    GraphNeighbors {
        node: String,
        edge_label: Option<String>,
        direction: GraphDirection,
    },
    GraphPath {
        src: String,
        dst: String,
        max_depth: usize,
        edge_label: Option<String>,
    },
    GraphAlgo {
        algorithm: String,
        collection: String,
        damping: Option<f64>,
        tolerance: Option<f64>,
        resolution: Option<f64>,
        max_iterations: Option<usize>,
        sample_size: Option<usize>,
        source_node: Option<String>,
        direction: Option<String>,
        mode: Option<String>,
    },
    /// `MATCH (x)-[:l]->(y) RETURN x, y` — the query body is
    /// compiled deep inside the Data Plane via
    /// `engine::graph::pattern::compiler::parse`, so the AST
    /// variant just captures the raw SQL for that consumer.
    MatchQuery {
        raw_sql: String,
    },

    /// `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`
    ///
    /// All numeric and label options are optional at parse time; the pgwire
    /// handler applies defaults and caps so validation errors surface with
    /// proper SQLSTATE codes rather than parser panics.
    ///
    /// `query_vector` is `None` when no `ARRAY[…]` clause was found — the
    /// handler rejects such requests with SQLSTATE 42601.
    GraphRagFusion {
        collection: String,
        params: crate::ddl_ast::graph_parse::FusionParams,
    },

    /// Catch-all for DDL-like commands not yet promoted to their
    /// own variant. Preserves the raw SQL for the legacy dispatch
    /// path so new variants can be added incrementally without
    /// breaking existing handlers.
    Other {
        raw_sql: String,
    },
}

/// Traversal direction for graph DSL variants. Mirrors the engine's
/// own `Direction` enum so `nodedb-sql` has no dependency cycle with
/// `nodedb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphDirection {
    In,
    Out,
    Both,
}

/// The `PROPERTIES` clause of `GRAPH INSERT EDGE`. Captured in its
/// source form so the pgwire handler — which already depends on a
/// JSON serializer (sonic_rs) — can do the conversion to storage
/// bytes without dragging JSON deps into `nodedb-sql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphProperties {
    None,
    /// Raw `{ ... }` object-literal span, including the outer braces
    /// and brace-balanced inner content. Parsed by
    /// `crate::parser::object_literal` at the handler boundary.
    Object(String),
    /// Content of `'...'` (outer quotes stripped, `''` un-escaped);
    /// expected to already be a JSON document.
    Quoted(String),
}
