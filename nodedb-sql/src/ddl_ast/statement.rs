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
        /// Canonical engine name (e.g. `"kv"`, `"vector"`, `"document_strict"`).
        /// `None` means no `engine=` key was present — the handler applies its
        /// default (`document_schemaless` for `CREATE COLLECTION`).
        engine: Option<String>,
        /// `(col_name, col_type)` pairs extracted from the parenthesised column
        /// list, e.g. `[(id, BIGINT), (name, TEXT)]`. Empty when no column
        /// list is present.
        columns: Vec<(String, String)>,
        /// Key-value pairs from the `WITH (...)` clause, excluding `engine=`
        /// (already in `engine`). E.g. `[(partition_by, 1h), (vector_field, emb)]`.
        options: Vec<(String, String)>,
        /// Free-standing modifier keywords detected at parse time:
        /// `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
        flags: Vec<String>,
        /// Raw interior of a `BALANCED ON (group_key = col, ...)` clause,
        /// or `None` when the clause is absent. The handler calls
        /// `parse_balanced_clause_from_raw` to produce the typed constraint.
        balanced_raw: Option<String>,
    },
    /// `CREATE TABLE <name> (<col_list>)` — Postgres-style strict-default DDL.
    ///
    /// Parsed from `CREATE TABLE` (not `CREATE COLLECTION`). The handler
    /// infers strict relational mode unless overridden via `WITH (engine='...')`.
    /// No column list → rejected with SQLSTATE `42601`.
    CreateTable {
        name: String,
        if_not_exists: bool,
        /// See `CreateCollection.engine`.
        engine: Option<String>,
        /// See `CreateCollection.columns`.
        columns: Vec<(String, String)>,
        /// See `CreateCollection.options`.
        options: Vec<(String, String)>,
        /// See `CreateCollection.flags`.
        flags: Vec<String>,
        /// See `CreateCollection.balanced_raw`.
        balanced_raw: Option<String>,
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
        /// Typed sub-operation — one variant per `ALTER COLLECTION` form.
        operation: AlterCollectionOp,
    },
    DescribeCollection {
        name: String,
    },
    ShowCollections,

    // ── Index ────────────────────────────────────────────────────
    CreateIndex {
        unique: bool,
        /// Optional explicit index name. `None` means auto-generate from
        /// `idx_{collection}_{field}` in the handler.
        index_name: Option<String>,
        /// Target collection name (lowercased).
        collection: String,
        /// Index field or path expression, e.g. `$.price` or `tags[]`.
        field: String,
        /// `COLLATE NOCASE` / `COLLATE CI` was present.
        case_insensitive: bool,
        /// Optional `WHERE` predicate text (original case, trimmed).
        where_condition: Option<String>,
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
        index_name: Option<String>,
        concurrent: bool,
    },

    // ── Trigger ──────────────────────────────────────────────────
    CreateTrigger {
        or_replace: bool,
        /// "ASYNC", "SYNC", or "DEFERRED".
        execution_mode: String,
        name: String,
        /// "BEFORE", "AFTER", or "INSTEAD OF".
        timing: String,
        events_insert: bool,
        events_update: bool,
        events_delete: bool,
        collection: String,
        /// "ROW" or "STATEMENT".
        granularity: String,
        when_condition: Option<String>,
        priority: i32,
        /// "INVOKER" or "DEFINER".
        security: String,
        body_sql: String,
    },
    DropTrigger {
        name: String,
        collection: String,
        if_exists: bool,
    },
    AlterTrigger {
        /// Trigger name (lowercased).
        name: String,
        /// "ENABLE", "DISABLE", or "OWNER" (when combined with `new_owner`).
        action: String,
        /// Present when `action == "OWNER"`.
        new_owner: Option<String>,
    },
    ShowTriggers {
        collection: Option<String>,
    },

    // ── Schedule ─────────────────────────────────────────────────
    CreateSchedule {
        name: String,
        cron_expr: String,
        body_sql: String,
        /// "NORMAL" or "LOCAL".
        scope: String,
        /// "SKIP", "CATCH_UP", or "QUEUE".
        missed_policy: String,
        allow_overlap: bool,
    },
    DropSchedule {
        name: String,
        if_exists: bool,
    },
    AlterSchedule {
        /// Schedule name (lowercased).
        name: String,
        /// "ENABLE", "DISABLE", or "SET".
        action: String,
        /// The quoted cron expression when `action == "SET"`.
        cron_expr: Option<String>,
    },
    ShowSchedules,
    ShowScheduleHistory {
        name: String,
    },

    // ── Sequence ─────────────────────────────────────────────────
    CreateSequence {
        name: String,
        if_not_exists: bool,
        /// `START [WITH] n` value, or `None` for engine default.
        start: Option<i64>,
        /// `INCREMENT [BY] n` value, or `None` for default (1).
        increment: Option<i64>,
        /// `MINVALUE n` value, or `None` for default.
        min_value: Option<i64>,
        /// `MAXVALUE n` value, or `None` for default.
        max_value: Option<i64>,
        /// `CYCLE` or `NO CYCLE`. Default: `false`.
        cycle: bool,
        /// `CACHE n` value, or `None` for default (1).
        cache: Option<i64>,
        /// Raw `FORMAT 'template'` string (quotes stripped), or `None`.
        /// The handler calls `parse_format_template` on this.
        format_template_raw: Option<String>,
        /// Raw `RESET YEARLY|MONTHLY|QUARTERLY|DAILY` token, or `None`.
        /// The handler calls `ResetScope::parse` on this.
        reset_period_raw: Option<String>,
        /// `GAP_FREE` modifier was present.
        gap_free: bool,
        /// `SCOPE TENANT` or similar scope specifier, or `None`.
        scope: Option<String>,
    },
    DropSequence {
        name: String,
        if_exists: bool,
    },
    AlterSequence {
        /// Sequence name (lowercased).
        name: String,
        /// "RESTART" or "FORMAT".
        action: String,
        /// For RESTART: optional WITH value token.
        /// For FORMAT: the quoted format string (quotes stripped).
        with_value: Option<String>,
    },
    DescribeSequence {
        name: String,
    },
    ShowSequences,

    // ── Alert ────────────────────────────────────────────────────
    CreateAlert {
        name: String,
        collection: String,
        /// Optional WHERE filter text (original case, between WHERE and CONDITION keywords).
        where_filter: Option<String>,
        /// Raw condition text after CONDITION keyword, e.g. "AVG(temperature) > 90.0".
        condition_raw: String,
        /// GROUP BY column list (lowercased).
        group_by: Vec<String>,
        /// WINDOW duration string (e.g. "5 minutes").
        window_raw: String,
        /// FOR 'N consecutive windows' count (default 1).
        fire_after: u32,
        /// RECOVER AFTER 'N consecutive windows' count (default 1).
        recover_after: u32,
        severity: String,
        /// Raw NOTIFY section text after NOTIFY keyword.
        notify_targets_raw: String,
    },
    DropAlert {
        name: String,
        if_exists: bool,
    },
    AlterAlert {
        /// Alert name (lowercased).
        name: String,
        /// "ENABLE" or "DISABLE".
        action: String,
    },
    ShowAlerts,
    ShowAlertStatus {
        name: String,
    },

    // ── Retention policy ─────────────────────────────────────────
    CreateRetentionPolicy {
        name: String,
        collection: String,
        /// Raw policy body — everything between the outer parentheses.
        body_raw: String,
        /// Optional EVAL_INTERVAL value string from WITH clause.
        eval_interval_raw: Option<String>,
    },
    DropRetentionPolicy {
        name: String,
        if_exists: bool,
    },
    AlterRetentionPolicy {
        /// Policy name (lowercased).
        name: String,
        /// "ENABLE", "DISABLE", or "SET".
        action: String,
        /// The SET target key, e.g. "AUTO_TIER" or "EVAL_INTERVAL".
        set_key: Option<String>,
        /// The raw value string for the SET target.
        set_value: Option<String>,
    },
    ShowRetentionPolicies,

    // ── Change stream ────────────────────────────────────────────
    CreateChangeStream {
        name: String,
        collection: String,
        /// Raw WITH clause key=value pairs string (everything inside the outer parens), or empty.
        with_clause_raw: String,
    },
    DropChangeStream {
        name: String,
        if_exists: bool,
    },
    AlterChangeStream {
        /// Stream name (lowercased).
        name: String,
        /// Action token, e.g. "ENABLE", "DISABLE", "SUSPEND".
        action: String,
    },
    ShowChangeStreams,

    // ── Consumer group ───────────────────────────────────────────
    CreateConsumerGroup {
        /// Consumer group name.
        group_name: String,
        /// Change stream or topic name (after ON).
        stream_name: String,
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
        /// Policy name.
        name: String,
        /// Target collection name.
        collection: String,
        /// Policy type token: "READ", "WRITE", or "ALL".
        policy_type: String,
        /// Predicate expression text (outer parens stripped).
        predicate_raw: String,
        /// RESTRICTIVE modifier present.
        is_restrictive: bool,
        /// Optional ON DENY clause text (everything after ON DENY).
        on_deny_raw: Option<String>,
        /// Optional TENANT <id> override.
        tenant_id_override: Option<u64>,
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
        name: String,
        source: String,
        query_sql: String,
        /// "FULL", "INCREMENTAL", or "STREAMING".
        refresh_mode: String,
    },
    DropMaterializedView {
        name: String,
        if_exists: bool,
    },
    ShowMaterializedViews,

    // ── Continuous aggregate ─────────────────────────────────────
    CreateContinuousAggregate {
        name: String,
        source: String,
        /// BUCKET interval string (e.g. "5m").
        bucket_raw: String,
        /// Raw AGGREGATE expressions text (everything after AGGREGATE keyword up to GROUP BY or WITH).
        aggregate_exprs_raw: String,
        /// GROUP BY column list (lowercased).
        group_by: Vec<String>,
        /// Raw WITH clause inner text (everything inside outer parens), or empty.
        with_clause_raw: String,
    },
    DropContinuousAggregate {
        name: String,
        if_exists: bool,
    },
    ShowContinuousAggregates,

    // ── Backup / restore ─────────────────────────────────────────
    BackupTenant {
        /// Tenant ID as a string token from the SQL.
        tenant_id: String,
    },
    RestoreTenant {
        dry_run: bool,
        /// Tenant ID as a string token from the SQL.
        tenant_id: String,
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
        /// Raft group ID as a string token from the SQL.
        group_id: String,
        /// "ADD" or "REMOVE".
        action: String,
        /// Node ID as a string token from the SQL.
        node_id: String,
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
        username: String,
        password: String,
        /// Role name string (e.g. "read_write", "admin"); handler converts to Role enum.
        role: Option<String>,
        /// Optional tenant ID override (superuser only).
        tenant_id: Option<u64>,
    },
    DropUser {
        username: String,
    },
    AlterUser {
        username: String,
        /// Typed sub-operation — one variant per `ALTER USER` form.
        op: AlterUserOp,
    },
    ShowUsers,
    /// `ALTER ROLE <name> GRANT/REVOKE/SET` — typed sub-operations.
    ///
    /// Folds three ALTER ROLE forms into one variant:
    /// - `ALTER ROLE <name> GRANT <perm> ON [FUNCTION] <target>`
    /// - `ALTER ROLE <name> REVOKE <perm> ON [FUNCTION] <target>`
    /// - `ALTER ROLE <name> SET INHERIT <parent>`
    AlterRole {
        /// Role name (original case preserved).
        name: String,
        sub_op: AlterRoleOp,
    },
    GrantRole {
        /// Role name token from the SQL.
        role: String,
        /// Target username (token after TO).
        username: String,
    },
    RevokeRole {
        /// Role name token from the SQL.
        role: String,
        /// Target username (token after FROM).
        username: String,
    },
    GrantPermission {
        /// Permission token, e.g. "READ", "WRITE", "ALL".
        permission: String,
        /// "COLLECTION" or "FUNCTION".
        target_type: String,
        /// The collection or function name.
        target_name: String,
        /// Grantee username (token after TO).
        grantee: String,
    },
    RevokePermission {
        /// Permission token, e.g. "READ", "WRITE", "ALL".
        permission: String,
        /// "COLLECTION" or "FUNCTION".
        target_type: String,
        /// The collection or function name.
        target_name: String,
        /// Grantee username (token after FROM).
        grantee: String,
    },
    ShowPermissions {
        /// Collection name from `ON <collection>` clause, or `None` for all.
        on_collection: Option<String>,
        /// Grantee name from `FOR <user|role>` clause, or `None` for all.
        for_grantee: Option<String>,
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
    /// variant just captures the original SQL for that consumer.
    MatchQuery {
        /// Full original SQL forwarded verbatim to
        /// `engine::graph::pattern::compiler::parse`.
        /// `nodedb-sql` does not interpret its contents.
        body: String,
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
}

/// Typed sub-operation for `ALTER COLLECTION <name> ...`.
///
/// Each variant corresponds to one ALTER sub-command parsed by
/// `nodedb-sql/src/ddl_ast/parse/collection.rs`. The handler in
/// `nodedb/src/control/server/pgwire/ddl/collection/alter/` matches
/// on this enum instead of rescanning raw SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterCollectionOp {
    /// `ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT expr]`
    AddColumn {
        column_name: String,
        column_type: String,
        not_null: bool,
        default_expr: Option<String>,
    },
    /// `DROP COLUMN <name>`
    DropColumn { column_name: String },
    /// `RENAME COLUMN <old> TO <new>`
    RenameColumn { old_name: String, new_name: String },
    /// `ALTER COLUMN <name> TYPE <type>`
    AlterColumnType {
        column_name: String,
        new_type: String,
    },
    /// `OWNER TO <user>`
    OwnerTo { new_owner: String },
    /// `SET RETENTION = '<duration>'`
    SetRetention { value: String },
    /// `SET APPEND_ONLY`
    SetAppendOnly,
    /// `SET LAST_VALUE_CACHE = TRUE|FALSE`
    SetLastValueCache { enabled: bool },
    /// `SET LEGAL_HOLD = TRUE|FALSE TAG '<tag>'`
    SetLegalHold { enabled: bool, tag: String },
    /// `ADD [COLUMN] <target_column> ... AS MATERIALIZED_SUM SOURCE <source_collection>
    /// ON <join_column> VALUE <value_expr>` — fully parsed by
    /// `nodedb-sql`; the handler receives typed fields and never
    /// rescans raw SQL.
    AddMaterializedSum {
        /// Target collection name (lowercased).
        target_collection: String,
        /// Target column name to hold the sum (lowercased).
        target_column: String,
        /// Source collection name (lowercased).
        source_collection: String,
        /// Join column on the source side (lowercased).
        join_column: String,
        /// Value expression (column name or qualified `source.column`, lowercased).
        value_expr: String,
    },
}

/// Typed sub-operation for `ALTER USER <name> ...`.
///
/// Five forms are supported:
/// - `SET PASSWORD '<pw>'` — change password
/// - `SET ROLE <role>` — change role
/// - `MUST CHANGE PASSWORD` — require password change on next login
/// - `PASSWORD NEVER EXPIRES` — clear expiry date
/// - `PASSWORD EXPIRES '<iso8601>'` or `PASSWORD EXPIRES IN <N> DAYS` — set expiry
#[derive(Debug, Clone, PartialEq)]
pub enum AlterUserOp {
    /// `SET PASSWORD '<password>'`
    SetPassword { password: String },
    /// `SET ROLE <role>`
    SetRole { role: String },
    /// `MUST CHANGE PASSWORD`
    MustChangePassword,
    /// `PASSWORD NEVER EXPIRES`
    PasswordNeverExpires,
    /// `PASSWORD EXPIRES '<iso8601_datetime>'`
    PasswordExpiresAt { iso8601: String },
    /// `PASSWORD EXPIRES IN <n> DAYS`
    PasswordExpiresInDays { days: u32 },
}

/// Typed sub-operation for `ALTER ROLE <name> ...`.
///
/// Three forms are supported:
/// - `GRANT <perm> ON [FUNCTION] <target>` — grant a permission to the role
/// - `REVOKE <perm> ON [FUNCTION] <target>` — revoke a permission from the role
/// - `SET INHERIT <parent>` — update role inheritance (original ALTER ROLE form)
#[derive(Debug, Clone, PartialEq)]
pub enum AlterRoleOp {
    /// `GRANT <perm> ON [FUNCTION] <target>`
    Grant {
        /// Permission token, e.g. "READ", "WRITE", "ALL".
        permission: String,
        /// "COLLECTION" or "FUNCTION".
        target_type: String,
        /// Collection or function name.
        target_name: String,
    },
    /// `REVOKE <perm> ON [FUNCTION] <target>`
    Revoke {
        /// Permission token, e.g. "READ", "WRITE", "ALL".
        permission: String,
        /// "COLLECTION" or "FUNCTION".
        target_type: String,
        /// Collection or function name.
        target_name: String,
    },
    /// `SET INHERIT <parent>`
    SetInherit {
        /// Parent role name.
        parent: String,
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
