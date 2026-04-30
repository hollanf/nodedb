//! NodeDbQueryParser — pgwire `QueryParser` implementation.
//!
//! Converts incoming SQL (from a Parse message) into a `ParsedStatement`
//! with inferred parameter types and result schema. Uses nodedb-sql for
//! schema resolution instead of DataFusion.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;

use crate::control::state::SharedState;

use super::statement::ParsedStatement;

/// Implements pgwire's `QueryParser` trait for NodeDB.
///
/// On Parse message: parses SQL via sqlparser, extracts placeholder types
/// from the catalog schema, and computes the result schema.
pub struct NodeDbQueryParser {
    state: Arc<SharedState>,
}

impl NodeDbQueryParser {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Infer parameter and result types using nodedb-sql catalog, scoped to
    /// the connecting user's tenant so a tenant-N user's Parse message
    /// resolves against tenant-N's catalog (not tenant 1).
    fn try_infer_types(
        &self,
        sql: &str,
        client_types: &[Option<Type>],
        tenant_id: u32,
    ) -> (Vec<Option<Type>>, Vec<FieldInfo>) {
        let catalog = crate::control::planner::catalog_adapter::OriginCatalog::new(
            Arc::clone(&self.state.credentials),
            tenant_id,
            Some(Arc::clone(&self.state.retention_policy_registry)),
        );

        // Placeholder inference runs unconditionally so an unplannable
        // SQL string (e.g. `WHERE id = $1` where the planner needs bound
        // params to typecheck) still reports the right number of
        // parameter slots in Describe.
        let param_count = count_placeholders(sql);
        let mut param_types = vec![None; param_count.max(client_types.len())];
        for (i, ct) in client_types.iter().enumerate() {
            if let Some(t) = ct {
                param_types[i] = Some(t.clone());
            }
        }

        // Parse and plan to get collection info for result schema.
        //
        // The planner type-checks WHERE/projection expressions, which
        // fails on raw `$N` placeholders (no bound value to typecheck).
        // For schema inference we only need the collection + projection
        // structure, so substitute placeholders with NULL literals just
        // for this planning pass. Execution re-plans with real bound
        // values.
        let sql_for_inference = substitute_placeholders_with_null(sql);
        let plans = match nodedb_sql::plan_sql(&sql_for_inference, &catalog) {
            Ok(p) => p,
            Err(_) => return (param_types, Vec::new()),
        };

        // Infer result fields.
        //
        // Prefer the explicit SELECT list from sqlparser: the planner
        // drops it for PointGet/PointLookup variants, but the projection
        // the client sees must match what they wrote in the SQL. When
        // the list is `*` or missing, fall back to the plan's collection
        // schema.
        let result_fields = if let Some(projection) = parse_select_projection(&sql_for_inference) {
            fields_from_projection(&projection, plans.first(), &catalog)
        } else if let Some(plan) = plans.first() {
            infer_result_fields(plan, &catalog)
        } else {
            Vec::new()
        };

        (param_types, result_fields)
    }
}

#[async_trait]
impl QueryParser for NodeDbQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Wire-streaming COPY shapes for backup/restore: bypass nodedb-sql
        // entirely. The Execute handler intercepts these via
        // `control::backup::detect`. Returning early avoids a fruitless
        // sqlparser pass on syntax it doesn't model.
        if crate::control::backup::detect(sql).is_some() {
            return Ok(ParsedStatement {
                sql: sql.to_owned(),
                param_types: Vec::new(),
                result_fields: Vec::new(),
                is_dsl: false,
                pg_catalog_table: None,
            });
        }

        // pg_catalog virtual tables: bypass the planner entirely — they
        // aren't real collections. Populate result_fields from the static
        // catalog schema so Describe can report column types before Bind.
        let upper = sql.to_uppercase();
        if let Some(table) =
            crate::control::server::pgwire::pg_catalog::extract_pg_catalog_table(&upper)
        {
            let result_fields =
                crate::control::server::pgwire::pg_catalog::pg_catalog_schema(table)
                    .unwrap_or_default();
            let count = count_placeholders(sql).max(types.len());
            let param_types: Vec<Option<Type>> = (0..count)
                .map(|i| types.get(i).and_then(|t| t.clone()))
                .collect();
            return Ok(ParsedStatement {
                sql: sql.to_owned(),
                param_types,
                result_fields,
                is_dsl: false,
                pg_catalog_table: Some(table),
            });
        }

        // Resolve the connecting user's tenant from pgwire metadata so
        // parse-time catalog lookups are scoped to the right tenant.
        // Unknown users fall back to tenant 1 only during bootstrap
        // (credential store empty) — otherwise parse-time inference
        // returns empty field info, which is the safe default.
        let tenant_id = client
            .metadata()
            .get("user")
            .and_then(|u| {
                self.state
                    .credentials
                    .to_identity(u, crate::control::security::identity::AuthMethod::Trust)
                    .or_else(|| {
                        self.state.credentials.to_identity(
                            u,
                            crate::control::security::identity::AuthMethod::ScramSha256,
                        )
                    })
            })
            .map(|id| id.tenant_id.as_u32())
            .unwrap_or(1);
        let (param_types, result_fields) = self.try_infer_types(sql, types, tenant_id);

        // If type inference produced no result fields and the SQL matches a
        // known DSL prefix, mark the statement as a DSL passthrough. The
        // Execute handler will route it through the full DSL dispatcher
        // (same as the simple-query path) instead of `execute_planned_sql_with_params`.
        let is_dsl = result_fields.is_empty() && is_dsl_statement(sql);

        Ok(ParsedStatement {
            sql: sql.to_owned(),
            param_types,
            result_fields,
            is_dsl,
            pg_catalog_table: None,
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt
            .param_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(stmt.result_fields.clone())
    }
}

/// Return true if `sql` starts with a DSL keyword that `plan_sql` cannot parse.
///
/// Mirrors the prefix checks in `ddl/router/dsl.rs` so the extended-query
/// Parse handler can mark such statements as DSL passthroughs and route them
/// through the DSL dispatcher at Execute time.
fn is_dsl_statement(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("SEARCH ")
        || upper.starts_with("GRAPH ")
        || upper.starts_with("MATCH ")
        || upper.starts_with("OPTIONAL MATCH ")
        || upper.starts_with("CRDT MERGE ")
        || upper.starts_with("UPSERT INTO ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE FULLTEXT INDEX ")
        || upper.starts_with("CREATE SEARCH INDEX ")
        || upper.starts_with("CREATE SPARSE INDEX ")
}

/// Parse the top-level SELECT projection list with sqlparser. Returns
/// the list of (column_name, is_star) pairs, or `None` if the SQL isn't
/// a simple SELECT or parsing fails. A `Star` entry signals "all
/// columns from the target collection".
enum ProjectionItem {
    Star,
    Named(String),
}

fn parse_select_projection(sql: &str) -> Option<Vec<ProjectionItem>> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let stmt = stmts.into_iter().next()?;
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    let mut out = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                out.push(ProjectionItem::Star);
            }
            SelectItem::UnnamedExpr(expr) => {
                out.push(ProjectionItem::Named(expr_column_name(expr)));
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                out.push(ProjectionItem::Named(alias.value.clone()));
            }
        }
    }
    Some(out)
}

/// Extract a reasonable column label from an expression. For a bare
/// identifier this is the identifier name; for compound identifiers,
/// the last segment; otherwise the stringified expression.
fn expr_column_name(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| expr.to_string()),
        other => other.to_string(),
    }
}

/// Build `FieldInfo`s from a parsed SELECT projection. Star entries
/// expand against the collection's schema (if available); named entries
/// use the typed column when found, else default to TEXT.
fn fields_from_projection(
    projection: &[ProjectionItem],
    plan: Option<&nodedb_sql::SqlPlan>,
    catalog: &dyn nodedb_sql::SqlCatalog,
) -> Vec<FieldInfo> {
    use pgwire::api::results::FieldFormat;

    let info = plan
        .and_then(|p| plan_collection(p))
        .and_then(|name| catalog.get_collection(name).ok().flatten());

    let mut fields = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            ProjectionItem::Star => {
                if let Some(info) = &info {
                    fields.extend(columns_to_field_info(&info.columns));
                }
            }
            ProjectionItem::Named(name) => {
                let pg_type = info
                    .as_ref()
                    .and_then(|info| info.columns.iter().find(|c| c.name == *name))
                    .map(|c| sql_data_type_to_pg(&c.data_type))
                    .unwrap_or(Type::TEXT);
                fields.push(FieldInfo::new(
                    name.clone(),
                    None,
                    None,
                    pg_type,
                    FieldFormat::Text,
                ));
            }
        }
    }
    fields
}

/// The collection targeted by a plan, if any. Used to pull typed schema
/// when building projection field info.
fn plan_collection(plan: &nodedb_sql::SqlPlan) -> Option<&str> {
    use nodedb_sql::SqlPlan;
    match plan {
        SqlPlan::Scan { collection, .. }
        | SqlPlan::PointGet { collection, .. }
        | SqlPlan::DocumentIndexLookup { collection, .. } => Some(collection.as_str()),
        SqlPlan::Aggregate { input, .. } => plan_collection(input),
        SqlPlan::Join { left, .. } => plan_collection(left),
        _ => None,
    }
}

/// Replace each `$N` placeholder in `sql` with the literal `NULL`.
/// Used only for Parse-time schema inference — the real bound values
/// are substituted at Execute time.
fn substitute_placeholders_with_null(sql: &str) -> String {
    let ranges = super::sql_placeholder::placeholder_ranges(sql);
    if ranges.is_empty() {
        return sql.to_owned();
    }
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;
    for (start, end, _idx) in ranges {
        out.push_str(&sql[cursor..start]);
        out.push_str("NULL");
        cursor = end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Count $1, $2, ... placeholders in SQL text.
fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    for (_, _, idx) in super::sql_placeholder::placeholder_ranges(sql) {
        if idx > max_idx {
            max_idx = max_idx.max(idx);
        }
    }
    max_idx
}

/// Infer result FieldInfo from a SqlPlan by looking up collection schema.
fn infer_result_fields(
    plan: &nodedb_sql::SqlPlan,
    catalog: &dyn nodedb_sql::SqlCatalog,
) -> Vec<FieldInfo> {
    use nodedb_sql::types::*;
    use pgwire::api::results::FieldFormat;

    let collection = match plan {
        SqlPlan::Scan { collection, .. } => collection,
        SqlPlan::PointGet { collection, .. } => collection,
        SqlPlan::DocumentIndexLookup { collection, .. } => collection,
        SqlPlan::ConstantResult { columns, .. } => {
            // FROM-less SELECT: one FieldInfo per aliased column.
            // Types are TEXT — bound params don't carry source-side type
            // info the planner can propagate here, and pgwire lets the
            // server encode any value as text.
            return columns
                .iter()
                .map(|name| FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text))
                .collect();
        }
        SqlPlan::Aggregate { aggregates, .. } => {
            // Aggregate output: one field per aggregate expression, named
            // by its alias. COUNT returns INT8; SUM/AVG return FLOAT8;
            // MIN/MAX keep source type but we default to TEXT since the
            // source column type isn't threaded here.
            // Values are text-encoded on the wire in the current extended-
            // query path; declaring INT8/FLOAT8 here would trip client-side
            // binary decoding. Keep TEXT so the client applies its own
            // parse against the alias.
            return aggregates
                .iter()
                .map(|agg| {
                    FieldInfo::new(agg.alias.clone(), None, None, Type::TEXT, FieldFormat::Text)
                })
                .collect();
        }
        SqlPlan::Join { left, right, .. } => {
            // Merge left + right schemas so both sides' columns are in
            // the Describe result.
            let mut fields = infer_result_fields(left, catalog);
            fields.extend(infer_result_fields(right, catalog));
            return fields;
        }
        _ => return Vec::new(),
    };

    // `infer_result_fields` runs on a successful plan so any
    // `RetryableSchemaChanged` drain signal would have surfaced
    // earlier. Treat any error here as "no field info", same as
    // None — the caller is non-critical (prepared-stmt
    // description fallback).
    let info = match catalog.get_collection(collection) {
        Ok(Some(i)) => i,
        Ok(None) | Err(_) => return Vec::new(),
    };

    // Check if projection specifies columns.
    let projected_cols = match plan {
        SqlPlan::Scan { projection, .. } => projection,
        SqlPlan::DocumentIndexLookup { projection, .. } => projection,
        _ => return columns_to_field_info(&info.columns),
    };

    if projected_cols.is_empty() || projected_cols.iter().any(|p| matches!(p, Projection::Star)) {
        return columns_to_field_info(&info.columns);
    }

    projected_cols
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => {
                let col = info.columns.iter().find(|c| c.name == *name);
                let pg_type = col
                    .map(|c| sql_data_type_to_pg(&c.data_type))
                    .unwrap_or(Type::TEXT);
                Some(FieldInfo::new(
                    name.clone(),
                    None,
                    None,
                    pg_type,
                    FieldFormat::Text,
                ))
            }
            Projection::Computed { alias, .. } => Some(FieldInfo::new(
                alias.clone(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )),
            _ => None,
        })
        .collect()
}

fn columns_to_field_info(columns: &[nodedb_sql::ColumnInfo]) -> Vec<FieldInfo> {
    use pgwire::api::results::FieldFormat;
    columns
        .iter()
        .map(|c| {
            FieldInfo::new(
                c.name.clone(),
                None,
                None,
                sql_data_type_to_pg(&c.data_type),
                FieldFormat::Text,
            )
        })
        .collect()
}

fn sql_data_type_to_pg(dt: &nodedb_sql::SqlDataType) -> Type {
    use nodedb_sql::types::SqlDataType;
    match dt {
        SqlDataType::Int64 => Type::INT8,
        SqlDataType::Float64 => Type::FLOAT8,
        SqlDataType::String => Type::TEXT,
        SqlDataType::Bool => Type::BOOL,
        SqlDataType::Bytes => Type::BYTEA,
        SqlDataType::Timestamp => Type::TIMESTAMP,
        SqlDataType::Timestamptz => Type::TIMESTAMPTZ,
        SqlDataType::Decimal => Type::NUMERIC,
        SqlDataType::Uuid => Type::TEXT,
        SqlDataType::Vector(_) => Type::BYTEA,
        SqlDataType::Geometry => Type::BYTEA,
    }
}
