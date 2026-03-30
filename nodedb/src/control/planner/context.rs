use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

use crate::control::planner::converter::PlanConverter;
use crate::control::security::credential::CredentialStore;

use super::catalog::NodeDbSchemaProvider;

/// DataFusion session context for the Control Plane.
///
/// Wraps a DataFusion `SessionContext` configured for NodeDB's hybrid
/// execution model. SQL/DSL queries are parsed and logically planned here,
/// then converted to `PhysicalPlan` variants for dispatch to the Data Plane.
///
/// This type is `Send + Sync` — lives on the Control Plane (Tokio).
pub struct QueryContext {
    session: SessionContext,
    converter: PlanConverter,
}

impl QueryContext {
    /// Create a new query context without catalog integration.
    ///
    /// Collections won't be resolvable by name. Use `with_catalog()` for
    /// production use where `SELECT * FROM <collection>` should work.
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");

        let session = SessionContext::new_with_config(config);
        register_udfs(&session);

        Self {
            session,
            converter: PlanConverter::new(),
        }
    }

    /// Create a query context with catalog integration.
    ///
    /// Collections stored in the system catalog will be visible to DataFusion,
    /// enabling `SELECT * FROM <collection>` to resolve correctly.
    ///
    /// Also loads all user-defined functions for the tenant from the catalog
    /// and registers them as DataFusion ScalarUDFs + an inlining AnalyzerRule.
    pub fn with_catalog(credentials: Arc<CredentialStore>, tenant_id: u32) -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");

        let session = SessionContext::new_with_config(config);
        register_udfs(&session);

        // Load and register user-defined functions for this tenant.
        register_user_functions(&session, &credentials, tenant_id);

        // Register our custom schema provider so DataFusion can resolve
        // collection names during SQL planning.
        let schema_provider = Arc::new(NodeDbSchemaProvider::new(
            Arc::clone(&credentials),
            tenant_id,
        ));
        let catalog = MemoryCatalogProvider::new();
        catalog
            .register_schema("public", schema_provider)
            .expect("register schema");
        session.register_catalog("nodedb", Arc::new(catalog));

        Self {
            session,
            converter: PlanConverter::with_credentials(credentials),
        }
    }

    /// Parse a SQL string into a DataFusion logical plan.
    pub async fn sql_to_logical(
        &self,
        sql: &str,
    ) -> crate::Result<datafusion::logical_expr::LogicalPlan> {
        let df = self
            .session
            .sql(sql)
            .await
            .map_err(|e| crate::Error::PlanError {
                detail: format!("SQL parse: {e}"),
            })?;
        let plan = df
            .into_optimized_plan()
            .map_err(|e| crate::Error::PlanError {
                detail: format!("optimization: {e}"),
            })?;
        Ok(plan)
    }

    /// Parse SQL and convert to NodeDB physical plan(s).
    ///
    /// Returns one or more `PhysicalTask` for dispatch to the Data Plane.
    pub async fn plan_sql(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        let logical = self.sql_to_logical(sql).await?;
        self.converter.convert(&logical, tenant_id)
    }

    /// Parse SQL, convert to physical plan, and inject RLS predicates.
    ///
    /// This is the primary query entry point when RLS is active. It:
    /// 1. Parses SQL → logical plan via DataFusion.
    /// 2. Converts logical → physical plan(s).
    /// 3. Injects RLS read predicates for each task's collection.
    ///
    /// Superusers bypass RLS (handled inside `inject_rls`).
    pub async fn plan_sql_with_rls(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        auth: &crate::control::security::auth_context::AuthContext,
        rls_store: &crate::control::security::rls::RlsPolicyStore,
    ) -> crate::Result<Vec<super::physical::PhysicalTask>> {
        let logical = self.sql_to_logical(sql).await?;
        let mut tasks = self.converter.convert(&logical, tenant_id)?;
        super::rls_injection::inject_rls(&mut tasks, rls_store, auth)?;
        Ok(tasks)
    }

    /// Access the underlying DataFusion session for advanced configuration
    /// (e.g., registering UDFs, table providers).
    pub fn session(&self) -> &SessionContext {
        &self.session
    }

    /// Register a custom scalar UDF with the DataFusion context.
    pub fn register_udf(&self, udf: datafusion::logical_expr::ScalarUDF) {
        self.session.register_udf(udf);
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Register all system UDFs on an arbitrary `SessionContext`.
///
/// Exposed for use by the DDL body validator (CREATE FUNCTION) which needs
/// a temporary session with all system UDFs registered.
pub fn register_udfs_on(session: &SessionContext) {
    register_udfs(session);
}

/// Load user-defined functions from the catalog and register them with DataFusion.
///
/// For each stored function:
/// 1. Creates a `UserDefinedFunction` (ScalarUDFImpl) for vectorized fallback execution.
/// 2. Adds the function to an `InlineUserFunctions` AnalyzerRule for plan-level inlining.
///
/// The AnalyzerRule is only registered if there are user functions to inline.
fn register_user_functions(
    session: &SessionContext,
    credentials: &CredentialStore,
    tenant_id: u32,
) {
    use super::udf::{InlineUserFunctions, UserDefinedFunction};
    use datafusion::logical_expr::ScalarUDF;

    let catalog = match credentials.catalog() {
        Some(c) => c,
        None => return, // In-memory mode (tests) — no catalog, no user UDFs.
    };

    let functions = match catalog.load_functions_for_tenant(tenant_id) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(tenant_id, error = %e, "failed to load user functions");
            return;
        }
    };

    if functions.is_empty() {
        return;
    }

    let mut inliner = InlineUserFunctions::new();

    for func in &functions {
        // Register as ScalarUDF (vectorized fallback).
        if let Some(udf_impl) = UserDefinedFunction::from_stored(func) {
            let param_names = udf_impl.param_names().to_vec();
            let body_sql = udf_impl.body_sql().to_string();

            session.register_udf(ScalarUDF::new_from_impl(udf_impl));

            // Also register for inlining.
            inliner.add(func.name.clone(), param_names, body_sql);
        } else {
            tracing::warn!(
                function = %func.name,
                "skipping user function with unmappable types"
            );
        }
    }

    // Register the inlining analyzer rule.
    if !inliner.is_empty() {
        session.add_analyzer_rule(Arc::new(inliner));
    }
}

fn register_udfs(session: &SessionContext) {
    use super::udf::spatial::{
        GeoDistance, StContains, StDistance, StDwithin, StIntersects, StWithin,
    };
    use super::udf::{
        Bm25Score, DocArrayContains, DocExists, DocGet, MultiVectorSearch, RrfScore, TextMatch,
        VectorDistance,
    };
    use datafusion::logical_expr::ScalarUDF;
    session.register_udf(ScalarUDF::new_from_impl(DocGet::new()));
    session.register_udf(ScalarUDF::new_from_impl(DocExists::new()));
    session.register_udf(ScalarUDF::new_from_impl(DocArrayContains::new()));
    session.register_udf(ScalarUDF::new_from_impl(VectorDistance::new()));
    session.register_udf(ScalarUDF::new_from_impl(MultiVectorSearch::new()));
    session.register_udf(ScalarUDF::new_from_impl(RrfScore::new()));
    session.register_udf(ScalarUDF::new_from_impl(Bm25Score::new()));
    session.register_udf(ScalarUDF::new_from_impl(TextMatch::new()));
    // Spatial UDFs.
    session.register_udf(ScalarUDF::new_from_impl(StDwithin::new()));
    session.register_udf(ScalarUDF::new_from_impl(StContains::new()));
    session.register_udf(ScalarUDF::new_from_impl(StIntersects::new()));
    session.register_udf(ScalarUDF::new_from_impl(StWithin::new()));
    session.register_udf(ScalarUDF::new_from_impl(StDistance::new()));
    session.register_udf(ScalarUDF::new_from_impl(GeoDistance::new()));
    // Timeseries UDFs (window + aggregate).
    nodedb_query::ts_udfs::register_timeseries_udfs(session);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parse_simple_select() {
        let ctx = QueryContext::new();

        // Register a dummy table so the SQL parses.
        ctx.session()
            .sql("CREATE TABLE users (id INT, name VARCHAR, email VARCHAR) AS VALUES (1, 'alice', 'a@b.com')")
            .await
            .unwrap();

        let plan = ctx
            .sql_to_logical("SELECT id, name FROM users WHERE id = 1")
            .await;
        assert!(plan.is_ok(), "failed: {:?}", plan.err());
    }

    #[tokio::test]
    async fn invalid_sql_returns_error() {
        let ctx = QueryContext::new();
        let result = ctx.sql_to_logical("SELECTT * FROMM nowhere").await;
        assert!(result.is_err());
    }
}
