use datafusion::logical_expr::{FetchType, LogicalPlan};
use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{DocumentOp, KvOp, QueryOp, TextOp, TimeseriesOp};
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::credential::CredentialStore;
use crate::types::{TenantId, VShardId};

use super::extract::{expr_to_scan_filters, expr_to_usize, try_range_scan_from_predicate};
use super::search::try_extract_text_match_predicate;
use super::sql_expr_convert::try_convert_projection;

/// Converts DataFusion logical plans into NodeDB physical tasks.
///
/// This is the bridge between DataFusion's `Send` logical plans and
/// our `!Send` Data Plane execution. The converter walks the logical
/// plan tree and produces `PhysicalPlan` variants that the CoreLoop
/// can execute.
///
/// Lives on the Control Plane (Send + Sync).
#[derive(Default)]
pub struct PlanConverter {
    /// Optional catalog reference for collection-type-aware routing.
    credentials: Option<std::sync::Arc<CredentialStore>>,
}

impl PlanConverter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a converter with catalog access for timeseries detection.
    pub fn with_credentials(credentials: std::sync::Arc<CredentialStore>) -> Self {
        Self {
            credentials: Some(credentials),
        }
    }

    /// Check if a collection is a timeseries collection.
    fn is_timeseries(&self, tenant_id: TenantId, collection: &str) -> bool {
        if let Some(ref creds) = self.credentials
            && let Some(catalog) = creds.catalog()
            && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), collection)
        {
            return coll.collection_type.is_timeseries();
        }
        false
    }

    /// Check if a collection is a KV collection.
    pub(super) fn is_kv(&self, tenant_id: TenantId, collection: &str) -> bool {
        if let Some(ref creds) = self.credentials
            && let Some(catalog) = creds.catalog()
            && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), collection)
        {
            return coll.collection_type.is_kv();
        }
        false
    }

    /// Convert a DataFusion logical plan into one or more physical tasks.
    ///
    /// Simple queries (point gets, scans) produce a single task.
    /// Complex queries (joins, aggregations) may produce multiple tasks
    /// targeting different vShards.
    pub fn convert(
        &self,
        plan: &LogicalPlan,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        match plan {
            LogicalPlan::Projection(proj) => {
                let mut tasks = self.convert(&proj.input, tenant_id)?;

                // Try to convert computed expressions (e.g., price * qty AS total).
                if let Some(computed) = try_convert_projection(&proj.expr) {
                    let computed_bytes = rmp_serde::to_vec_named(&computed).unwrap_or_default();
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan {
                            computed_columns, ..
                        }) = &mut task.plan
                        {
                            *computed_columns = computed_bytes.clone();
                        }
                    }
                    return Ok(tasks);
                }

                // Simple column projection (no computed expressions).
                let columns: Vec<String> = proj
                    .expr
                    .iter()
                    .filter_map(|e| {
                        if let Expr::Column(col) = e {
                            Some(col.name.clone())
                        } else if let Expr::Alias(a) = e
                            && let Expr::Column(col) = &*a.expr
                        {
                            Some(col.name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                if !columns.is_empty() {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { projection, .. }) =
                            &mut task.plan
                        {
                            *projection = columns.clone();
                        }
                    }
                }

                Ok(tasks)
            }

            LogicalPlan::Filter(filter) => {
                // Check for text_match() UDF in filter → TextSearch plan.
                if let Some((collection, query_text)) =
                    try_extract_text_match_predicate(&filter.predicate, &filter.input)
                {
                    let vshard = VShardId::from_collection(&collection);
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Text(TextOp::Search {
                            collection,
                            query: query_text,
                            top_k: 1000,
                            fuzzy: true,
                        }),
                    }]);
                }

                // Check if the filter predicate can be converted to a point get
                // before recursing into the input.
                if let LogicalPlan::TableScan(scan) = filter.input.as_ref() {
                    let collection = scan.table_name.to_string().to_lowercase();
                    let vshard = VShardId::from_collection(&collection);

                    // KV routing: emit KvScan with filters for KV collections.
                    if self.is_kv(tenant_id, &collection) {
                        let filters = expr_to_scan_filters(&filter.predicate);
                        let filter_bytes =
                            rmp_serde::to_vec_named(&filters).map_err(|e| {
                                crate::Error::Serialization {
                                    format: "msgpack".into(),
                                    detail: format!("kv filter serialization: {e}"),
                                }
                            })?;
                        let limit = scan.fetch.unwrap_or(1000);
                        return Ok(vec![PhysicalTask {
                            tenant_id,
                            vshard_id: vshard,
                            plan: PhysicalPlan::Kv(KvOp::Scan {
                                collection,
                                cursor: Vec::new(),
                                count: limit,
                                filters: filter_bytes,
                                match_pattern: None,
                            }),
                        }]);
                    }

                    if let Some(task) = self.try_point_get(
                        &collection,
                        std::slice::from_ref(&filter.predicate),
                        tenant_id,
                        vshard,
                    )? {
                        return Ok(vec![task]);
                    }

                    // Try secondary index: equality or range on a non-id field → RangeScan.
                    if let Some(task) = try_range_scan_from_predicate(
                        &collection,
                        &filter.predicate,
                        tenant_id,
                        vshard,
                    ) {
                        return Ok(vec![task]);
                    }

                    // Not a point get or indexed scan — emit DocumentScan with filters.
                    let filters = expr_to_scan_filters(&filter.predicate);
                    let filter_bytes = rmp_serde::to_vec_named(&filters).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("filter serialization: {e}"),
                        }
                    })?;
                    let limit = scan.fetch.unwrap_or(1000);

                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Document(DocumentOp::Scan {
                            collection,
                            limit,
                            offset: 0,
                            sort_keys: Vec::new(),
                            filters: filter_bytes,
                            distinct: false,
                            projection: Vec::new(),
                            computed_columns: Vec::new(),
                            window_functions: Vec::new(),
                        }),
                    }]);
                }
                // Filter wrapping Aggregate = HAVING clause.
                if matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_)) {
                    let mut tasks = self.convert(&filter.input, tenant_id)?;
                    let having_filters = expr_to_scan_filters(&filter.predicate);
                    let having_bytes = rmp_serde::to_vec_named(&having_filters).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("having serialization: {e}"),
                        }
                    })?;
                    for task in &mut tasks {
                        if let PhysicalPlan::Query(QueryOp::Aggregate { having, .. }) =
                            &mut task.plan
                        {
                            *having = having_bytes.clone();
                        }
                    }
                    return Ok(tasks);
                }

                // Filter wrapping something else — recurse and apply filters.
                let mut tasks = self.convert(&filter.input, tenant_id)?;
                let filters = expr_to_scan_filters(&filter.predicate);
                let filter_bytes =
                    rmp_serde::to_vec_named(&filters).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("filter serialization: {e}"),
                    })?;
                for task in &mut tasks {
                    if let PhysicalPlan::Document(DocumentOp::Scan { filters, .. }) =
                        &mut task.plan
                    {
                        *filters = filter_bytes.clone();
                    }
                }
                Ok(tasks)
            }

            LogicalPlan::TableScan(scan) => {
                let collection = scan.table_name.to_string().to_lowercase();
                let vshard = VShardId::from_collection(&collection);

                // Timeseries routing: if collection is timeseries, emit TimeseriesScan.
                if self.is_timeseries(tenant_id, &collection) {
                    let limit = scan.fetch.unwrap_or(10_000);
                    let (time_range, filter_bytes) =
                        super::converter_helpers::extract_timeseries_filters(&scan.filters)?;
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                            collection,
                            time_range,
                            projection: Vec::new(),
                            limit,
                            filters: filter_bytes,
                            bucket_interval_ms: 0,
                        }),
                    }]);
                }

                // KV routing: if collection is KV, emit KvScan.
                if self.is_kv(tenant_id, &collection) {
                    let limit = scan.fetch.unwrap_or(1000);
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Kv(KvOp::Scan {
                            collection,
                            cursor: Vec::new(),
                            count: limit,
                            filters: Vec::new(),
                            match_pattern: None,
                        }),
                    }]);
                }

                // Check for filter pushdown: equality on id → point get.
                if let Some(task) =
                    self.try_point_get(&collection, &scan.filters, tenant_id, vshard)?
                {
                    return Ok(vec![task]);
                }

                // Default: full document scan.
                let limit = scan.fetch.unwrap_or(1000);

                // Convert any TableScan filters to scan filters.
                let filter_bytes = if !scan.filters.is_empty() {
                    let mut all_filters = Vec::new();
                    for f in &scan.filters {
                        all_filters.extend(expr_to_scan_filters(f));
                    }
                    rmp_serde::to_vec_named(&all_filters).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("filter serialization: {e}"),
                        }
                    })?
                } else {
                    Vec::new()
                };

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::Scan {
                        collection,
                        limit,
                        offset: 0,
                        sort_keys: Vec::new(),
                        filters: filter_bytes,
                        distinct: false,
                        projection: Vec::new(),
                        computed_columns: Vec::new(),
                        window_functions: Vec::new(),
                    }),
                }])
            }

            LogicalPlan::Limit(limit_plan) => {
                let mut tasks = self.convert(&limit_plan.input, tenant_id)?;

                // Extract LIMIT (fetch) value.
                if let Ok(FetchType::Literal(Some(n))) = limit_plan.get_fetch_type() {
                    for task in &mut tasks {
                        match &mut task.plan {
                            PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) => *limit = n,
                            PhysicalPlan::Document(DocumentOp::RangeScan { limit, .. }) => {
                                *limit = n
                            }
                            _ => {}
                        }
                    }
                }

                // Extract OFFSET (skip) value.
                if let Some(skip) = &limit_plan.skip
                    && let Ok(skip_n) = expr_to_usize(skip)
                {
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan { offset, .. }) =
                            &mut task.plan
                        {
                            *offset = skip_n;
                        }
                    }
                }

                Ok(tasks)
            }

            LogicalPlan::SubqueryAlias(alias) => self.convert(&alias.input, tenant_id),

            LogicalPlan::Sort(sort) => self.convert_sort(sort, tenant_id),

            LogicalPlan::Aggregate(agg) => {
                super::converter_helpers::convert_aggregate(agg, tenant_id)
            }

            LogicalPlan::Dml(dml) => self.convert_dml(dml, tenant_id),

            LogicalPlan::Join(join) => super::join::convert_join(join, tenant_id),

            // DISTINCT: recurse into input, then mark DocumentScan for dedup.
            LogicalPlan::Distinct(distinct) => {
                let mut tasks = self.convert(distinct.input(), tenant_id)?;
                for task in &mut tasks {
                    if let PhysicalPlan::Document(DocumentOp::Scan { distinct, .. }) =
                        &mut task.plan
                    {
                        *distinct = true;
                    }
                }
                Ok(tasks)
            }

            // Window functions: convert inner plan + extract window specs.
            LogicalPlan::Window(window) => {
                let mut tasks = self.convert(&window.input, tenant_id)?;

                // Convert window expressions to WindowFuncSpec.
                let specs = super::sql_expr_convert::convert_window_exprs(&window.window_expr);
                if !specs.is_empty() {
                    let spec_bytes = rmp_serde::to_vec_named(&specs).unwrap_or_default();
                    for task in &mut tasks {
                        if let PhysicalPlan::Document(DocumentOp::Scan {
                            window_functions, ..
                        }) = &mut task.plan
                        {
                            *window_functions = spec_bytes.clone();
                        }
                    }
                }

                Ok(tasks)
            }

            // Subqueries: DataFusion's optimizer decorrelates many subqueries
            // into joins before they reach the converter. If a Subquery node
            // arrives here, it means the optimizer couldn't decorrelate it.
            // We handle it by converting the subquery's inner plan — this
            // works for uncorrelated subqueries (IN, EXISTS, scalar).
            // Correlated subqueries that reference outer columns will fail
            // at execution time if the Data Plane can't resolve them.
            LogicalPlan::Subquery(subquery) => self.convert(&subquery.subquery, tenant_id),

            // UNION / UNION ALL: convert each input plan and concatenate tasks.
            // The Data Plane executes each sub-plan independently; the Control
            // Plane concatenates the result sets (UNION ALL) or deduplicates
            // (UNION DISTINCT — handled by wrapping in Distinct if needed).
            LogicalPlan::Union(union) => {
                let mut all_tasks = Vec::new();
                for input in &union.inputs {
                    let tasks = self.convert(input, tenant_id)?;
                    all_tasks.extend(tasks);
                }
                Ok(all_tasks)
            }

            LogicalPlan::RecursiveQuery(_) => Err(crate::Error::PlanError {
                detail: "WITH RECURSIVE is not yet supported. Use iterative queries or graph traversal (GRAPH TRAVERSE) instead.".into(),
            }),

            _ => Err(crate::Error::PlanError {
                detail: format!("unsupported logical plan type: {}", plan.display()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    async fn make_context_with_table() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE users (id VARCHAR, name VARCHAR, email VARCHAR) AS VALUES ('u1', 'alice', 'a@b.com'), ('u2', 'bob', 'b@c.com')")
            .await
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn converts_point_get() {
        let ctx = make_context_with_table().await;
        let df = ctx
            .sql("SELECT * FROM users WHERE id = 'u1'")
            .await
            .unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);

        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::PointGet { document_id, .. }) => {
                assert_eq!(document_id, "u1");
            }
            other => panic!("expected PointGet, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn converts_table_scan() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT name FROM users").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::Document(DocumentOp::Scan { .. })
        ));
    }

    #[tokio::test]
    async fn limit_propagates() {
        let ctx = make_context_with_table().await;
        let df = ctx.sql("SELECT * FROM users LIMIT 5").await.unwrap();
        let plan = df.into_optimized_plan().unwrap();

        let converter = PlanConverter::new();
        let tasks = converter.convert(&plan, TenantId::new(1)).unwrap();
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) => assert_eq!(*limit, 5),
            other => panic!("expected DocumentScan, got {other:?}"),
        }
    }
}
