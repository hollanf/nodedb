//! Lite query engine: full SQL via DataFusion over Loro documents.
//!
//! Manages a DataFusion `SessionContext` with collections registered
//! as table providers backed by the CRDT engine.

use std::sync::{Arc, Mutex};

use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::htap::HtapBridge;
use crate::engine::strict::StrictEngine;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::arrow_convert::arrow_value_at;
use super::columnar_provider::ColumnarTableProvider;
use super::strict_provider::StrictTableProvider;
use super::table_provider::LiteTableProvider;

/// Lite-side query engine wrapping DataFusion.
///
/// Registered collections appear as tables. SQL queries execute
/// entirely in-process against the Loro CRDT state, strict Binary Tuple store,
/// or columnar compressed segments.
pub struct LiteQueryEngine<S: StorageEngine> {
    pub(in crate::query) ctx: SessionContext,
    pub(in crate::query) crdt: Arc<Mutex<CrdtEngine>>,
    pub(in crate::query) strict: Arc<Mutex<StrictEngine<S>>>,
    pub(in crate::query) columnar: Arc<Mutex<ColumnarEngine<S>>>,
    pub(in crate::query) htap: Arc<Mutex<HtapBridge>>,
    pub(in crate::query) storage: Arc<S>,
    pub(in crate::query) timeseries:
        Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Create a new query engine.
    pub fn new(
        crdt: Arc<Mutex<CrdtEngine>>,
        strict: Arc<Mutex<StrictEngine<S>>>,
        columnar: Arc<Mutex<ColumnarEngine<S>>>,
        htap: Arc<Mutex<HtapBridge>>,
        storage: Arc<S>,
        timeseries: Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
    ) -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");

        let ctx = SessionContext::new_with_config(config);
        super::spatial_udf::register_spatial_udfs(&ctx);
        nodedb_query::ts_udfs::register_timeseries_udfs(&ctx);
        Self {
            ctx,
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
        }
    }

    /// Register a collection as a queryable table.
    ///
    /// Call this before executing SQL that references the collection.
    /// For auto-registration, call `register_all_collections()`.
    pub fn register_collection(&self, name: &str) {
        let provider = LiteTableProvider::new(name.to_string(), Arc::clone(&self.crdt));
        // Register directly via the session context.
        let _ = self.ctx.register_table(name, Arc::new(provider));
    }

    /// Register a strict collection as a queryable table.
    pub fn register_strict_collection(&self, name: &str) {
        let strict = match self.strict.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        if let Some(schema) = strict.schema(name) {
            let provider =
                StrictTableProvider::new(name.to_string(), schema, Arc::clone(&self.storage));
            let _ = self.ctx.register_table(name, Arc::new(provider));
        }
    }

    /// Register all existing collections as tables (both CRDT and strict).
    pub fn register_all_collections(&self) {
        // Register CRDT (schemaless) collections.
        let crdt = match self.crdt.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        let crdt_collections = crdt.collection_names();
        drop(crdt);

        for name in &crdt_collections {
            if name.starts_with("__") {
                continue;
            }
            self.register_collection(name);
        }

        // Register strict document collections.
        let strict = match self.strict.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        let strict_names: Vec<String> = strict
            .collection_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        drop(strict);

        for name in &strict_names {
            self.register_strict_collection(name);
        }

        // Register columnar collections.
        let columnar = match self.columnar.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        let columnar_names: Vec<String> = columnar
            .collection_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        drop(columnar);

        for name in &columnar_names {
            self.register_columnar_collection(name);
        }
    }

    /// Apply HTAP routing: for analytical queries, re-register source strict
    /// tables to point at their materialized columnar views.
    ///
    /// Only applies when the HtapBridge has registered views. The source table
    /// name is re-registered to point at the columnar view, so DataFusion
    /// transparently reads from the faster columnar format.
    fn apply_htap_routing(&self, sql: &str) {
        use crate::engine::htap::routing::is_analytical_query;

        if !is_analytical_query(sql) {
            return;
        }

        let htap = match self.htap.lock() {
            Ok(h) => h,
            Err(p) => p.into_inner(),
        };

        if htap.is_empty() {
            return;
        }

        // For each materialized view, re-register the source table name to
        // point at the columnar view's table provider.
        for target_name in htap.all_targets() {
            if let Some(view) = htap.view_by_target(target_name) {
                let source = view.source.clone();
                let target = view.target.clone();
                drop(htap); // Release lock before registering.

                // Re-register the SOURCE name to point at the COLUMNAR target.
                // This makes `SELECT ... FROM customers GROUP BY ...` read from
                // the columnar materialized view instead of the strict B-tree.
                self.register_columnar_collection_as(&target, &source);
                return; // Only one routing redirect per query.
            }
        }
    }

    /// Register a columnar collection under a different table name.
    ///
    /// Used by HTAP routing to make a source table name point at its
    /// materialized columnar view.
    fn register_columnar_collection_as(&self, collection: &str, table_name: &str) {
        let columnar = match self.columnar.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        let Some(schema) = columnar.schema(collection) else {
            return;
        };
        let schema = schema.clone();
        drop(columnar);

        let provider = ColumnarTableProvider::new(
            collection.to_string(),
            &schema,
            Arc::clone(&self.storage),
            Vec::new(),
            Vec::new(),
        );
        let _ = self.ctx.register_table(table_name, Arc::new(provider));
    }

    /// Register a columnar collection as a queryable table.
    pub fn register_columnar_collection(&self, name: &str) {
        let columnar = match self.columnar.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        let Some(schema) = columnar.schema(name) else {
            return;
        };
        let schema = schema.clone();

        // Collect segment IDs and delete bitmaps.
        drop(columnar);

        let provider = ColumnarTableProvider::new(
            name.to_string(),
            &schema,
            Arc::clone(&self.storage),
            Vec::new(),
            Vec::new(),
        );
        let _ = self.ctx.register_table(name, Arc::new(provider));
    }

    /// Execute a SQL query and return results.
    ///
    /// DDL statements (CREATE/DROP COLLECTION) are intercepted and handled
    /// directly. All other statements are passed to DataFusion.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult, LiteError> {
        // Intercept DDL before DataFusion.
        if let Some(result) = self.try_handle_ddl(sql).await {
            return result;
        }

        // Auto-register collections mentioned in the query.
        self.register_all_collections();

        // HTAP routing: for analytical queries, re-register source tables to point
        // at their materialized columnar views (if any exist and session allows it).
        self.apply_htap_routing(sql);

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| LiteError::Query(format!("SQL parse/plan: {e}")))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| LiteError::Query(format!("SQL execute: {e}")))?;

        // Convert Arrow RecordBatches to QueryResult.
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for batch in &batches {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
            }

            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(columns.len());
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let value = arrow_value_at(col, row_idx)?;
                    row.push(value);
                }
                rows.push(row);
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    }
}
