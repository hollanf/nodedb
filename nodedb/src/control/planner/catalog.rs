//! Custom DataFusion catalog provider for NodeDB collections.
//!
//! Makes NodeDB collections visible to DataFusion's SQL planner so that
//! `SELECT * FROM orders` resolves correctly instead of returning
//! "table not found". The actual execution still goes through our
//! `PlanConverter` — this provider only supplies schema metadata.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{SchemaProvider, Session, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::control::security::credential::CredentialStore;

/// A DataFusion `SchemaProvider` backed by NodeDB's system catalog.
///
/// Returns `NodeDbTableStub` instances for each collection. These stubs
/// provide schema information to DataFusion for planning but do not
/// execute — the `PlanConverter` handles physical execution.
pub struct NodeDbSchemaProvider {
    credentials: Arc<CredentialStore>,
    tenant_id: u32,
    /// Stream registry for resolving change stream names as virtual tables.
    stream_registry: Arc<crate::event::cdc::StreamRegistry>,
    /// CDC router for accessing stream buffers.
    cdc_router: Arc<crate::event::cdc::CdcRouter>,
    /// Streaming MV registry for resolving MV names as virtual tables.
    mv_registry: Arc<crate::event::streaming_mv::MvRegistry>,
}

impl std::fmt::Debug for NodeDbSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDbSchemaProvider")
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl NodeDbSchemaProvider {
    pub fn new(
        credentials: Arc<CredentialStore>,
        tenant_id: u32,
        stream_registry: Arc<crate::event::cdc::StreamRegistry>,
        cdc_router: Arc<crate::event::cdc::CdcRouter>,
        mv_registry: Arc<crate::event::streaming_mv::MvRegistry>,
    ) -> Self {
        Self {
            credentials,
            tenant_id,
            stream_registry,
            cdc_router,
            mv_registry,
        }
    }

    fn load_collections(&self) -> Vec<crate::control::security::catalog::StoredCollection> {
        let catalog = self.credentials.catalog();
        match catalog {
            Some(c) => c
                .load_collections_for_tenant(self.tenant_id)
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }
}

#[async_trait]
impl SchemaProvider for NodeDbSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .load_collections()
            .into_iter()
            .map(|c| c.name)
            .collect();
        // Include change stream names as virtual tables.
        for stream in self.stream_registry.list_for_tenant(self.tenant_id) {
            names.push(stream.name);
        }
        // Include streaming MV names as virtual tables.
        for mv in self.mv_registry.list_for_tenant(self.tenant_id) {
            names.push(mv.name);
        }
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        let name = name.to_lowercase();
        let name = name.as_str();
        // Check streaming MVs.
        if self.mv_registry.get_def(self.tenant_id, name).is_some() {
            return true;
        }
        // Check change streams.
        if self.stream_registry.get(self.tenant_id, name).is_some() {
            return true;
        }
        // Then check collections.
        let catalog = self.credentials.catalog();
        match catalog {
            Some(c) => c
                .get_collection(self.tenant_id, name)
                .ok()
                .flatten()
                .is_some_and(|c| c.is_active),
            None => false,
        }
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let name = name.to_lowercase();
        let name = name.as_str();

        // Check if this is a streaming MV — return a MemTable with aggregate results.
        if let Some(mv_state) = self.mv_registry.get_state(self.tenant_id, name) {
            let schema = crate::event::streaming_mv::query::mv_result_schema(&mv_state);
            let batches =
                match crate::event::streaming_mv::query::mv_state_to_record_batch(&mv_state) {
                    Some(batch) => vec![vec![batch]],
                    None => vec![vec![]],
                };
            let mem_table = datafusion::datasource::MemTable::try_new(schema, batches)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Ok(Some(Arc::new(mem_table)));
        }

        // Check if this is a change stream — return a MemTable backed by buffer events.
        if self.stream_registry.get(self.tenant_id, name).is_some()
            && let Some(buffer) = self.cdc_router.get_buffer(self.tenant_id, name)
        {
            let schema = super::stream_table::stream_event_schema();
            let events = buffer.read_from_lsn(0, usize::MAX);
            let batches = match super::stream_table::events_to_record_batch(&events) {
                Some(batch) => vec![vec![batch]],
                None => vec![vec![]],
            };
            let mem_table = datafusion::datasource::MemTable::try_new(schema, batches)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Ok(Some(Arc::new(mem_table)));
        }

        // Fall through to collection lookup.
        let catalog = self.credentials.catalog();
        let coll = match catalog {
            Some(c) => c
                .get_collection(self.tenant_id, name)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            None => return Ok(None),
        };

        match coll {
            Some(stored) if stored.is_active => {
                let schema = collection_to_arrow_schema(&stored);
                Ok(Some(Arc::new(NodeDbTableStub {
                    name: stored.name,
                    schema,
                })))
            }
            _ => Ok(None),
        }
    }
}

/// A stub `TableProvider` that gives DataFusion schema info for planning.
///
/// Does not scan data — the `PlanConverter` extracts the collection name
/// from the `TableScan` node and dispatches to the Data Plane.
#[derive(Debug)]
struct NodeDbTableStub {
    name: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for NodeDbTableStub {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // This should never be called — the PlanConverter intercepts
        // TableScan nodes before DataFusion tries to execute them.
        Err(DataFusionError::NotImplemented(format!(
            "NodeDB collection '{}' is executed via the Data Plane, not DataFusion scan",
            self.name
        )))
    }
}

/// Convert a `ColumnType` to the equivalent Arrow `DataType`.
fn column_type_to_arrow(ct: &nodedb_types::columnar::ColumnType) -> DataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => DataType::Int64,
        ColumnType::Float64 => DataType::Float64,
        ColumnType::String => DataType::Utf8,
        ColumnType::Bool => DataType::Boolean,
        ColumnType::Bytes | ColumnType::Geometry => DataType::Binary,
        ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::Decimal | ColumnType::Uuid => DataType::Utf8,
        ColumnType::Vector(_) => DataType::Binary,
    }
}

/// Convert a `StoredCollection` to an Arrow schema for DataFusion query planning.
///
/// Strict and KV collections carry their full schema in `collection_type`.
/// Columnar collections (plain / timeseries / spatial) store column info in
/// the legacy `fields` tuple vec populated at CREATE time.
/// Schemaless documents expose `(id, document)` — fields are dynamic.
fn collection_to_arrow_schema(
    coll: &crate::control::security::catalog::StoredCollection,
) -> SchemaRef {
    use nodedb_types::CollectionType;
    use nodedb_types::columnar::DocumentMode;

    match &coll.collection_type {
        // Strict document: full schema lives in the DocumentMode variant.
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            let fields = schema
                .columns
                .iter()
                .map(|c| Field::new(&c.name, column_type_to_arrow(&c.column_type), c.nullable))
                .collect::<Vec<_>>();
            Arc::new(Schema::new(fields))
        }

        // Schemaless document: expose tracked fields if any, else id + document.
        CollectionType::Document(DocumentMode::Schemaless) => {
            if coll.fields.is_empty() {
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("document", DataType::Utf8, true),
                ]))
            } else {
                let mut fields = vec![Field::new("id", DataType::Utf8, false)];
                for (name, type_str) in &coll.fields {
                    let dt = match type_str.to_uppercase().as_str() {
                        "INT" | "INTEGER" | "INT4" | "INT8" | "BIGINT" => DataType::Int64,
                        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "DOUBLE" | "REAL" => {
                            DataType::Float64
                        }
                        "BOOL" | "BOOLEAN" => DataType::Boolean,
                        _ => DataType::Utf8,
                    };
                    fields.push(Field::new(name, dt, true));
                }
                // Keep document blob for raw access.
                fields.push(Field::new("document", DataType::Utf8, true));
                Arc::new(Schema::new(fields))
            }
        }

        // Key-Value: declared columns + implicit nullable `value TEXT` catch-all.
        // Declared columns cover the key (PK) and any typed value fields.
        // The implicit `value` column lets callers store arbitrary blobs without
        // declaring every field in the schema — that's the whole point of KV.
        CollectionType::KeyValue(config) => {
            let mut fields: Vec<Field> = config
                .schema
                .columns
                .iter()
                .map(|c| Field::new(&c.name, column_type_to_arrow(&c.column_type), c.nullable))
                .collect();
            // Add implicit `value` field only when no non-PK typed fields are declared.
            // If the user declared typed value fields (e.g. counter INT, label TEXT),
            // they manage the schema explicitly — no implicit field needed.
            let has_value = config.schema.columns.iter().any(|c| c.name == "value");
            let has_typed_value_fields = config.schema.columns.iter().any(|c| !c.primary_key);
            if !has_value && !has_typed_value_fields {
                fields.push(Field::new("value", DataType::Utf8, true));
            }
            Arc::new(Schema::new(fields))
        }

        // Columnar (plain / timeseries / spatial): schema stored as (name, type_str)
        // tuples in `coll.fields`, populated at CREATE time.
        // Fall back to (id, document) for collections created before typed DDL.
        CollectionType::Columnar(profile) => {
            if coll.fields.is_empty() {
                return Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("document", DataType::Utf8, true),
                ]));
            }

            let is_timeseries = profile.is_timeseries();
            // Don't prepend `id` if the user already declared it as a column.
            let has_id = coll.fields.iter().any(|(name, _)| name == "id");
            let mut fields = if is_timeseries || has_id {
                Vec::new()
            } else {
                vec![Field::new("id", DataType::Utf8, false)]
            };

            for (name, type_str) in &coll.fields {
                let dt = match type_str.to_uppercase().as_str() {
                    "INT" | "INTEGER" | "INT4" | "INT8" | "BIGINT" => DataType::Int64,
                    "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "DOUBLE" | "REAL" => {
                        DataType::Float64
                    }
                    "VARCHAR" | "TEXT" | "STRING" => DataType::Utf8,
                    "BOOL" | "BOOLEAN" => DataType::Boolean,
                    "BYTES" | "BYTEA" | "BLOB" => DataType::Binary,
                    "JSON" | "JSONB" => DataType::Utf8,
                    "TIMESTAMP" | "TIMESTAMPTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
                    t if t.starts_with("VECTOR") => DataType::Binary,
                    _ => DataType::Utf8,
                };
                fields.push(Field::new(name, dt, true));
            }
            Arc::new(Schema::new(fields))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    fn base_coll(name: &str, ct: nodedb_types::CollectionType) -> StoredCollection {
        StoredCollection {
            tenant_id: 1,
            name: name.into(),
            owner: "admin".into(),
            created_at: 0,
            fields: vec![],
            field_defs: vec![],
            event_defs: vec![],
            collection_type: ct,
            timeseries_config: None,
            is_active: true,
            append_only: false,
            hash_chain: false,
            balanced: None,
            last_chain_hash: None,
            period_lock: None,
            retention_period: None,
            legal_holds: Vec::new(),
            state_constraints: Vec::new(),
            transition_checks: Vec::new(),
            materialized_sums: Vec::new(),
            lvc_enabled: false,
            permission_tree_def: None,
        }
    }

    #[test]
    fn schemaless_collection_schema() {
        let coll = base_coll("test", nodedb_types::CollectionType::document());
        let schema = collection_to_arrow_schema(&coll);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "document");
    }

    #[test]
    fn strict_collection_schema() {
        let strict_schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Uuid).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("amount", ColumnType::Decimal),
            ColumnDef::required("created_at", ColumnType::Timestamp),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .unwrap();
        let coll = base_coll(
            "orders",
            nodedb_types::CollectionType::strict(strict_schema),
        );
        let schema = collection_to_arrow_schema(&coll);

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8); // UUID → Utf8
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(schema.field(2).name(), "amount");
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8); // Decimal → Utf8
        assert_eq!(schema.field(3).name(), "created_at");
        assert_eq!(
            *schema.field(3).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(schema.field(4).name(), "score");
        assert_eq!(*schema.field(4).data_type(), DataType::Float64);
        assert!(schema.field(4).is_nullable());
    }

    #[test]
    fn kv_collection_schema() {
        let kv_schema = StrictSchema::new(vec![
            ColumnDef::required("key", ColumnType::String).with_primary_key(),
            ColumnDef::required("user_id", ColumnType::Uuid),
            ColumnDef::nullable("expires_at", ColumnType::Timestamp),
        ])
        .unwrap();
        let coll = base_coll("sessions", nodedb_types::CollectionType::kv(kv_schema));
        let schema = collection_to_arrow_schema(&coll);

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "key");
        assert!(!schema.field(0).is_nullable()); // primary key → NOT NULL
        assert_eq!(schema.field(1).name(), "user_id");
        assert_eq!(schema.field(2).name(), "expires_at");
        assert!(schema.field(2).is_nullable());
    }

    #[test]
    fn columnar_collection_schema() {
        let mut coll = base_coll("web_events", nodedb_types::CollectionType::columnar());
        coll.fields = vec![
            ("ts".into(), "TIMESTAMP".into()),
            ("user_id".into(), "UUID".into()),
            ("page".into(), "VARCHAR".into()),
            ("duration_ms".into(), "INT".into()),
        ];
        let schema = collection_to_arrow_schema(&coll);

        // Plain columnar gets an implicit id column prepended.
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "ts");
        assert_eq!(
            *schema.field(1).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(*schema.field(4).data_type(), DataType::Int64);
    }

    #[test]
    fn timeseries_collection_schema() {
        let mut coll = base_coll(
            "cpu_metrics",
            nodedb_types::CollectionType::timeseries("ts", "1h"),
        );
        coll.fields = vec![
            ("ts".into(), "TIMESTAMP".into()),
            ("host".into(), "VARCHAR".into()),
            ("cpu".into(), "FLOAT64".into()),
        ];
        let schema = collection_to_arrow_schema(&coll);

        // Timeseries does NOT prepend an id column.
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "ts");
        assert_eq!(
            *schema.field(0).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(*schema.field(2).data_type(), DataType::Float64);
    }

    #[test]
    fn columnar_empty_fields_falls_back_to_schemaless() {
        // Collections created before typed DDL may have empty fields.
        let coll = base_coll("legacy", nodedb_types::CollectionType::columnar());
        let schema = collection_to_arrow_schema(&coll);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "document");
    }
}
