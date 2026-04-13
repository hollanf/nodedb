//! Implements `nodedb_sql::SqlCatalog` for Origin using CredentialStore.
//!
//! Two-tier resolution:
//!
//! 1. **Replicated metadata cache** (written by the
//!    `MetadataCommitApplier` from committed raft entries). When a
//!    node proposes a DDL, every other node sees the result here.
//! 2. **Local `SystemCatalog` redb** (legacy single-node path). Still
//!    populated by the current pgwire DDL handlers until batch 1c
//!    finishes the migration.
//!
//! The planner asks `get_collection` during every query. The cache
//! hits the path for cross-node DDL visibility; the redb fallback
//! keeps the single-node flow and all enforcement metadata
//! (append_only / balanced / retention / ...) working unchanged
//! during the migration.

use std::sync::{Arc, RwLock};

use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, SqlCatalog, SqlDataType};

use crate::control::security::credential::CredentialStore;

/// Adapter bridging the NodeDB catalog to the `SqlCatalog` trait.
pub struct OriginCatalog {
    credentials: Arc<CredentialStore>,
    tenant_id: u32,
    retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
    /// Replicated metadata cache — the first tier of catalog reads.
    /// Optional so unit tests that construct an `OriginCatalog`
    /// without a full `SharedState` still compile.
    metadata_cache: Option<Arc<RwLock<nodedb_cluster::MetadataCache>>>,
}

impl OriginCatalog {
    pub fn new(
        credentials: Arc<CredentialStore>,
        tenant_id: u32,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials,
            tenant_id,
            retention_policy_registry,
            metadata_cache: None,
        }
    }

    /// Install a replicated metadata cache handle. Called from
    /// `QueryContext::for_state` so every per-request catalog sees
    /// the same cache the `MetadataCommitApplier` writes into.
    pub fn with_metadata_cache(
        mut self,
        cache: Arc<RwLock<nodedb_cluster::MetadataCache>>,
    ) -> Self {
        self.metadata_cache = Some(cache);
        self
    }

    /// Read from the replicated metadata cache. Returns `None` if
    /// the cache is not installed or the collection is not present.
    fn lookup_cached(&self, name: &str) -> Option<CollectionInfo> {
        let cache = self.metadata_cache.as_ref()?;
        let id = nodedb_cluster::DescriptorId::new(
            self.tenant_id,
            nodedb_cluster::DescriptorKind::Collection,
            name,
        );
        let guard = cache.read().unwrap_or_else(|p| p.into_inner());
        let desc = guard.collection(&id)?;
        if !desc.header.state.is_public() {
            return None;
        }
        // Map the replicated descriptor back to the planner's
        // `CollectionInfo`. The descriptor encodes the collection
        // type as a canonical string; we parse it into the
        // corresponding `EngineType`.
        let engine = engine_type_from_str(&desc.collection_type);
        let columns: Vec<ColumnInfo> = desc
            .columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                data_type: parse_type_str(&c.data_type),
                nullable: c.nullable,
                is_primary_key: desc.primary_key.as_deref() == Some(c.name.as_str()),
                default: c.default.clone(),
            })
            .collect();
        Some(CollectionInfo {
            name: name.to_string(),
            engine,
            columns,
            primary_key: desc.primary_key.clone(),
            has_auto_tier: self.has_auto_tier(name),
        })
    }

    fn has_auto_tier(&self, collection: &str) -> bool {
        let registry = match &self.retention_policy_registry {
            Some(r) => r,
            None => return false,
        };
        registry
            .get(self.tenant_id, collection)
            .is_some_and(|p| p.auto_tier)
    }
}

impl SqlCatalog for OriginCatalog {
    fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        // Tier 1: replicated metadata cache (cross-node DDL
        // visibility). Hit when a CREATE COLLECTION committed on
        // another node has been applied locally by the
        // MetadataCommitApplier.
        if let Some(info) = self.lookup_cached(name) {
            return Some(info);
        }

        // Tier 2: legacy local SystemCatalog redb. Populated today
        // by the current pgwire DDL handlers; batch 1c flips them to
        // propose-through-raft and this fallback shrinks to "data
        // not yet replicated to this node" + "enforcement metadata
        // the replicated descriptor does not yet carry".
        let catalog = self.credentials.catalog().as_ref()?;
        let stored = catalog.get_collection(self.tenant_id, name).ok()??;
        if !stored.is_active {
            return None;
        }

        let (engine, columns, primary_key) = convert_collection_type(&stored);

        Some(CollectionInfo {
            name: stored.name,
            engine,
            columns,
            primary_key,
            has_auto_tier: self.has_auto_tier(name),
        })
    }
}

/// Convert a StoredCollection to engine type, columns, and primary key.
fn convert_collection_type(
    stored: &crate::control::security::catalog::StoredCollection,
) -> (EngineType, Vec<ColumnInfo>, Option<String>) {
    use nodedb_types::CollectionType;
    use nodedb_types::columnar::DocumentMode;

    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            let columns = schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                })
                .collect();
            let pk = schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone());
            (EngineType::DocumentStrict, columns, pk)
        }

        CollectionType::Document(DocumentMode::Schemaless) => {
            let mut columns = vec![ColumnInfo {
                name: "id".into(),
                data_type: SqlDataType::String,
                nullable: false,
                is_primary_key: true,
                default: None,
            }];
            // Add tracked fields from catalog.
            for (name, type_str) in &stored.fields {
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                });
            }
            (EngineType::DocumentSchemaless, columns, Some("id".into()))
        }

        CollectionType::KeyValue(config) => {
            let columns = config
                .schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                })
                .collect();
            let pk = config
                .schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone())
                .or_else(|| Some("key".into()));
            (EngineType::KeyValue, columns, pk)
        }

        CollectionType::Columnar(profile) => {
            let engine = if profile.is_timeseries() {
                EngineType::Timeseries
            } else if profile.is_spatial() {
                EngineType::Spatial
            } else {
                EngineType::Columnar
            };
            let mut columns = Vec::new();
            if !profile.is_timeseries() {
                columns.push(ColumnInfo {
                    name: "id".into(),
                    data_type: SqlDataType::String,
                    nullable: false,
                    is_primary_key: true,
                    default: Some("UUID_V7".into()),
                });
            }
            for (name, type_str) in &stored.fields {
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                });
            }
            let pk = if profile.is_timeseries() {
                None
            } else {
                Some("id".into())
            };
            (engine, columns, pk)
        }
    }
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes | ColumnType::Geometry | ColumnType::Json => SqlDataType::Bytes,
        ColumnType::Timestamp => SqlDataType::Timestamp,
        ColumnType::Decimal | ColumnType::Uuid | ColumnType::Ulid | ColumnType::Regex => {
            SqlDataType::String
        }
        ColumnType::Duration => SqlDataType::Int64,
        ColumnType::Array | ColumnType::Set | ColumnType::Range | ColumnType::Record => {
            SqlDataType::Bytes
        }
        ColumnType::Vector(dim) => SqlDataType::Vector(*dim as usize),
    }
}

fn parse_type_str(s: &str) -> SqlDataType {
    match s.to_uppercase().as_str() {
        "INT" | "INTEGER" | "INT4" | "INT8" | "BIGINT" => SqlDataType::Int64,
        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "DOUBLE" | "REAL" => SqlDataType::Float64,
        "BOOL" | "BOOLEAN" => SqlDataType::Bool,
        "BYTES" | "BYTEA" | "BLOB" => SqlDataType::Bytes,
        "TIMESTAMP" | "TIMESTAMPTZ" => SqlDataType::Timestamp,
        _ => SqlDataType::String,
    }
}

/// Map the canonical string form of a collection type (stored in the
/// replicated [`nodedb_cluster::CollectionDescriptor`]) back to the
/// planner's [`EngineType`]. Unknown / future variants fall through to
/// `DocumentSchemaless` — the planner's safest default.
fn engine_type_from_str(s: &str) -> EngineType {
    match s {
        "document_strict" => EngineType::DocumentStrict,
        "document_schemaless" => EngineType::DocumentSchemaless,
        "key_value" | "kv" => EngineType::KeyValue,
        "columnar" | "columnar_plain" => EngineType::Columnar,
        "timeseries" => EngineType::Timeseries,
        "spatial" => EngineType::Spatial,
        _ => EngineType::DocumentSchemaless,
    }
}
