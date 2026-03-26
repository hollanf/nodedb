//! Strict document engine: Binary Tuple CRUD against the StorageEngine.
//!
//! Each strict collection stores rows as Binary Tuples in the `Strict`
//! namespace, keyed by `{collection}:{pk_bytes}`. The schema is stored
//! in the `Meta` namespace as `strict_schema:{collection}`.
//!
//! Uses the `nodedb-strict` crate for encoding/decoding and Arrow extraction.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_strict::arrow_extract::extract_column_to_arrow;
use nodedb_strict::{StrictError, TupleDecoder, TupleEncoder};
use nodedb_types::Namespace;
use nodedb_types::columnar::{ColumnType, SchemaOps, StrictSchema};
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

/// Meta key prefix for strict schemas in the Meta namespace.
const META_STRICT_SCHEMA_PREFIX: &str = "strict_schema:";

/// Meta key listing all strict collections.
const META_STRICT_COLLECTIONS: &[u8] = b"meta:strict_collections";

/// Manages all strict document collections for a NodeDbLite instance.
///
/// Holds per-collection schemas, encoders, and decoders. Delegates storage
/// to the shared `StorageEngine` under `Namespace::Strict`.
pub struct StrictEngine<S: StorageEngine> {
    storage: Arc<S>,
    /// Per-collection schema + encoder + decoder.
    collections: HashMap<String, CollectionState>,
}

struct CollectionState {
    schema: StrictSchema,
    encoder: TupleEncoder,
    decoder: TupleDecoder,
    /// Column indices of PK columns (for key construction).
    pk_indices: Vec<usize>,
}

impl CollectionState {
    fn new(schema: StrictSchema) -> Self {
        let pk_indices: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);
        Self {
            schema,
            encoder,
            decoder,
            pk_indices,
        }
    }

    /// Build a storage key from PK values: `{collection}:{pk_bytes}`.
    fn storage_key(&self, collection: &str, values: &[Value]) -> Vec<u8> {
        let mut key = collection.as_bytes().to_vec();
        key.push(b':');
        for &pk_idx in &self.pk_indices {
            encode_pk_component(&mut key, &values[pk_idx]);
        }
        key
    }

    /// Build a storage key from a single PK value (common case).
    fn storage_key_from_pk(&self, collection: &str, pk: &Value) -> Vec<u8> {
        let mut key = collection.as_bytes().to_vec();
        key.push(b':');
        encode_pk_component(&mut key, pk);
        key
    }
}

/// Encode a PK value into sortable bytes for the storage key.
///
/// Uses big-endian for integers (preserves sort order in redb scans),
/// raw UTF-8 for strings, and raw bytes for UUIDs.
fn encode_pk_component(key: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Integer(v) => {
            // XOR with sign bit to make signed integers sort correctly as unsigned bytes.
            let sortable = (*v as u64) ^ (1u64 << 63);
            key.extend_from_slice(&sortable.to_be_bytes());
        }
        Value::String(s) => {
            key.extend_from_slice(s.as_bytes());
        }
        Value::Uuid(s) => {
            // Store UUID as raw UTF-8 bytes for sortable key. UUID strings
            // are already lexicographically sortable in hyphenated form.
            key.extend_from_slice(s.as_bytes());
        }
        Value::Decimal(d) => {
            key.extend_from_slice(&d.serialize());
        }
        _ => {
            // Fallback: use debug representation. Should not happen for valid PK types.
            key.extend_from_slice(format!("{value:?}").as_bytes());
        }
    }
}

impl<S: StorageEngine> StrictEngine<S> {
    /// Create a new empty strict engine.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            collections: HashMap::new(),
        }
    }

    /// Restore strict collections from storage on startup.
    pub async fn restore(storage: Arc<S>) -> Result<Self, LiteError> {
        let mut engine = Self::new(Arc::clone(&storage));

        // Load collection list from meta.
        let list_bytes = storage
            .get(Namespace::Meta, META_STRICT_COLLECTIONS)
            .await?;
        let names: Vec<String> = match list_bytes {
            Some(bytes) => rmp_serde::from_slice(&bytes).map_err(|e| LiteError::Storage {
                detail: format!("strict collection list deserialization failed: {e}"),
            })?,
            None => Vec::new(),
        };

        // Load each schema.
        for name in names {
            let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
            if let Some(schema_bytes) = storage.get(Namespace::Meta, meta_key.as_bytes()).await?
                && let Ok(schema) = rmp_serde::from_slice::<StrictSchema>(&schema_bytes)
            {
                engine
                    .collections
                    .insert(name, CollectionState::new(schema));
            }
        }

        Ok(engine)
    }

    // -- Schema management --

    /// Register a new strict collection with the given schema.
    pub async fn create_collection(
        &mut self,
        name: &str,
        schema: StrictSchema,
    ) -> Result<(), LiteError> {
        if self.collections.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!("strict collection '{name}' already exists"),
            });
        }

        // Persist schema to meta.
        let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
        let schema_bytes =
            rmp_serde::to_vec_named(&schema).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;

        // Update collection list.
        let mut names: Vec<String> = self.collections.keys().cloned().collect();
        names.push(name.to_string());
        let names_bytes =
            rmp_serde::to_vec_named(&names).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;

        self.storage
            .batch_write(&[
                WriteOp::Put {
                    ns: Namespace::Meta,
                    key: meta_key.into_bytes(),
                    value: schema_bytes,
                },
                WriteOp::Put {
                    ns: Namespace::Meta,
                    key: META_STRICT_COLLECTIONS.to_vec(),
                    value: names_bytes,
                },
            ])
            .await?;

        self.collections
            .insert(name.to_string(), CollectionState::new(schema));
        Ok(())
    }

    /// Drop a strict collection and all its data.
    pub async fn drop_collection(&mut self, name: &str) -> Result<(), LiteError> {
        if !self.collections.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!("strict collection '{name}' does not exist"),
            });
        }

        // Scan and delete all rows.
        let prefix = format!("{name}:");
        let rows = self
            .storage
            .scan_prefix(Namespace::Strict, prefix.as_bytes())
            .await?;
        let mut ops: Vec<WriteOp> = rows
            .iter()
            .map(|(k, _)| WriteOp::Delete {
                ns: Namespace::Strict,
                key: k.clone(),
            })
            .collect();

        // Delete schema meta.
        let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
        ops.push(WriteOp::Delete {
            ns: Namespace::Meta,
            key: meta_key.into_bytes(),
        });

        // Update collection list.
        self.collections.remove(name);
        let names: Vec<String> = self.collections.keys().cloned().collect();
        let names_bytes =
            rmp_serde::to_vec_named(&names).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;
        ops.push(WriteOp::Put {
            ns: Namespace::Meta,
            key: META_STRICT_COLLECTIONS.to_vec(),
            value: names_bytes,
        });

        self.storage.batch_write(&ops).await?;
        Ok(())
    }

    /// Get the schema for a collection.
    pub fn schema(&self, name: &str) -> Option<&StrictSchema> {
        self.collections.get(name).map(|c| &c.schema)
    }

    /// List all strict collection names.
    pub fn collection_names(&self) -> Vec<&str> {
        self.collections.keys().map(|s| s.as_str()).collect()
    }

    // -- Write path --

    /// Insert a row into a strict collection.
    ///
    /// Validates schema, encodes as Binary Tuple, writes to storage keyed by PK.
    /// Returns an error if the PK already exists.
    pub async fn insert(&self, collection: &str, values: &[Value]) -> Result<(), LiteError> {
        let state = self.get_state(collection)?;

        // Encode the row.
        let tuple = state.encoder.encode(values).map_err(strict_err_to_lite)?;

        // Build storage key from PK values.
        let key = state.storage_key(collection, values);

        // Check for duplicate PK.
        if self.storage.get(Namespace::Strict, &key).await?.is_some() {
            return Err(LiteError::BadRequest {
                detail: format!("duplicate primary key in collection '{collection}'"),
            });
        }

        self.storage.put(Namespace::Strict, &key, &tuple).await
    }

    /// Insert multiple rows atomically.
    pub async fn insert_batch(
        &self,
        collection: &str,
        rows: &[Vec<Value>],
    ) -> Result<(), LiteError> {
        let state = self.get_state(collection)?;

        let mut ops = Vec::with_capacity(rows.len());
        for values in rows {
            let tuple = state.encoder.encode(values).map_err(strict_err_to_lite)?;
            let key = state.storage_key(collection, values);
            ops.push(WriteOp::Put {
                ns: Namespace::Strict,
                key,
                value: tuple,
            });
        }

        self.storage.batch_write(&ops).await
    }

    /// Update a row by PK. Reads the existing tuple, patches the specified
    /// fields, and writes the modified tuple back.
    ///
    /// `updates` maps column names to new values. Columns not in the map
    /// retain their existing values.
    pub async fn update(
        &self,
        collection: &str,
        pk: &Value,
        updates: &HashMap<String, Value>,
    ) -> Result<bool, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        // Read existing tuple.
        let existing = match self.storage.get(Namespace::Strict, &key).await? {
            Some(bytes) => bytes,
            None => return Ok(false),
        };

        // Extract all current values.
        let mut values = state
            .decoder
            .extract_all(&existing)
            .map_err(strict_err_to_lite)?;

        // Apply updates.
        for (col_name, new_value) in updates {
            let col_idx =
                state
                    .schema
                    .column_index(col_name)
                    .ok_or_else(|| LiteError::BadRequest {
                        detail: format!("unknown column '{col_name}' in collection '{collection}'"),
                    })?;

            // Validate the new value against the column type.
            if !matches!(new_value, Value::Null)
                && !state.schema.columns[col_idx].column_type.accepts(new_value)
            {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "column '{}': type mismatch",
                        state.schema.columns[col_idx].name
                    ),
                });
            }

            values[col_idx] = new_value.clone();
        }

        // Re-encode and write.
        let new_tuple = state.encoder.encode(&values).map_err(strict_err_to_lite)?;

        // If PK columns were updated, we need to delete the old key and insert the new one.
        let new_key = state.storage_key(collection, &values);
        if new_key != key {
            self.storage
                .batch_write(&[
                    WriteOp::Delete {
                        ns: Namespace::Strict,
                        key,
                    },
                    WriteOp::Put {
                        ns: Namespace::Strict,
                        key: new_key,
                        value: new_tuple,
                    },
                ])
                .await?;
        } else {
            self.storage
                .put(Namespace::Strict, &key, &new_tuple)
                .await?;
        }

        Ok(true)
    }

    /// Delete a row by PK. Returns true if the row existed.
    pub async fn delete(&self, collection: &str, pk: &Value) -> Result<bool, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        let existed = self.storage.get(Namespace::Strict, &key).await?.is_some();
        if existed {
            self.storage.delete(Namespace::Strict, &key).await?;
        }
        Ok(existed)
    }

    // -- Read path --

    /// Point lookup by PK. Returns the row as a Vec<Value>, or None.
    pub async fn get(&self, collection: &str, pk: &Value) -> Result<Option<Vec<Value>>, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        match self.storage.get(Namespace::Strict, &key).await? {
            Some(bytes) => {
                let values = state
                    .decoder
                    .extract_all(&bytes)
                    .map_err(strict_err_to_lite)?;
                Ok(Some(values))
            }
            None => Ok(None),
        }
    }

    /// Point lookup with column projection. Only decodes the requested columns.
    pub async fn get_projected(
        &self,
        collection: &str,
        pk: &Value,
        columns: &[&str],
    ) -> Result<Option<Vec<Value>>, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        match self.storage.get(Namespace::Strict, &key).await? {
            Some(bytes) => {
                let mut values = Vec::with_capacity(columns.len());
                for col_name in columns {
                    let val = state
                        .decoder
                        .extract_by_name(&bytes, col_name)
                        .map_err(strict_err_to_lite)?;
                    values.push(val);
                }
                Ok(Some(values))
            }
            None => Ok(None),
        }
    }

    /// Scan all rows in a collection. Returns raw tuple bytes for Arrow extraction.
    pub async fn scan_raw(&self, collection: &str) -> Result<Vec<Vec<u8>>, LiteError> {
        let _state = self.get_state(collection)?;
        let prefix = format!("{collection}:");
        let entries = self
            .storage
            .scan_prefix(Namespace::Strict, prefix.as_bytes())
            .await?;
        Ok(entries.into_iter().map(|(_, v)| v).collect())
    }

    /// Scan all rows and extract a single column into an Arrow array.
    pub async fn scan_column_to_arrow(
        &self,
        collection: &str,
        col_idx: usize,
    ) -> Result<datafusion::arrow::array::ArrayRef, LiteError> {
        let state = self.get_state(collection)?;
        let tuples = self.scan_raw(collection).await?;
        let refs: Vec<&[u8]> = tuples.iter().map(|t| t.as_slice()).collect();

        extract_column_to_arrow(&state.schema, &state.decoder, &refs, col_idx)
            .map_err(strict_err_to_lite)
    }

    /// Scan all rows and extract multiple columns into Arrow arrays.
    pub async fn scan_columns_to_arrow(
        &self,
        collection: &str,
        col_indices: &[usize],
    ) -> Result<Vec<datafusion::arrow::array::ArrayRef>, LiteError> {
        let state = self.get_state(collection)?;
        let tuples = self.scan_raw(collection).await?;
        let refs: Vec<&[u8]> = tuples.iter().map(|t| t.as_slice()).collect();

        let mut arrays = Vec::with_capacity(col_indices.len());
        for &idx in col_indices {
            let arr = extract_column_to_arrow(&state.schema, &state.decoder, &refs, idx)
                .map_err(strict_err_to_lite)?;
            arrays.push(arr);
        }
        Ok(arrays)
    }

    /// Count the number of rows in a collection.
    pub async fn count(&self, collection: &str) -> Result<usize, LiteError> {
        let _state = self.get_state(collection)?;
        let prefix = format!("{collection}:");
        let entries = self
            .storage
            .scan_prefix(Namespace::Strict, prefix.as_bytes())
            .await?;
        Ok(entries.len())
    }

    // -- Internal helpers --

    fn get_state(&self, collection: &str) -> Result<&CollectionState, LiteError> {
        self.collections
            .get(collection)
            .ok_or(LiteError::BadRequest {
                detail: format!("strict collection '{collection}' does not exist"),
            })
    }
}

/// Convert a `StrictError` to `LiteError`.
fn strict_err_to_lite(e: StrictError) -> LiteError {
    LiteError::BadRequest {
        detail: e.to_string(),
    }
}

/// Convert an Arrow `DataType` from a `ColumnType` (for schema construction).
pub fn column_type_to_arrow(ct: &ColumnType) -> datafusion::arrow::datatypes::DataType {
    match ct {
        ColumnType::Int64 => datafusion::arrow::datatypes::DataType::Int64,
        ColumnType::Float64 => datafusion::arrow::datatypes::DataType::Float64,
        ColumnType::String => datafusion::arrow::datatypes::DataType::Utf8,
        ColumnType::Bool => datafusion::arrow::datatypes::DataType::Boolean,
        ColumnType::Bytes | ColumnType::Geometry => datafusion::arrow::datatypes::DataType::Binary,
        ColumnType::Timestamp => datafusion::arrow::datatypes::DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            None,
        ),
        ColumnType::Decimal => datafusion::arrow::datatypes::DataType::Utf8, // Lossless string representation
        ColumnType::Uuid => datafusion::arrow::datatypes::DataType::Utf8,
        ColumnType::Vector(_) => datafusion::arrow::datatypes::DataType::Binary, // Packed f32 bytes
    }
}

/// Build an Arrow schema from a StrictSchema (for DataFusion table registration).
pub fn strict_schema_to_arrow(schema: &StrictSchema) -> datafusion::arrow::datatypes::SchemaRef {
    use datafusion::arrow::datatypes::{Field, Schema};
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|col| {
            Field::new(
                &col.name,
                column_type_to_arrow(&col.column_type),
                col.nullable,
            )
        })
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::WriteOp;
    use nodedb_types::columnar::ColumnDef;

    /// In-memory storage for tests.
    struct MemStorage {
        data: tokio::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    }

    impl MemStorage {
        fn new() -> Self {
            Self {
                data: tokio::sync::Mutex::new(HashMap::new()),
            }
        }

        fn make_key(ns: Namespace, key: &[u8]) -> Vec<u8> {
            let mut k = Vec::with_capacity(1 + key.len());
            k.push(ns as u8);
            k.extend_from_slice(key);
            k
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for MemStorage {
        async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
            let data = self.data.lock().await;
            Ok(data.get(&Self::make_key(ns, key)).cloned())
        }

        async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
            let mut data = self.data.lock().await;
            data.insert(Self::make_key(ns, key), value.to_vec());
            Ok(())
        }

        async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
            let mut data = self.data.lock().await;
            data.remove(&Self::make_key(ns, key));
            Ok(())
        }

        async fn scan_prefix(
            &self,
            ns: Namespace,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LiteError> {
            let data = self.data.lock().await;
            let ns_byte = ns as u8;
            let mut full_prefix = vec![ns_byte];
            full_prefix.extend_from_slice(prefix);

            let mut results: Vec<(Vec<u8>, Vec<u8>)> = data
                .iter()
                .filter(|(k, _)| k.starts_with(&full_prefix))
                .map(|(k, v)| (k[1..].to_vec(), v.clone()))
                .collect();
            results.sort_by(|(a, _), (b, _)| a.cmp(b));
            Ok(results)
        }

        async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
            let mut data = self.data.lock().await;
            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        data.insert(Self::make_key(*ns, key), value.clone());
                    }
                    WriteOp::Delete { ns, key } => {
                        data.remove(&Self::make_key(*ns, key));
                    }
                }
            }
            Ok(())
        }

        async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
            let data = self.data.lock().await;
            let ns_byte = ns as u8;
            Ok(data.keys().filter(|k| k[0] == ns_byte).count() as u64)
        }
    }

    fn crm_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("email", ColumnType::String),
            ColumnDef::required("balance", ColumnType::Decimal),
            ColumnDef::nullable("active", ColumnType::Bool),
        ])
        .expect("valid schema")
    }

    #[tokio::test]
    async fn create_collection_and_insert() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));

        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create failed");

        assert!(engine.schema("customers").is_some());
        assert_eq!(engine.collection_names().len(), 1);

        // Insert a row.
        engine
            .insert(
                "customers",
                &[
                    Value::Integer(1),
                    Value::String("Alice".into()),
                    Value::String("alice@co.com".into()),
                    Value::Decimal(rust_decimal::Decimal::new(5000, 2)),
                    Value::Bool(true),
                ],
            )
            .await
            .expect("insert failed");

        // Verify count.
        assert_eq!(engine.count("customers").await.expect("count"), 1);
    }

    #[tokio::test]
    async fn get_by_pk() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        engine
            .insert(
                "customers",
                &[
                    Value::Integer(42),
                    Value::String("Bob".into()),
                    Value::Null,
                    Value::Decimal(rust_decimal::Decimal::ZERO),
                    Value::Null,
                ],
            )
            .await
            .expect("insert");

        let row = engine
            .get("customers", &Value::Integer(42))
            .await
            .expect("get")
            .expect("row should exist");

        assert_eq!(row[0], Value::Integer(42));
        assert_eq!(row[1], Value::String("Bob".into()));
        assert_eq!(row[2], Value::Null);
    }

    #[tokio::test]
    async fn get_projected() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        engine
            .insert(
                "customers",
                &[
                    Value::Integer(1),
                    Value::String("Charlie".into()),
                    Value::String("c@co.com".into()),
                    Value::Decimal(rust_decimal::Decimal::new(100, 0)),
                    Value::Bool(false),
                ],
            )
            .await
            .expect("insert");

        // Only fetch name and balance.
        let row = engine
            .get_projected("customers", &Value::Integer(1), &["name", "balance"])
            .await
            .expect("get_projected")
            .expect("row");

        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Value::String("Charlie".into()));
        assert_eq!(row[1], Value::Decimal(rust_decimal::Decimal::new(100, 0)));
    }

    #[tokio::test]
    async fn update_row() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        engine
            .insert(
                "customers",
                &[
                    Value::Integer(1),
                    Value::String("Dana".into()),
                    Value::Null,
                    Value::Decimal(rust_decimal::Decimal::new(500, 0)),
                    Value::Bool(true),
                ],
            )
            .await
            .expect("insert");

        // Update balance.
        let mut updates = HashMap::new();
        updates.insert(
            "balance".into(),
            Value::Decimal(rust_decimal::Decimal::new(600, 0)),
        );
        let updated = engine
            .update("customers", &Value::Integer(1), &updates)
            .await
            .expect("update");
        assert!(updated);

        // Verify.
        let row = engine
            .get("customers", &Value::Integer(1))
            .await
            .expect("get")
            .expect("row");
        assert_eq!(row[3], Value::Decimal(rust_decimal::Decimal::new(600, 0)));
        // Other fields unchanged.
        assert_eq!(row[1], Value::String("Dana".into()));
    }

    #[tokio::test]
    async fn delete_row() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        engine
            .insert(
                "customers",
                &[
                    Value::Integer(1),
                    Value::String("Eve".into()),
                    Value::Null,
                    Value::Decimal(rust_decimal::Decimal::ZERO),
                    Value::Null,
                ],
            )
            .await
            .expect("insert");

        assert!(
            engine
                .delete("customers", &Value::Integer(1))
                .await
                .expect("delete")
        );
        assert!(
            !engine
                .delete("customers", &Value::Integer(1))
                .await
                .expect("delete again")
        );
        assert!(
            engine
                .get("customers", &Value::Integer(1))
                .await
                .expect("get")
                .is_none()
        );
    }

    #[tokio::test]
    async fn duplicate_pk_rejected() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        let row = vec![
            Value::Integer(1),
            Value::String("X".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ];

        engine
            .insert("customers", &row)
            .await
            .expect("first insert");
        let err = engine.insert("customers", &row).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn scan_and_count() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        for i in 0..5 {
            engine
                .insert(
                    "customers",
                    &[
                        Value::Integer(i),
                        Value::String(format!("user_{i}")),
                        Value::Null,
                        Value::Decimal(rust_decimal::Decimal::from(i)),
                        Value::Null,
                    ],
                )
                .await
                .expect("insert");
        }

        assert_eq!(engine.count("customers").await.expect("count"), 5);

        let raw = engine.scan_raw("customers").await.expect("scan_raw");
        assert_eq!(raw.len(), 5);
    }

    #[tokio::test]
    async fn restore_from_storage() {
        let storage = Arc::new(MemStorage::new());

        // Create and populate.
        {
            let mut engine = StrictEngine::new(Arc::clone(&storage));
            engine
                .create_collection("customers", crm_schema())
                .await
                .expect("create");
            engine
                .insert(
                    "customers",
                    &[
                        Value::Integer(1),
                        Value::String("Alice".into()),
                        Value::Null,
                        Value::Decimal(rust_decimal::Decimal::ZERO),
                        Value::Null,
                    ],
                )
                .await
                .expect("insert");
        }

        // Restore from storage (simulating restart).
        let engine = StrictEngine::restore(Arc::clone(&storage))
            .await
            .expect("restore");

        assert!(engine.schema("customers").is_some());
        let row = engine
            .get("customers", &Value::Integer(1))
            .await
            .expect("get")
            .expect("row");
        assert_eq!(row[0], Value::Integer(1));
        assert_eq!(row[1], Value::String("Alice".into()));
    }

    #[tokio::test]
    async fn batch_insert() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        let rows: Vec<Vec<Value>> = (0..10)
            .map(|i| {
                vec![
                    Value::Integer(i),
                    Value::String(format!("batch_{i}")),
                    Value::Null,
                    Value::Decimal(rust_decimal::Decimal::from(i)),
                    Value::Null,
                ]
            })
            .collect();

        engine
            .insert_batch("customers", &rows)
            .await
            .expect("batch_insert");

        assert_eq!(engine.count("customers").await.expect("count"), 10);
    }

    #[tokio::test]
    async fn drop_collection() {
        let storage = Arc::new(MemStorage::new());
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");

        engine
            .insert(
                "customers",
                &[
                    Value::Integer(1),
                    Value::String("X".into()),
                    Value::Null,
                    Value::Decimal(rust_decimal::Decimal::ZERO),
                    Value::Null,
                ],
            )
            .await
            .expect("insert");

        engine.drop_collection("customers").await.expect("drop");

        assert!(engine.schema("customers").is_none());
        assert!(engine.collection_names().is_empty());
    }
}
