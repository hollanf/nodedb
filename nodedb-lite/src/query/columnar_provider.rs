//! DataFusion `TableProvider` for columnar collections.
//!
//! Reads compressed segments from storage, decodes requested columns via
//! SegmentReader with projection pushdown and delete bitmap masking,
//! and returns Arrow RecordBatches to DataFusion.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use nodedb_columnar::delete_bitmap::DeleteBitmap;
use nodedb_columnar::reader::{DecodedColumn, SegmentReader};
use nodedb_types::Namespace;
use nodedb_types::columnar::{ColumnType, ColumnarSchema};

use crate::storage::engine::StorageEngine;

/// A DataFusion `TableProvider` that reads from columnar segments.
///
/// Supports column projection pushdown: only requested columns are decoded.
/// Delete bitmaps are applied to mask deleted rows.
pub struct ColumnarTableProvider<S: StorageEngine> {
    collection: String,
    arrow_schema: SchemaRef,
    columnar_schema: ColumnarSchema,
    storage: Arc<S>,
    /// Segment IDs to read.
    segment_ids: Vec<u32>,
    /// Per-segment delete bitmaps (cloned from engine state).
    delete_bitmaps: Vec<(u32, DeleteBitmap)>,
}

impl<S: StorageEngine> std::fmt::Debug for ColumnarTableProvider<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarTableProvider")
            .field("collection", &self.collection)
            .field("segments", &self.segment_ids.len())
            .finish()
    }
}

impl<S: StorageEngine> ColumnarTableProvider<S> {
    /// Create a table provider for a columnar collection.
    pub fn new(
        collection: String,
        schema: &ColumnarSchema,
        storage: Arc<S>,
        segment_ids: Vec<u32>,
        delete_bitmaps: Vec<(u32, DeleteBitmap)>,
    ) -> Self {
        let arrow_schema = columnar_schema_to_arrow(schema);
        Self {
            collection,
            arrow_schema,
            columnar_schema: schema.clone(),
            storage,
            segment_ids,
            delete_bitmaps,
        }
    }

    /// Scan segments and build RecordBatches.
    fn scan_to_batches(
        &self,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let col_indices: Vec<usize> = match projection {
            Some(proj) => proj.clone(),
            None => (0..self.columnar_schema.columns.len()).collect(),
        };

        let projected_schema = if projection.is_some() {
            Arc::new(
                self.arrow_schema
                    .project(&col_indices)
                    .map_err(|e| DataFusionError::Execution(format!("project schema: {e}")))?,
            )
        } else {
            self.arrow_schema.clone()
        };

        // Collect decoded columns across all segments.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;

        for &seg_id in &self.segment_ids {
            let seg_key = format!("{}:seg:{}", self.collection, seg_id);
            let seg_bytes = tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(async {
                    self.storage
                        .get(Namespace::Columnar, seg_key.as_bytes())
                        .await
                })
            })
            .map_err(|e| DataFusionError::Execution(format!("storage read: {e}")))?;

            let Some(seg_bytes) = seg_bytes else {
                continue;
            };

            let reader = SegmentReader::open(&seg_bytes)
                .map_err(|e| DataFusionError::Execution(format!("open segment: {e}")))?;

            // Find delete bitmap for this segment.
            let empty_bitmap = DeleteBitmap::new();
            let bitmap = self
                .delete_bitmaps
                .iter()
                .find(|(id, _)| *id == seg_id)
                .map(|(_, bm)| bm)
                .unwrap_or(&empty_bitmap);

            // Read requested columns with delete masking (one batch per segment).
            let seg_columns: Vec<DecodedColumn> = col_indices
                .iter()
                .map(|&idx| reader.read_column_with_deletes(idx, &[], bitmap))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| DataFusionError::Execution(format!("read columns: {e}")))?;

            let seg_row_count = reader.row_count() as usize;

            // Convert decoded columns to Arrow arrays.
            let mut arrow_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(col_indices.len());
            for (i, decoded) in seg_columns.into_iter().enumerate() {
                let col_schema_idx = col_indices[i];
                let col_type = &self.columnar_schema.columns[col_schema_idx].column_type;
                let arr = decoded_to_arrow(decoded, col_type, seg_row_count)?;
                arrow_arrays.push(arr);
            }

            let batch = RecordBatch::try_new(projected_schema.clone(), arrow_arrays)
                .map_err(|e| DataFusionError::Execution(format!("build batch: {e}")))?;

            batches.push(batch);
            total_rows += batches.last().map_or(0, |b| b.num_rows());
            if let Some(lim) = limit
                && total_rows >= lim
            {
                break;
            }
        }

        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(projected_schema)]);
        }

        Ok(batches)
    }
}

#[async_trait]
impl<S: StorageEngine> TableProvider for ColumnarTableProvider<S> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let batches = self.scan_to_batches(projection, limit)?;
        let schema = if let Some(proj) = projection {
            Arc::new(self.arrow_schema.project(proj)?)
        } else {
            self.arrow_schema.clone()
        };
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
        mem_table.scan(state, None, &[], limit).await
    }
}

/// Convert a ColumnarSchema to Arrow Schema.
fn columnar_schema_to_arrow(schema: &ColumnarSchema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|col| {
            let dt = match &col.column_type {
                ColumnType::Int64 => DataType::Int64,
                ColumnType::Float64 => DataType::Float64,
                ColumnType::String => DataType::Utf8,
                ColumnType::Bool => DataType::Boolean,
                ColumnType::Bytes | ColumnType::Geometry => DataType::Binary,
                ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
                ColumnType::Decimal | ColumnType::Uuid => DataType::Utf8,
                ColumnType::Vector(_) => DataType::Binary,
            };
            Field::new(&col.name, dt, col.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Convert a DecodedColumn to an Arrow ArrayRef.
fn decoded_to_arrow(
    decoded: DecodedColumn,
    col_type: &ColumnType,
    _row_count: usize,
) -> Result<Arc<dyn Array>, DataFusionError> {
    match decoded {
        DecodedColumn::Int64 { values, valid } => {
            let null_buf = build_null_buffer(&valid);
            let arr = Int64Array::new(values.into(), null_buf);
            Ok(Arc::new(arr))
        }
        DecodedColumn::Float64 { values, valid } => {
            let null_buf = build_null_buffer(&valid);
            let arr = Float64Array::new(values.into(), null_buf);
            Ok(Arc::new(arr))
        }
        DecodedColumn::Timestamp { values, valid } => {
            let null_buf = build_null_buffer(&valid);
            let arr = TimestampMicrosecondArray::new(values.into(), null_buf);
            Ok(Arc::new(arr))
        }
        DecodedColumn::Bool { values, valid } => {
            let null_buf = build_null_buffer(&valid);
            let bool_buf = BooleanBuffer::from(values);
            let arr = BooleanArray::new(bool_buf, null_buf);
            Ok(Arc::new(arr))
        }
        DecodedColumn::Binary {
            data,
            offsets,
            valid,
        } => {
            match col_type {
                ColumnType::String | ColumnType::Uuid | ColumnType::Decimal => {
                    // Build StringArray from offsets + data.
                    let null_buf = build_null_buffer(&valid);
                    let mut strs: Vec<Option<&str>> = Vec::with_capacity(valid.len());
                    for (i, &is_valid) in valid.iter().enumerate() {
                        if is_valid && i + 1 < offsets.len() {
                            let start = offsets[i] as usize;
                            let end = offsets[i + 1] as usize;
                            strs.push(Some(std::str::from_utf8(&data[start..end]).unwrap_or("")));
                        } else {
                            strs.push(None);
                        }
                    }
                    let arr = StringArray::from(strs);
                    if null_buf.is_some() {
                        let data = arr
                            .into_data()
                            .into_builder()
                            .null_bit_buffer(null_buf.map(|nb| nb.into_inner().into_inner()))
                            .build()
                            .map_err(|e| {
                                DataFusionError::Execution(format!("build string array: {e}"))
                            })?;
                        Ok(Arc::new(StringArray::from(data)))
                    } else {
                        Ok(Arc::new(arr))
                    }
                }
                _ => {
                    // Binary types (Bytes, Geometry, Vector).
                    let mut blobs: Vec<Option<&[u8]>> = Vec::with_capacity(valid.len());
                    for (i, &is_valid) in valid.iter().enumerate() {
                        if is_valid && i + 1 < offsets.len() {
                            let start = offsets[i] as usize;
                            let end = offsets[i + 1] as usize;
                            blobs.push(Some(&data[start..end]));
                        } else {
                            blobs.push(None);
                        }
                    }
                    let arr = BinaryArray::from(blobs);
                    Ok(Arc::new(arr))
                }
            }
        }
        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            // Resolve IDs to strings and produce a StringArray.
            let strs: Vec<Option<&str>> = ids
                .iter()
                .zip(valid.iter())
                .map(|(&id, &is_valid)| {
                    if is_valid {
                        dictionary.get(id as usize).map(|s| s.as_str())
                    } else {
                        None
                    }
                })
                .collect();
            let arr = StringArray::from(strs);
            Ok(Arc::new(arr))
        }
    }
}

/// Build an Arrow NullBuffer from a validity vector.
fn build_null_buffer(valid: &[bool]) -> Option<NullBuffer> {
    let null_count = valid.iter().filter(|&&v| !v).count();
    if null_count == 0 {
        return None;
    }
    Some(NullBuffer::new(BooleanBuffer::from(valid.to_vec())))
}
