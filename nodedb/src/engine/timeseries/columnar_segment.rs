//! Columnar segment writer and reader for time-partitioned storage.
//!
//! Flushes columnar memtable data to per-column files within a partition
//! directory. Each column is stored as a separate file for projection pushdown
//! (only read columns the query needs).
//!
//! Layout per partition directory:
//! ```text
//! {collection}/ts-{start}_{end}/
//! ├── timestamp.col       # Codec-compressed i64 timestamps
//! ├── value.col           # Codec-compressed f64 values
//! ├── {tag}.sym           # Symbol dictionary for tag columns
//! ├── {tag}.col           # u32 symbol IDs (raw LE or codec-compressed)
//! ├── schema.json         # Column names, types, and codecs
//! └── partition.meta      # PartitionMeta + per-column statistics (JSON)
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use nodedb_codec::{ColumnCodec, ColumnStatistics, ColumnTypeHint};
use nodedb_types::timeseries::{PartitionMeta, PartitionState, SymbolDictionary};
use sonic_rs;

use super::columnar_memtable::{ColumnData, ColumnType, ColumnarDrainResult, ColumnarSchema};

// ---------------------------------------------------------------------------
// Segment writer
// ---------------------------------------------------------------------------

/// Writes drained columnar memtable data to a partition directory.
pub struct ColumnarSegmentWriter {
    base_dir: PathBuf,
}

impl ColumnarSegmentWriter {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Write a drained memtable to a partition directory.
    ///
    /// Creates the directory and writes one file per column plus metadata.
    /// Uses per-column codec selection: resolves `Auto` codecs at write time
    /// based on column type and data distribution.
    /// Returns the partition metadata with per-column statistics.
    pub fn write_partition(
        &self,
        partition_name: &str,
        drain: &ColumnarDrainResult,
        interval_ms: u64,
        flush_wal_lsn: u64,
    ) -> Result<PartitionMeta, SegmentError> {
        let partition_dir = self.base_dir.join(partition_name);
        std::fs::create_dir_all(&partition_dir)
            .map_err(|e| SegmentError::Io(format!("create dir: {e}")))?;

        let mut column_stats = HashMap::new();
        let mut resolved_codecs = Vec::with_capacity(drain.schema.columns.len());

        for (i, (col_name, col_type)) in drain.schema.columns.iter().enumerate() {
            let col_data = &drain.columns[i];
            let requested_codec = drain.schema.codec(i);

            let (encoded, resolved_codec, stats) =
                encode_column(col_data, *col_type, requested_codec)?;

            let path = partition_dir.join(format!("{col_name}.col"));
            std::fs::write(&path, &encoded)
                .map_err(|e| SegmentError::Io(format!("write {}: {e}", path.display())))?;

            // Write symbol dictionary for tag columns.
            if *col_type == ColumnType::Symbol
                && let Some(dict) = drain.symbol_dicts.get(&i)
            {
                let dict_json = sonic_rs::to_vec(dict)
                    .map_err(|e| SegmentError::Io(format!("serialize dict: {e}")))?;
                let sym_path = partition_dir.join(format!("{col_name}.sym"));
                std::fs::write(&sym_path, &dict_json)
                    .map_err(|e| SegmentError::Io(format!("write {}: {e}", sym_path.display())))?;
            }

            column_stats.insert(col_name.clone(), stats);
            resolved_codecs.push(resolved_codec);
        }

        // Write schema with resolved codecs.
        let schema_with_codecs = ColumnarSchema {
            columns: drain.schema.columns.clone(),
            timestamp_idx: drain.schema.timestamp_idx,
            codecs: resolved_codecs,
        };
        let schema_json = sonic_rs::to_vec(&schema_to_json(&schema_with_codecs))
            .map_err(|e| SegmentError::Io(format!("serialize schema: {e}")))?;
        std::fs::write(partition_dir.join("schema.json"), &schema_json)
            .map_err(|e| SegmentError::Io(format!("write schema: {e}")))?;

        // Build and write sparse index from raw (pre-compression) column data.
        let sparse_idx = super::sparse_index::SparseIndex::build(
            &drain.columns,
            &drain.schema,
            drain.row_count,
            super::sparse_index::DEFAULT_BLOCK_SIZE,
        );
        let sparse_bytes = sparse_idx.to_bytes();
        std::fs::write(partition_dir.join("sparse_index.bin"), &sparse_bytes)
            .map_err(|e| SegmentError::Io(format!("write sparse index: {e}")))?;

        let size_bytes = dir_size(&partition_dir)?;

        let meta = PartitionMeta {
            min_ts: drain.min_ts,
            max_ts: drain.max_ts,
            row_count: drain.row_count,
            size_bytes,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms,
            last_flushed_wal_lsn: flush_wal_lsn,
            column_stats,
        };

        let meta_json = sonic_rs::to_vec(&meta)
            .map_err(|e| SegmentError::Io(format!("serialize meta: {e}")))?;
        std::fs::write(partition_dir.join("partition.meta"), &meta_json)
            .map_err(|e| SegmentError::Io(format!("write meta: {e}")))?;

        Ok(meta)
    }
}

// ---------------------------------------------------------------------------
// Per-column encoding with codec selection
// ---------------------------------------------------------------------------

/// Encode a single column using the appropriate codec pipeline.
///
/// Resolves `Auto` to the optimal codec based on column type and data.
/// Delegates to `nodedb_codec::pipeline` for both cascading and legacy codecs.
/// Returns (encoded_bytes, resolved_codec, column_statistics).
fn encode_column(
    col_data: &ColumnData,
    col_type: ColumnType,
    requested_codec: ColumnCodec,
) -> Result<(Vec<u8>, ColumnCodec, ColumnStatistics), SegmentError> {
    match col_type {
        ColumnType::Timestamp => {
            let values = col_data.as_timestamps();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect::detect_i64_codec(values)
            } else {
                requested_codec
            };
            let encoded = nodedb_codec::encode_i64_pipeline(values, codec)
                .map_err(|e| SegmentError::Io(format!("encode ts: {e}")))?;
            let stats = ColumnStatistics::from_i64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Float64 => {
            let values = col_data.as_f64();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect::detect_f64_codec(values)
            } else {
                requested_codec
            };
            let encoded = nodedb_codec::encode_f64_pipeline(values, codec)
                .map_err(|e| SegmentError::Io(format!("encode f64: {e}")))?;
            let stats = ColumnStatistics::from_f64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Int64 => {
            let values = col_data.as_i64();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect::detect_i64_codec(values)
            } else {
                requested_codec
            };
            let encoded = nodedb_codec::encode_i64_pipeline(values, codec)
                .map_err(|e| SegmentError::Io(format!("encode i64: {e}")))?;
            let stats = ColumnStatistics::from_i64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Symbol => {
            let values = col_data.as_symbols();
            let codec = if requested_codec == ColumnCodec::Auto {
                nodedb_codec::detect_codec(requested_codec, ColumnTypeHint::Symbol)
            } else {
                requested_codec
            };
            let raw: Vec<u8> = values.iter().flat_map(|id| id.to_le_bytes()).collect();
            let encoded = nodedb_codec::encode_bytes_pipeline(&raw, codec)
                .map_err(|e| SegmentError::Io(format!("encode sym: {e}")))?;
            let cardinality = values.iter().copied().max().map_or(0, |m| m + 1);
            let stats =
                ColumnStatistics::from_symbols(values, cardinality, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
    }
}

// ---------------------------------------------------------------------------
// Segment reader
// ---------------------------------------------------------------------------

/// Reads columnar data from a partition directory.
pub struct ColumnarSegmentReader;

impl ColumnarSegmentReader {
    /// Read a partition's metadata.
    pub fn read_meta(partition_dir: &Path) -> Result<PartitionMeta, SegmentError> {
        let meta_path = partition_dir.join("partition.meta");
        let data = std::fs::read(&meta_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", meta_path.display())))?;
        sonic_rs::from_slice(&data).map_err(|e| SegmentError::Io(format!("parse meta: {e}")))
    }

    /// Read the schema from a partition directory.
    pub fn read_schema(partition_dir: &Path) -> Result<ColumnarSchema, SegmentError> {
        let schema_path = partition_dir.join("schema.json");
        let data = std::fs::read(&schema_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", schema_path.display())))?;
        let json: SchemaJson = sonic_rs::from_slice(&data)
            .map_err(|e| SegmentError::Io(format!("parse schema: {e}")))?;
        schema_from_parsed(&json)
    }

    /// Read a single column from a partition directory, using the codec
    /// stored in schema metadata.
    pub fn read_column(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
    ) -> Result<ColumnData, SegmentError> {
        Self::read_column_with_codec(partition_dir, col_name, col_type, None)
    }

    /// Read a single column with an explicit codec override.
    ///
    /// If `codec` is None, reads the codec from the partition metadata.
    /// Falls back to legacy Gorilla decoding for old partitions without
    /// codec metadata.
    pub fn read_column_with_codec(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ColumnCodec>,
    ) -> Result<ColumnData, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let data = std::fs::read(&col_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))?;

        // Determine codec: explicit override > metadata > legacy default.
        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });

        decode_column(&data, col_type, codec)
    }

    /// Decode a column from pre-read raw bytes.
    ///
    /// Used with `UringReader::read_files()` which batch-reads all column
    /// files via io_uring, returning raw bytes. This method decodes those
    /// bytes into `ColumnData` without additional disk I/O.
    pub fn decode_column_from_bytes(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ColumnCodec>,
        raw_bytes: &[u8],
    ) -> Result<ColumnData, SegmentError> {
        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });
        decode_column(raw_bytes, col_type, codec)
    }

    /// Read the symbol dictionary for a tag column.
    pub fn read_symbol_dict(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<SymbolDictionary, SegmentError> {
        let sym_path = partition_dir.join(format!("{col_name}.sym"));
        let data = std::fs::read(&sym_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", sym_path.display())))?;
        sonic_rs::from_slice(&data).map_err(|e| SegmentError::Io(format!("parse symbol dict: {e}")))
    }

    /// Read specific columns by name (projection pushdown).
    ///
    /// Reads partition metadata once and resolves codecs for all columns,
    /// avoiding per-column metadata reads.
    pub fn read_columns(
        partition_dir: &Path,
        requested: &[(String, ColumnType)],
    ) -> Result<Vec<ColumnData>, SegmentError> {
        // Read metadata once to resolve codecs for all columns.
        let meta = Self::read_meta(partition_dir).ok();
        let mut result = Vec::with_capacity(requested.len());
        for (name, ty) in requested {
            let codec = meta
                .as_ref()
                .and_then(|m| m.column_stats.get(name).map(|s| s.codec));
            result.push(Self::read_column_with_codec(
                partition_dir,
                name,
                *ty,
                codec,
            )?);
        }
        Ok(result)
    }

    /// Read raw compressed bytes of a column without decoding.
    pub fn read_column_raw(partition_dir: &Path, col_name: &str) -> Result<Vec<u8>, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        std::fs::read(&col_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))
    }

    /// Decode specific FastLanes blocks from a column.
    ///
    /// Only decodes blocks in `block_indices` (0-based). Other blocks are
    /// skipped without decompression. Returns concatenated values from all
    /// requested blocks in order.
    ///
    /// Falls back to full decode for non-FastLanes codecs.
    pub fn read_column_blocks(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ColumnCodec>,
        block_indices: &[usize],
    ) -> Result<(ColumnData, Vec<(usize, usize)>), SegmentError> {
        use nodedb_codec::ColumnCodec;

        let raw = Self::read_column_raw(partition_dir, col_name)?;
        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });

        // Only plain FastLanesLz4 supports block-level decode.
        // DeltaFastLanesLz4 and AlpFastLanesLz4 have preceding transform
        // stages that prevent independent block decode.
        let is_fastlanes = matches!(codec, ColumnCodec::FastLanesLz4);

        if !is_fastlanes || block_indices.is_empty() {
            // Fall back to full decode.
            let data = decode_column(&raw, col_type, codec)?;
            let total = match &data {
                ColumnData::Timestamp(v) => v.len(),
                ColumnData::Float64(v) => v.len(),
                ColumnData::Int64(v) => v.len(),
                ColumnData::Symbol(v) => v.len(),
                ColumnData::DictEncoded { ids, .. } => ids.len(),
            };
            return Ok((data, vec![(0, total)]));
        }

        // For FastLanesLz4: decompress LZ4 first, then selective FastLanes decode.
        let fastlanes_bytes = if codec == ColumnCodec::FastLanesLz4 {
            nodedb_codec::decode_bytes_pipeline(&raw, ColumnCodec::Lz4)
                .map_err(|e| SegmentError::Io(format!("lz4 decode: {e}")))?
        } else {
            raw
        };

        // Decode only requested blocks.
        let mut all_values = Vec::new();
        let mut ranges = Vec::new();
        let mut iter = nodedb_codec::fastlanes::BlockIterator::new(&fastlanes_bytes)
            .map_err(|e| SegmentError::Io(format!("block iter: {e}")))?;

        let mut current_block = 0;
        let mut bi_pos = 0; // position in sorted block_indices

        while bi_pos < block_indices.len() {
            let target = block_indices[bi_pos];

            // Skip blocks until we reach the target.
            while current_block < target {
                if iter.skip_block().is_err() {
                    break;
                }
                current_block += 1;
            }

            if current_block != target {
                break;
            }

            // Decode this block.
            let start = all_values.len();
            match iter.next() {
                Some(Ok(block_vals)) => {
                    all_values.extend(block_vals);
                }
                Some(Err(e)) => {
                    return Err(SegmentError::Io(format!("block decode: {e}")));
                }
                None => break,
            }
            ranges.push((start, all_values.len()));
            current_block += 1;
            bi_pos += 1;
        }

        // Convert i64 values to the correct ColumnData type.
        let data = match col_type {
            ColumnType::Timestamp => ColumnData::Timestamp(all_values),
            ColumnType::Int64 => ColumnData::Int64(all_values),
            ColumnType::Float64 => {
                let f64_vals: Vec<f64> = all_values
                    .iter()
                    .map(|&v| f64::from_bits(v as u64))
                    .collect();
                ColumnData::Float64(f64_vals)
            }
            ColumnType::Symbol => {
                let u32_vals: Vec<u32> = all_values.iter().map(|&v| v as u32).collect();
                ColumnData::Symbol(u32_vals)
            }
        };

        Ok((data, ranges))
    }

    /// Memory-map a column file for zero-copy SIMD access.
    pub fn mmap_column(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<memmap2::Mmap, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let file = std::fs::File::open(&col_path)
            .map_err(|e| SegmentError::Io(format!("open {}: {e}", col_path.display())))?;
        // SAFETY: We require the file not be modified while mapped.
        // Data Plane partitions are immutable once sealed.
        unsafe {
            memmap2::MmapOptions::new()
                .map(&file)
                .map_err(|e| SegmentError::Io(format!("mmap {}: {e}", col_path.display())))
        }
    }

    /// Interpret mmap'd raw LE bytes as an i64 slice (zero-copy).
    pub fn mmap_as_i64(mmap: &memmap2::Mmap) -> Result<&[i64], SegmentError> {
        if !mmap.len().is_multiple_of(8) {
            return Err(SegmentError::Corrupt(
                "i64 mmap not aligned to 8 bytes".into(),
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const i64, mmap.len() / 8) })
    }

    /// Interpret mmap'd raw LE bytes as a u32 slice (zero-copy).
    pub fn mmap_as_u32(mmap: &memmap2::Mmap) -> Result<&[u32], SegmentError> {
        if !mmap.len().is_multiple_of(4) {
            return Err(SegmentError::Corrupt(
                "u32 mmap not aligned to 4 bytes".into(),
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const u32, mmap.len() / 4) })
    }

    /// Read the sparse primary index for a partition.
    ///
    /// Returns `None` if no sparse index exists (legacy partitions).
    pub fn read_sparse_index(
        partition_dir: &Path,
    ) -> Result<Option<super::sparse_index::SparseIndex>, SegmentError> {
        let idx_path = partition_dir.join("sparse_index.bin");
        match std::fs::read(&idx_path) {
            Ok(data) => {
                let idx = super::sparse_index::SparseIndex::from_bytes(&data)
                    .map_err(|e| SegmentError::Corrupt(format!("sparse index: {e}")))?;
                Ok(Some(idx))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SegmentError::Io(format!(
                "read {}: {e}",
                idx_path.display()
            ))),
        }
    }

    // -- Metadata-only query methods (Level 0 read avoidance) --

    /// Get the row count from partition metadata without reading any column data.
    pub fn metadata_row_count(partition_dir: &Path) -> Result<u64, SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        Ok(meta.row_count)
    }

    /// Get the timestamp range from partition metadata.
    pub fn metadata_ts_range(partition_dir: &Path) -> Result<(i64, i64), SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        Ok((meta.min_ts, meta.max_ts))
    }

    /// Get per-column statistics from partition metadata.
    ///
    /// Enables answering `MIN/MAX/SUM/COUNT` queries without decompression:
    /// - `COUNT(*)` → `row_count`
    /// - `MIN(col)` → `column_stats[col].min`
    /// - `MAX(col)` → `column_stats[col].max`
    /// - `SUM(col)` → `column_stats[col].sum`
    pub fn metadata_column_stats(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<Option<ColumnStatistics>, SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        Ok(meta.column_stats.get(col_name).cloned())
    }

    /// Check if a partition could contain rows matching a predicate,
    /// using only partition-level column statistics (no block-level index).
    ///
    /// Returns `false` if the partition can definitely be skipped.
    pub fn metadata_might_match(
        partition_dir: &Path,
        col_name: &str,
        predicate: &super::sparse_index::BlockPredicate,
    ) -> Result<bool, SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        match meta.column_stats.get(col_name) {
            Some(stats) => {
                let block_stats = super::sparse_index::BlockColumnStats {
                    min: stats.min.unwrap_or(f64::NEG_INFINITY),
                    max: stats.max.unwrap_or(f64::INFINITY),
                };
                Ok(predicate.might_match(&block_stats))
            }
            None => Ok(true), // No stats → can't skip.
        }
    }
}

// ---------------------------------------------------------------------------
// Per-column decoding with codec selection
// ---------------------------------------------------------------------------

/// Legacy default codecs for partitions written before V2 codec metadata.
fn legacy_default_codec(col_type: ColumnType) -> ColumnCodec {
    match col_type {
        ColumnType::Timestamp => ColumnCodec::Gorilla,
        ColumnType::Float64 => ColumnCodec::Gorilla,
        ColumnType::Int64 => ColumnCodec::Raw,
        ColumnType::Symbol => ColumnCodec::Raw,
    }
}

/// Decode a column file using the codec pipeline.
///
/// Delegates to `nodedb_codec::pipeline` for both cascading and legacy codecs.
fn decode_column(
    data: &[u8],
    col_type: ColumnType,
    codec: ColumnCodec,
) -> Result<ColumnData, SegmentError> {
    let map_err = |e: nodedb_codec::CodecError| SegmentError::Corrupt(format!("{codec}: {e}"));

    match col_type {
        ColumnType::Timestamp => {
            let values = nodedb_codec::decode_i64_pipeline(data, codec).map_err(map_err)?;
            Ok(ColumnData::Timestamp(values))
        }
        ColumnType::Float64 => {
            let values = nodedb_codec::decode_f64_pipeline(data, codec).map_err(map_err)?;
            Ok(ColumnData::Float64(values))
        }
        ColumnType::Int64 => {
            let values = nodedb_codec::decode_i64_pipeline(data, codec).map_err(map_err)?;
            Ok(ColumnData::Int64(values))
        }
        ColumnType::Symbol => {
            let raw = nodedb_codec::decode_bytes_pipeline(data, codec).map_err(map_err)?;
            let ids: Vec<u32> = raw
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            Ok(ColumnData::Symbol(ids))
        }
    }
}

// ---------------------------------------------------------------------------
// Schema serialization (V2: includes codec per column)
// ---------------------------------------------------------------------------

/// Schema entry for JSON serialization.
#[derive(serde::Serialize, serde::Deserialize)]
struct SchemaEntry {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
    /// Codec used for this column. Absent in legacy schemas (defaults to Auto).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    codec: Option<ColumnCodec>,
}

/// Schema JSON format — V2 is an array of objects, V1 is an array of tuples.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum SchemaJson {
    /// V2: array of `{ name, type, codec }` objects.
    V2(Vec<SchemaEntry>),
    /// V1 (legacy): array of `[name, type]` tuples.
    V1(Vec<(String, String)>),
}

fn schema_to_json(schema: &ColumnarSchema) -> Vec<SchemaEntry> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| {
            let ty_str = match ty {
                ColumnType::Timestamp => "timestamp",
                ColumnType::Float64 => "float64",
                ColumnType::Int64 => "int64",
                ColumnType::Symbol => "symbol",
            };
            let codec = schema.codecs.get(i).copied();
            SchemaEntry {
                name: name.clone(),
                col_type: ty_str.to_string(),
                codec,
            }
        })
        .collect()
}

fn schema_from_parsed(json: &SchemaJson) -> Result<ColumnarSchema, SegmentError> {
    match json {
        SchemaJson::V2(entries) => {
            let mut columns = Vec::with_capacity(entries.len());
            let mut codecs = Vec::with_capacity(entries.len());
            let mut timestamp_idx = 0;

            for (i, entry) in entries.iter().enumerate() {
                let ty = parse_column_type(&entry.col_type)?;
                if ty == ColumnType::Timestamp {
                    timestamp_idx = i;
                }
                columns.push((entry.name.clone(), ty));
                codecs.push(entry.codec.unwrap_or(ColumnCodec::Auto));
            }

            Ok(ColumnarSchema {
                columns,
                timestamp_idx,
                codecs,
            })
        }
        SchemaJson::V1(tuples) => {
            let mut columns = Vec::with_capacity(tuples.len());
            let mut timestamp_idx = 0;

            for (i, (name, ty_str)) in tuples.iter().enumerate() {
                let ty = parse_column_type(ty_str)?;
                if ty == ColumnType::Timestamp {
                    timestamp_idx = i;
                }
                columns.push((name.clone(), ty));
            }

            Ok(ColumnarSchema {
                codecs: vec![ColumnCodec::Auto; columns.len()],
                columns,
                timestamp_idx,
            })
        }
    }
}

fn parse_column_type(ty_str: &str) -> Result<ColumnType, SegmentError> {
    match ty_str {
        "timestamp" => Ok(ColumnType::Timestamp),
        "float64" => Ok(ColumnType::Float64),
        "int64" => Ok(ColumnType::Int64),
        "symbol" => Ok(ColumnType::Symbol),
        other => Err(SegmentError::Corrupt(format!(
            "unknown column type: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn dir_size(dir: &Path) -> Result<u64, SegmentError> {
    let mut size = 0u64;
    let entries = std::fs::read_dir(dir)
        .map_err(|e| SegmentError::Io(format!("read dir {}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| SegmentError::Io(format!("dir entry: {e}")))?;
        let meta = entry
            .metadata()
            .map_err(|e| SegmentError::Io(format!("metadata {}: {e}", entry.path().display())))?;
        size += meta.len();
    }
    Ok(size)
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum SegmentError {
    Io(String),
    Corrupt(String),
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "segment I/O error: {msg}"),
            Self::Corrupt(msg) => write!(f, "segment corrupt: {msg}"),
        }
    }
}

impl std::error::Error for SegmentError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig,
    };
    use nodedb_types::timeseries::MetricSample;
    use tempfile::TempDir;

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn write_and_read_simple_partition() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: i as f64 * 0.5,
                },
            );
        }
        let drain = mt.drain();

        let meta = writer
            .write_partition("ts-test", &drain, 86_400_000, 42)
            .unwrap();
        assert_eq!(meta.row_count, 100);
        assert_eq!(meta.min_ts, 1000);
        assert_eq!(meta.max_ts, 1099);
        assert_eq!(meta.last_flushed_wal_lsn, 42);
        assert!(meta.size_bytes > 0);

        // Verify column statistics were written.
        assert!(meta.column_stats.contains_key("timestamp"));
        assert!(meta.column_stats.contains_key("value"));
        let ts_stats = &meta.column_stats["timestamp"];
        assert_eq!(ts_stats.count, 100);
        assert_eq!(ts_stats.codec, ColumnCodec::DoubleDelta);
        let val_stats = &meta.column_stats["value"];
        assert_eq!(val_stats.count, 100);
        assert_eq!(val_stats.codec, ColumnCodec::Gorilla);

        // Read back.
        let part_dir = tmp.path().join("ts-test");
        let read_meta = ColumnarSegmentReader::read_meta(&part_dir).unwrap();
        assert_eq!(read_meta.row_count, 100);
        assert!(read_meta.column_stats.contains_key("timestamp"));

        let schema = ColumnarSegmentReader::read_schema(&part_dir).unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.codec(0), ColumnCodec::DoubleDelta);
        assert_eq!(schema.codec(1), ColumnCodec::Gorilla);

        let ts_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "timestamp",
            ColumnType::Timestamp,
            Some(ColumnCodec::DoubleDelta),
        )
        .unwrap();
        let timestamps = ts_col.as_timestamps();
        assert_eq!(timestamps.len(), 100);
        assert_eq!(timestamps[0], 1000);
        assert_eq!(timestamps[99], 1099);

        let val_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "value",
            ColumnType::Float64,
            Some(ColumnCodec::Gorilla),
        )
        .unwrap();
        let values = val_col.as_f64();
        assert_eq!(values.len(), 100);
        assert!((values[0] - 0.0).abs() < f64::EPSILON);
        assert!((values[99] - 49.5).abs() < f64::EPSILON);
    }

    #[test]
    fn write_and_read_with_tags() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());

        for i in 0..50 {
            let host = if i % 2 == 0 { "prod-1" } else { "prod-2" };
            mt.ingest_row(
                i % 2,
                &[
                    ColumnValue::Timestamp(5000 + i as i64),
                    ColumnValue::Float64(50.0 + i as f64),
                    ColumnValue::Symbol(host),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();

        let meta = writer
            .write_partition("ts-tags", &drain, 86_400_000, 99)
            .unwrap();
        assert_eq!(meta.row_count, 50);
        assert_eq!(meta.column_stats["host"].codec, ColumnCodec::FastLanesLz4);
        assert_eq!(meta.column_stats["host"].cardinality, Some(2));

        let part_dir = tmp.path().join("ts-tags");
        let host_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "host",
            ColumnType::Symbol,
            Some(ColumnCodec::FastLanesLz4),
        )
        .unwrap();
        assert_eq!(host_col.as_symbols().len(), 50);

        let host_dict = ColumnarSegmentReader::read_symbol_dict(&part_dir, "host").unwrap();
        assert_eq!(host_dict.len(), 2);
    }

    #[test]
    fn column_projection() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("extra".into(), ColumnType::Int64),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        for i in 0..20 {
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(i * 100),
                    ColumnValue::Float64(i as f64),
                    ColumnValue::Int64(i * 10),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();
        let meta = writer
            .write_partition("ts-proj", &drain, 86_400_000, 0)
            .unwrap();

        // Int64 column should use Delta codec.
        assert!(matches!(
            meta.column_stats["extra"].codec,
            ColumnCodec::Delta | ColumnCodec::DoubleDelta
        ));

        let part_dir = tmp.path().join("ts-proj");
        let projected = ColumnarSegmentReader::read_columns(
            &part_dir,
            &[
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
        )
        .unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].len(), 20);
        assert_eq!(projected[1].len(), 20);
    }

    #[test]
    fn explicit_codec_override() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        // Force Gorilla codec for timestamps (instead of DoubleDelta).
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Gorilla, ColumnCodec::Gorilla],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: 42.0 + i as f64 * 0.1,
                },
            );
        }
        let drain = mt.drain();
        let meta = writer
            .write_partition("ts-gorilla", &drain, 86_400_000, 0)
            .unwrap();

        assert_eq!(meta.column_stats["timestamp"].codec, ColumnCodec::Gorilla);
        assert_eq!(meta.column_stats["value"].codec, ColumnCodec::Gorilla);

        // Read back with explicit codec.
        let part_dir = tmp.path().join("ts-gorilla");
        let ts_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "timestamp",
            ColumnType::Timestamp,
            Some(ColumnCodec::Gorilla),
        )
        .unwrap();
        let timestamps = ts_col.as_timestamps();
        assert_eq!(timestamps.len(), 100);
        assert_eq!(timestamps[0], 1_700_000_000_000);
    }

    #[test]
    fn compression_stats_in_metadata() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..10_000 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: 42.0,
                },
            );
        }
        let drain = mt.drain();
        let meta = writer
            .write_partition("ts-stats", &drain, 86_400_000, 0)
            .unwrap();

        let ts_stats = &meta.column_stats["timestamp"];
        assert!(ts_stats.compression_ratio() > 3.0);
        assert_eq!(ts_stats.min, Some(1_700_000_000_000.0));
        assert_eq!(ts_stats.count, 10_000);

        let val_stats = &meta.column_stats["value"];
        assert!(val_stats.compression_ratio() > 1.0);
        assert_eq!(val_stats.min, Some(42.0));
        assert_eq!(val_stats.max, Some(42.0));
        assert_eq!(val_stats.sum, Some(420_000.0));
    }

    #[test]
    fn schema_v2_roundtrip() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![
                ColumnCodec::DoubleDelta,
                ColumnCodec::Gorilla,
                ColumnCodec::Raw,
            ],
        };
        let json = sonic_rs::to_vec(&schema_to_json(&schema)).unwrap();
        let parsed: SchemaJson = sonic_rs::from_slice(&json).unwrap();
        let recovered = schema_from_parsed(&parsed).unwrap();

        assert_eq!(recovered.columns, schema.columns);
        assert_eq!(recovered.timestamp_idx, 0);
        assert_eq!(recovered.codecs, schema.codecs);
    }

    #[test]
    fn sparse_index_written_during_flush() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..2048 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: (i % 100) as f64,
                },
            );
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-sparse", &drain, 86_400_000, 0)
            .unwrap();

        // Verify sparse index file exists and is readable.
        let part_dir = tmp.path().join("ts-sparse");
        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir)
            .unwrap()
            .expect("sparse index should exist");

        assert_eq!(sparse.total_rows(), 2048);
        assert_eq!(sparse.block_count(), 2); // 2048 / 1024 = 2 blocks
        assert_eq!(sparse.column_names, vec!["timestamp", "value"]);

        // Block 0 should have timestamps for rows 0..1024.
        assert_eq!(sparse.blocks[0].min_ts, 1_700_000_000_000);
        assert_eq!(sparse.blocks[0].max_ts, 1_700_000_000_000 + 1023 * 10_000);

        // Block 1 should have timestamps for rows 1024..2048.
        assert_eq!(sparse.blocks[1].min_ts, 1_700_000_000_000 + 1024 * 10_000);
    }

    #[test]
    fn sparse_index_time_range_query() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..5000 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 1000,
                    value: i as f64,
                },
            );
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-range", &drain, 86_400_000, 0)
            .unwrap();

        let part_dir = tmp.path().join("ts-range");
        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir)
            .unwrap()
            .unwrap();

        // 5000 rows / 1024 = 5 blocks (4 full + 1 partial).
        assert_eq!(sparse.block_count(), 5);

        // Query middle range: should match ~2 blocks, NOT all 5.
        let start = 1_700_000_000_000 + 2000 * 1000;
        let end = 1_700_000_000_000 + 3000 * 1000;
        let matching = sparse.blocks_in_time_range(start, end);
        assert!(matching.len() < 5, "should skip at least 1 block");
        assert!(!matching.is_empty());
    }

    #[test]
    fn sparse_index_predicate_pushdown() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        // Block 0: values 0..1024 → min=0, max=99 (cycling mod 100)
        // Block 1: values 1024..2048 → min=0, max=99 (same cycle)
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Auto; 2],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        for i in 0..2048 {
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(1_700_000_000_000 + i as i64 * 1000),
                    ColumnValue::Float64(if i < 1024 {
                        (i % 50) as f64 // Block 0: 0..49
                    } else {
                        50.0 + (i % 50) as f64 // Block 1: 50..99
                    }),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-pred", &drain, 86_400_000, 0)
            .unwrap();

        let part_dir = tmp.path().join("ts-pred");
        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir)
            .unwrap()
            .unwrap();

        // WHERE cpu > 60 → only block 1 (max of block 0 is 49).
        use crate::engine::timeseries::sparse_index::BlockPredicate;
        let preds = vec![BlockPredicate::GreaterThan {
            column_idx: 1,
            threshold: 60.0,
        }];
        let matching = sparse.filter_blocks(i64::MIN, i64::MAX, &preds);
        assert_eq!(matching, vec![1]);
    }

    #[test]
    fn metadata_only_queries() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..1000 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: 42.0 + i as f64 * 0.1,
                },
            );
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-meta", &drain, 86_400_000, 0)
            .unwrap();

        let part_dir = tmp.path().join("ts-meta");

        // Level 0: COUNT(*) from metadata — zero decompression.
        let count = ColumnarSegmentReader::metadata_row_count(&part_dir).unwrap();
        assert_eq!(count, 1000);

        // Level 0: MIN/MAX(ts) from metadata.
        let (min_ts, max_ts) = ColumnarSegmentReader::metadata_ts_range(&part_dir).unwrap();
        assert_eq!(min_ts, 1_700_000_000_000);
        assert_eq!(max_ts, 1_700_000_000_000 + 999 * 10_000);

        // Level 0: Column stats for SUM/MIN/MAX.
        let stats = ColumnarSegmentReader::metadata_column_stats(&part_dir, "value")
            .unwrap()
            .unwrap();
        assert_eq!(stats.count, 1000);
        assert!(stats.min.unwrap() < 43.0);
        assert!(stats.max.unwrap() > 140.0);

        // Partition-level predicate: WHERE value > 200 → skip entire partition.
        use crate::engine::timeseries::sparse_index::BlockPredicate;
        let pred = BlockPredicate::GreaterThan {
            column_idx: 0, // unused in metadata check
            threshold: 200.0,
        };
        let might = ColumnarSegmentReader::metadata_might_match(&part_dir, "value", &pred).unwrap();
        assert!(!might, "partition max < 200, should be skippable");

        // WHERE value > 50 → partition might match.
        let pred2 = BlockPredicate::GreaterThan {
            column_idx: 0,
            threshold: 50.0,
        };
        let might2 =
            ColumnarSegmentReader::metadata_might_match(&part_dir, "value", &pred2).unwrap();
        assert!(might2);
    }

    #[test]
    fn legacy_partition_no_sparse_index() {
        let tmp = TempDir::new().unwrap();
        // Create a partition dir without sparse_index.bin.
        let part_dir = tmp.path().join("ts-legacy");
        std::fs::create_dir_all(&part_dir).unwrap();
        std::fs::write(
            part_dir.join("partition.meta"),
            sonic_rs::to_vec(&PartitionMeta {
                min_ts: 0,
                max_ts: 100,
                row_count: 10,
                size_bytes: 100,
                schema_version: 1,
                state: PartitionState::Sealed,
                interval_ms: 86_400_000,
                last_flushed_wal_lsn: 0,
                column_stats: std::collections::HashMap::new(),
            })
            .unwrap(),
        )
        .unwrap();

        // read_sparse_index returns None for legacy partitions.
        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir).unwrap();
        assert!(sparse.is_none());
    }
}
