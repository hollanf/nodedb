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
                let dict_json = serde_json::to_vec(dict)
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
        let schema_json = serde_json::to_vec(&schema_to_json(&schema_with_codecs))
            .map_err(|e| SegmentError::Io(format!("serialize schema: {e}")))?;
        std::fs::write(partition_dir.join("schema.json"), &schema_json)
            .map_err(|e| SegmentError::Io(format!("write schema: {e}")))?;

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

        let meta_json = serde_json::to_vec(&meta)
            .map_err(|e| SegmentError::Io(format!("serialize meta: {e}")))?;
        std::fs::write(partition_dir.join("partition.meta"), &meta_json)
            .map_err(|e| SegmentError::Io(format!("write meta: {e}")))?;

        Ok(meta)
    }
}

// ---------------------------------------------------------------------------
// Per-column encoding with codec selection
// ---------------------------------------------------------------------------

/// Encode a single column using the appropriate codec.
///
/// Resolves `Auto` to the optimal codec based on column type and data.
/// Returns (encoded_bytes, resolved_codec, column_statistics).
fn encode_column(
    col_data: &ColumnData,
    col_type: ColumnType,
    requested_codec: ColumnCodec,
) -> Result<(Vec<u8>, ColumnCodec, ColumnStatistics), SegmentError> {
    match col_type {
        ColumnType::Timestamp => {
            let values = col_data.as_timestamps();
            let type_hint = ColumnTypeHint::Timestamp;
            let codec = nodedb_codec::detect_codec(requested_codec, type_hint);
            let encoded = encode_i64_with_codec(values, codec)?;
            let stats = ColumnStatistics::from_i64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Float64 => {
            let values = col_data.as_f64();
            let codec = nodedb_codec::detect_codec(requested_codec, ColumnTypeHint::Float64);
            let encoded = encode_f64_with_codec(values, codec)?;
            let stats = ColumnStatistics::from_f64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Int64 => {
            let values = col_data.as_i64();
            let codec = if requested_codec == ColumnCodec::Auto {
                // Use data-aware detection for i64 columns.
                nodedb_codec::detect::detect_i64_codec(values)
            } else {
                requested_codec
            };
            let encoded = encode_i64_with_codec(values, codec)?;
            let stats = ColumnStatistics::from_i64(values, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
        ColumnType::Symbol => {
            let values = col_data.as_symbols();
            let codec = nodedb_codec::detect_codec(requested_codec, ColumnTypeHint::Symbol);
            let raw: Vec<u8> = values.iter().flat_map(|id| id.to_le_bytes()).collect();
            let encoded = encode_bytes_with_codec(&raw, codec)?;
            let cardinality = values.iter().copied().max().map_or(0, |m| m + 1);
            let stats =
                ColumnStatistics::from_symbols(values, cardinality, codec, encoded.len() as u64);
            Ok((encoded, codec, stats))
        }
    }
}

/// Encode i64 values with a specific codec.
fn encode_i64_with_codec(values: &[i64], codec: ColumnCodec) -> Result<Vec<u8>, SegmentError> {
    match codec {
        ColumnCodec::DoubleDelta => Ok(nodedb_codec::double_delta::encode(values)),
        ColumnCodec::Delta => Ok(nodedb_codec::delta::encode(values)),
        ColumnCodec::Gorilla => Ok(nodedb_codec::gorilla::encode_timestamps(values)),
        ColumnCodec::Raw => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(nodedb_codec::raw::encode(&raw))
        }
        ColumnCodec::Lz4 => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(nodedb_codec::lz4::encode(&raw))
        }
        ColumnCodec::Zstd => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            nodedb_codec::zstd_codec::encode(&raw)
                .map_err(|e| SegmentError::Io(format!("zstd encode: {e}")))
        }
        ColumnCodec::Auto => {
            // Should have been resolved before calling this.
            Ok(nodedb_codec::double_delta::encode(values))
        }
    }
}

/// Encode f64 values with a specific codec.
fn encode_f64_with_codec(values: &[f64], codec: ColumnCodec) -> Result<Vec<u8>, SegmentError> {
    match codec {
        ColumnCodec::Gorilla => Ok(nodedb_codec::gorilla::encode_f64(values)),
        ColumnCodec::Raw => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(nodedb_codec::raw::encode(&raw))
        }
        ColumnCodec::Lz4 => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(nodedb_codec::lz4::encode(&raw))
        }
        ColumnCodec::Zstd => {
            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            nodedb_codec::zstd_codec::encode(&raw)
                .map_err(|e| SegmentError::Io(format!("zstd encode: {e}")))
        }
        _ => Ok(nodedb_codec::gorilla::encode_f64(values)),
    }
}

/// Encode raw bytes with a specific codec (for symbol columns).
fn encode_bytes_with_codec(raw: &[u8], codec: ColumnCodec) -> Result<Vec<u8>, SegmentError> {
    match codec {
        ColumnCodec::Raw => Ok(nodedb_codec::raw::encode(raw)),
        ColumnCodec::Lz4 => Ok(nodedb_codec::lz4::encode(raw)),
        ColumnCodec::Zstd => nodedb_codec::zstd_codec::encode(raw)
            .map_err(|e| SegmentError::Io(format!("zstd encode: {e}"))),
        // Symbol columns default to Raw (already dictionary-encoded).
        _ => Ok(nodedb_codec::raw::encode(raw)),
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
        serde_json::from_slice(&data).map_err(|e| SegmentError::Io(format!("parse meta: {e}")))
    }

    /// Read the schema from a partition directory.
    pub fn read_schema(partition_dir: &Path) -> Result<ColumnarSchema, SegmentError> {
        let schema_path = partition_dir.join("schema.json");
        let data = std::fs::read(&schema_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", schema_path.display())))?;
        let json: SchemaJson = serde_json::from_slice(&data)
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

    /// Read the symbol dictionary for a tag column.
    pub fn read_symbol_dict(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<SymbolDictionary, SegmentError> {
        let sym_path = partition_dir.join(format!("{col_name}.sym"));
        let data = std::fs::read(&sym_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", sym_path.display())))?;
        serde_json::from_slice(&data)
            .map_err(|e| SegmentError::Io(format!("parse symbol dict: {e}")))
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

/// Decode a column file using the specified codec.
fn decode_column(
    data: &[u8],
    col_type: ColumnType,
    codec: ColumnCodec,
) -> Result<ColumnData, SegmentError> {
    match (col_type, codec) {
        // --- Timestamp ---
        (ColumnType::Timestamp, ColumnCodec::DoubleDelta) => {
            let values = nodedb_codec::double_delta::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("double_delta: {e}")))?;
            Ok(ColumnData::Timestamp(values))
        }
        (ColumnType::Timestamp, ColumnCodec::Delta) => {
            let values = nodedb_codec::delta::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("delta: {e}")))?;
            Ok(ColumnData::Timestamp(values))
        }
        (ColumnType::Timestamp, ColumnCodec::Gorilla) => {
            let values = nodedb_codec::gorilla::decode_timestamps(data)
                .map_err(|e| SegmentError::Corrupt(format!("gorilla ts: {e}")))?;
            Ok(ColumnData::Timestamp(values))
        }
        (ColumnType::Timestamp, ColumnCodec::Raw) => {
            let raw = nodedb_codec::raw::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("raw: {e}")))?;
            Ok(ColumnData::Timestamp(raw_to_i64(&raw)?))
        }

        // --- Float64 ---
        (ColumnType::Float64, ColumnCodec::Gorilla) => {
            let values = nodedb_codec::gorilla::decode_f64(data)
                .map_err(|e| SegmentError::Corrupt(format!("gorilla f64: {e}")))?;
            Ok(ColumnData::Float64(values))
        }
        (ColumnType::Float64, ColumnCodec::Raw) => {
            let raw = nodedb_codec::raw::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("raw: {e}")))?;
            Ok(ColumnData::Float64(raw_to_f64(&raw)?))
        }

        // --- Int64 ---
        (ColumnType::Int64, ColumnCodec::Delta) => {
            let values = nodedb_codec::delta::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("delta: {e}")))?;
            Ok(ColumnData::Int64(values))
        }
        (ColumnType::Int64, ColumnCodec::DoubleDelta) => {
            let values = nodedb_codec::double_delta::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("double_delta: {e}")))?;
            Ok(ColumnData::Int64(values))
        }
        (ColumnType::Int64, ColumnCodec::Gorilla) => {
            let values = nodedb_codec::gorilla::decode_timestamps(data)
                .map_err(|e| SegmentError::Corrupt(format!("gorilla i64: {e}")))?;
            Ok(ColumnData::Int64(values))
        }
        (ColumnType::Int64, ColumnCodec::Raw) => {
            // Legacy: raw LE i64 without the nodedb-codec Raw header.
            // Try codec-aware decode first; fall back to bare LE bytes.
            match nodedb_codec::raw::decode(data) {
                Ok(raw) => Ok(ColumnData::Int64(raw_to_i64(&raw)?)),
                Err(_) => {
                    // Legacy bare LE bytes (no length header).
                    Ok(ColumnData::Int64(raw_to_i64(data)?))
                }
            }
        }

        // --- Symbol ---
        (ColumnType::Symbol, ColumnCodec::Raw) => {
            // Try codec-aware decode first; fall back to bare LE bytes.
            match nodedb_codec::raw::decode(data) {
                Ok(raw) => Ok(ColumnData::Symbol(raw_to_u32(&raw)?)),
                Err(_) => {
                    // Legacy bare LE bytes (no length header).
                    Ok(ColumnData::Symbol(raw_to_u32(data)?))
                }
            }
        }

        // --- Generic LZ4/Zstd for any column type ---
        (_, ColumnCodec::Lz4) => {
            let raw = nodedb_codec::lz4::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("lz4: {e}")))?;
            raw_to_column_data(&raw, col_type)
        }
        (_, ColumnCodec::Zstd) => {
            let raw = nodedb_codec::zstd_codec::decode(data)
                .map_err(|e| SegmentError::Corrupt(format!("zstd: {e}")))?;
            raw_to_column_data(&raw, col_type)
        }

        // Catch-all: unsupported codec for this column type.
        (_, codec) => Err(SegmentError::Corrupt(format!(
            "unsupported codec {codec} for column type {col_type:?}"
        ))),
    }
}

/// Convert raw LE bytes to i64 values.
fn raw_to_i64(data: &[u8]) -> Result<Vec<i64>, SegmentError> {
    if !data.len().is_multiple_of(8) {
        return Err(SegmentError::Corrupt(
            "i64 data not aligned to 8 bytes".into(),
        ));
    }
    Ok(data
        .chunks_exact(8)
        .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

/// Convert raw LE bytes to f64 values.
fn raw_to_f64(data: &[u8]) -> Result<Vec<f64>, SegmentError> {
    if !data.len().is_multiple_of(8) {
        return Err(SegmentError::Corrupt(
            "f64 data not aligned to 8 bytes".into(),
        ));
    }
    Ok(data
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

/// Convert raw LE bytes to u32 values.
fn raw_to_u32(data: &[u8]) -> Result<Vec<u32>, SegmentError> {
    if !data.len().is_multiple_of(4) {
        return Err(SegmentError::Corrupt(
            "u32 data not aligned to 4 bytes".into(),
        ));
    }
    Ok(data
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

/// Convert raw LE bytes to typed column data.
fn raw_to_column_data(raw: &[u8], col_type: ColumnType) -> Result<ColumnData, SegmentError> {
    match col_type {
        ColumnType::Timestamp => Ok(ColumnData::Timestamp(raw_to_i64(raw)?)),
        ColumnType::Float64 => Ok(ColumnData::Float64(raw_to_f64(raw)?)),
        ColumnType::Int64 => Ok(ColumnData::Int64(raw_to_i64(raw)?)),
        ColumnType::Symbol => Ok(ColumnData::Symbol(raw_to_u32(raw)?)),
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
        assert_eq!(meta.column_stats["host"].codec, ColumnCodec::Raw);
        assert_eq!(meta.column_stats["host"].cardinality, Some(2));

        let part_dir = tmp.path().join("ts-tags");
        let host_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "host",
            ColumnType::Symbol,
            Some(ColumnCodec::Raw),
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
        let json = serde_json::to_vec(&schema_to_json(&schema)).unwrap();
        let parsed: SchemaJson = serde_json::from_slice(&json).unwrap();
        let recovered = schema_from_parsed(&parsed).unwrap();

        assert_eq!(recovered.columns, schema.columns);
        assert_eq!(recovered.timestamp_idx, 0);
        assert_eq!(recovered.codecs, schema.codecs);
    }
}
