//! Multi-segment compaction: merge multiple sources into a single new segment.

use std::sync::Arc;

use nodedb_mem::{EngineId, MemoryGovernor};
use nodedb_types::columnar::ColumnarSchema;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;
use crate::reader::SegmentReader;
use crate::writer::SegmentWriter;

use super::extract::extract_row_value;
use super::segment::CompactionResult;

/// Compact multiple segments into a single merged segment.
///
/// Reads all source segments, skips deleted rows from each, and writes
/// a single merged output segment. This reduces segment count and reclaims
/// space from deleted rows across all sources.
///
/// When `kek` is `Some`, the merged output segment is AES-256-GCM encrypted.
/// Input segments must be pre-decrypted plaintext.
///
/// `governor` is optional: when `Some`, working-buffer allocations are
/// tracked against the `Columnar` engine budget. Pass `None` in embedded
/// (Lite) deployments where no governor is configured.
pub fn compact_segments(
    segments: &[(&[u8], &DeleteBitmap)],
    schema: &ColumnarSchema,
    profile_tag: u8,
    governor: Option<&Arc<MemoryGovernor>>,
    #[cfg(feature = "encryption")] kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    #[cfg(not(feature = "encryption"))] _kek: Option<&[u8; 32]>,
) -> Result<CompactionResult, ColumnarError> {
    let mut memtable = ColumnarMemtable::new(schema);
    let mut total_removed = 0usize;
    let col_len = schema.columns.len();
    let _row_guard = governor
        .map(|g| {
            g.reserve(
                EngineId::Columnar,
                col_len * std::mem::size_of::<usize>() * 3,
            )
        })
        .transpose()?;
    // no-governor: governed by _row_guard above; multi-line reserve call splits outside 5-line gate window
    let mut row_values = Vec::with_capacity(col_len);

    for &(segment_data, deletes) in segments {
        let reader = SegmentReader::open(segment_data)?;
        let total_rows = reader.row_count() as usize;

        let col_count = reader.column_count();
        let _cols_guard = governor
            .map(|g| {
                g.reserve(
                    EngineId::Columnar,
                    col_count * std::mem::size_of::<usize>() * 3,
                )
            })
            .transpose()?;
        // no-governor: governed by _cols_guard above; multi-line reserve call splits outside 5-line gate window
        let mut decoded_cols = Vec::with_capacity(col_count);
        for i in 0..col_count {
            decoded_cols.push(reader.read_column(i)?);
        }

        for row_idx in 0..total_rows {
            if deletes.is_deleted(row_idx as u32) {
                total_removed += 1;
                continue;
            }

            row_values.clear();
            for (col_idx, decoded) in decoded_cols.iter().enumerate() {
                let value =
                    extract_row_value(decoded, row_idx, &schema.columns[col_idx].column_type);
                row_values.push(value);
            }

            memtable.append_row(&row_values)?;
        }
    }

    let live_rows = memtable.row_count();
    if live_rows == 0 {
        return Ok(CompactionResult {
            segment: None,
            live_rows: 0,
            removed_rows: total_removed,
        });
    }

    let (schema, columns, row_count) = memtable.drain();
    let writer = match governor {
        Some(g) => SegmentWriter::with_governor(profile_tag, Arc::clone(g)),
        None => SegmentWriter::new(profile_tag),
    };
    #[cfg(feature = "encryption")]
    let new_segment = writer.write_segment(&schema, &columns, row_count, kek)?;
    #[cfg(not(feature = "encryption"))]
    let new_segment = writer.write_segment(&schema, &columns, row_count, None)?;

    Ok(CompactionResult {
        segment: Some(new_segment),
        live_rows: row_count,
        removed_rows: total_removed,
    })
}
