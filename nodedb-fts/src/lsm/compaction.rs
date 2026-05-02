//! Level-based compaction for the FTS LSM engine.
//!
//! Uses tiered compaction with configurable levels and segments-per-level.
//! When a level fills (exceeds `max_segments_per_level`), all segments at
//! that level are merged into a single segment at the next level.

use crate::backend::FtsBackend;

use super::merge;
use super::segment::{reader::SegmentReader, writer};

#[cfg(feature = "governor")]
use std::sync::Arc;

#[cfg(feature = "governor")]
use nodedb_mem::{EngineId, MemoryGovernor};

/// Compaction configuration.
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    /// Maximum number of levels.
    pub max_levels: usize,
    /// Maximum segments per level before triggering compaction.
    pub max_segments_per_level: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_levels: 8,
            max_segments_per_level: 8,
        }
    }
}

/// Segment metadata for level tracking.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// Segment id (unscoped, e.g. `"L{level}:{id:016x}"`).
    pub segment_id: String,
    /// Compaction level (0 = freshly flushed from memtable).
    pub level: u32,
    /// Byte size of the segment.
    pub size: u64,
}

/// Check which level needs compaction and return the level number, or None.
pub fn needs_compaction(segments: &[SegmentMeta], config: &CompactionConfig) -> Option<u32> {
    let mut counts = vec![0usize; config.max_levels];
    for seg in segments {
        let level = seg.level as usize;
        if level < config.max_levels {
            counts[level] += 1;
        }
    }

    for (level, &count) in counts.iter().enumerate() {
        if count >= config.max_segments_per_level && level + 1 < config.max_levels {
            return Some(level as u32);
        }
    }
    None
}

/// Result of a compaction: new segment bytes and ids of merged (to-remove) segments.
pub type CompactionResult = (Vec<u8>, Vec<String>);

/// Errors from `compact_level` — wraps the backend error and, when the
/// `governor` feature is enabled, budget exhaustion.
#[derive(Debug)]
pub enum CompactError<E> {
    /// Underlying backend storage error.
    Backend(E),
    /// Memory budget exhausted (only produced when `governor` feature is active).
    #[cfg(feature = "governor")]
    Budget(nodedb_mem::MemError),
}

impl<E: std::fmt::Display> std::fmt::Display for CompactError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactError::Backend(e) => write!(f, "compaction backend error: {e}"),
            #[cfg(feature = "governor")]
            CompactError::Budget(e) => write!(f, "compaction budget exhausted: {e}"),
        }
    }
}

/// Helper for converting a `CompactError` to `Backend` variant.
impl<E> CompactError<E> {
    pub(crate) fn backend(e: E) -> Self {
        CompactError::Backend(e)
    }
}

/// Perform compaction: merge all segments at `level` into one segment at `level + 1`.
///
/// Returns the merged segment bytes and the ids of segments that were merged
/// (which should be removed from storage after the new segment is written).
///
/// When `governor` is `Some`, each `Vec::with_capacity` allocation is budgeted
/// via [`MemoryGovernor::reserve`]. If the budget is exhausted the function
/// returns [`CompactError::Budget`] before allocating.
pub fn compact_level<B: FtsBackend>(
    backend: &B,
    tid: u64,
    collection: &str,
    segments: &[SegmentMeta],
    level: u32,
    #[cfg(feature = "governor")] governor: Option<&Arc<MemoryGovernor>>,
) -> Result<Option<CompactionResult>, CompactError<B::Error>> {
    let to_merge: Vec<&SegmentMeta> = segments.iter().filter(|s| s.level == level).collect();
    if to_merge.len() < 2 {
        return Ok(None);
    }

    #[cfg(feature = "governor")]
    let _readers_guard = if let Some(gov) = governor {
        let bytes = to_merge.len() * std::mem::size_of::<SegmentReader>();
        Some(
            gov.reserve(EngineId::Fts, bytes)
                .map_err(CompactError::Budget)?,
        )
    } else {
        None
    };

    // no-governor: governed by _readers_guard via gov.reserve above; cfg(feature="governor") block outside 5-line window
    let mut readers = Vec::with_capacity(to_merge.len());

    #[cfg(feature = "governor")]
    let _ids_guard = if let Some(gov) = governor {
        let bytes = to_merge.len() * std::mem::size_of::<String>();
        Some(
            gov.reserve(EngineId::Fts, bytes)
                .map_err(CompactError::Budget)?,
        )
    } else {
        None
    };

    // no-governor: governed by _ids_guard via gov.reserve above; cfg(feature="governor") block outside 5-line window
    let mut merged_ids = Vec::with_capacity(to_merge.len());

    for meta in &to_merge {
        if let Some(data) = backend
            .read_segment(tid, collection, &meta.segment_id)
            .map_err(CompactError::backend)?
            && let Ok(reader) = SegmentReader::open(data)
        {
            readers.push(reader);
            merged_ids.push(meta.segment_id.clone());
        }
    }

    if readers.len() < 2 {
        return Ok(None);
    }

    let merged_term_blocks = merge::merge_segments(
        &readers,
        #[cfg(feature = "governor")]
        governor,
    );
    let new_segment = writer::build_from_blocks(&merged_term_blocks)
        .expect("compaction produced a term longer than u16::MAX — data invariant violated");

    Ok(Some((new_segment, merged_ids)))
}

/// Generate a segment id (unscoped).
pub fn segment_id(segment_id: u64, level: u32) -> String {
    format!("L{level}:{segment_id:016x}")
}

/// Parse the level from a segment id. Returns 0 if unparseable.
pub fn parse_level(id: &str) -> u32 {
    // Format: "L{level}:{id}"
    id.strip_prefix('L')
        .and_then(|s| s.split(':').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Parse the segment number from a segment id.
pub fn parse_segment_number(id: &str) -> u64 {
    id.rsplit(':')
        .next()
        .and_then(|s| u64::from_str_radix(s, 16).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(id: &str, level: u32) -> SegmentMeta {
        SegmentMeta {
            segment_id: id.to_string(),
            level,
            size: 1000,
        }
    }

    #[test]
    fn needs_compaction_basic() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 3,
        };

        let segments = vec![make_meta("s1", 0), make_meta("s2", 0), make_meta("s3", 0)];
        assert_eq!(needs_compaction(&segments, &config), Some(0));
    }

    #[test]
    fn no_compaction_under_threshold() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 3,
        };
        let segments = vec![make_meta("s1", 0), make_meta("s2", 0)];
        assert_eq!(needs_compaction(&segments, &config), None);
    }

    #[test]
    fn compaction_at_higher_level() {
        let config = CompactionConfig {
            max_levels: 4,
            max_segments_per_level: 2,
        };
        let segments = vec![make_meta("s1", 0), make_meta("s2", 1), make_meta("s3", 1)];
        assert_eq!(needs_compaction(&segments, &config), Some(1));
    }

    #[test]
    fn segment_id_format() {
        let id = segment_id(42, 0);
        assert!(id.starts_with("L0:"));
        assert_eq!(parse_level(&id), 0);
        assert_eq!(parse_segment_number(&id), 42);
    }

    #[test]
    fn parse_level_and_id() {
        assert_eq!(parse_level("L2:000000000000002a"), 2);
        assert_eq!(parse_segment_number("L2:000000000000002a"), 42);
    }
}
