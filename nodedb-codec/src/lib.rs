//! Compression codecs for NodeDB timeseries columnar storage.
//!
//! Provides per-column codec selection with **cascading compression**:
//! type-aware encoding (ALP, FastLanes, FSST, Pcodec) followed by a terminal
//! byte compressor (lz4_flex for hot/warm, rANS for cold/S3).
//!
//! Cascading chains (hot/warm — lz4 terminal):
//! - `AlpFastLanesLz4`:   f64 metrics → ALP → FastLanes → lz4
//! - `DeltaFastLanesLz4`: i64 timestamps/counters → Delta → FastLanes → lz4
//! - `FastLanesLz4`:      i64 raw integers → FastLanes → lz4
//! - `FsstLz4`:           strings/logs → FSST → lz4
//! - `PcodecLz4`:         complex numerics → Pcodec → lz4
//! - `AlpRdLz4`:          true doubles → ALP-RD → lz4
//!
//! Cold/S3 tier chains (rANS terminal):
//! - `AlpFastLanesRans`, `DeltaFastLanesRans`, `FsstRans`
//!
//! Shared by Origin and Lite. Compiles to WASM.

pub mod alp;
pub mod alp_rd;
pub mod crdt_compress;
pub mod delta;
pub mod detect;
pub mod double_delta;
pub mod error;
pub mod fastlanes;
pub mod fsst;
pub mod gorilla;
pub mod lz4;
pub mod pcodec;
pub mod pipeline;
pub mod rans;
pub mod raw;
pub mod spherical;
pub mod vector_quant;
pub mod zstd_codec;

/// Number of values to sample for codec auto-detection and exponent selection.
/// Used by ALP, ALP-RD, and the codec detector.
pub const CODEC_SAMPLE_SIZE: usize = 1024;

pub use crdt_compress::CrdtOp;
pub use delta::{DeltaDecoder, DeltaEncoder};
pub use detect::detect_codec;
pub use double_delta::{DoubleDeltaDecoder, DoubleDeltaEncoder};
pub use error::CodecError;
pub use gorilla::{GorillaDecoder, GorillaEncoder};
pub use lz4::{Lz4Decoder, Lz4Encoder};
pub use pipeline::{
    decode_bytes_pipeline, decode_f64_pipeline, decode_i64_pipeline, encode_bytes_pipeline,
    encode_f64_pipeline, encode_i64_pipeline,
};
pub use raw::{RawDecoder, RawEncoder};
pub use zstd_codec::{ZstdDecoder, ZstdEncoder};

use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

/// Codec identifier for per-column compression selection.
///
/// Stored in partition schema metadata so the reader knows which decoder
/// to use for each column file.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ColumnCodec {
    /// Engine selects codec automatically based on column type and data
    /// distribution (analyzed at flush time).
    Auto = 0,

    // -- Cascading chains: hot/warm (lz4 terminal) --
    /// f64 metrics: ALP (decimal→int) → FastLanes → lz4.
    AlpFastLanesLz4 = 1,
    /// f64 true doubles: ALP-RD (front-bit dict) → lz4.
    AlpRdLz4 = 2,
    /// f64/i64 complex: Pcodec → lz4.
    PcodecLz4 = 3,
    /// i64 timestamps/counters: Delta → FastLanes → lz4.
    DeltaFastLanesLz4 = 4,
    /// i64/u32 raw integers: FastLanes → lz4.
    FastLanesLz4 = 5,
    /// Strings/logs: FSST (substring dict) → lz4.
    FsstLz4 = 6,

    // -- Cascading chains: cold/S3 (rANS terminal) --
    /// f64 metrics cold: ALP → FastLanes → rANS.
    AlpFastLanesRans = 7,
    /// i64 cold: Delta → FastLanes → rANS.
    DeltaFastLanesRans = 8,
    /// Strings cold: FSST → rANS.
    FsstRans = 9,

    // -- Legacy single-step codecs (small partitions, backward compat) --
    /// Gorilla XOR encoding — legacy f64 codec.
    Gorilla = 10,
    /// DoubleDelta — legacy timestamp codec.
    DoubleDelta = 11,
    /// Delta + varint — legacy counter codec.
    Delta = 12,
    /// LZ4 block compression — for string/log columns.
    Lz4 = 13,
    /// Zstd — for cold/archived partitions.
    Zstd = 14,
    /// No compression — for pre-compressed or symbol columns.
    Raw = 15,
}

impl ColumnCodec {
    pub fn is_compressed(&self) -> bool {
        !matches!(self, Self::Raw | Self::Auto)
    }

    /// Whether this is a cascading (multi-stage) codec.
    pub fn is_cascading(&self) -> bool {
        matches!(
            self,
            Self::AlpFastLanesLz4
                | Self::AlpRdLz4
                | Self::PcodecLz4
                | Self::DeltaFastLanesLz4
                | Self::FastLanesLz4
                | Self::FsstLz4
                | Self::AlpFastLanesRans
                | Self::DeltaFastLanesRans
                | Self::FsstRans
        )
    }

    /// Whether this codec uses rANS as terminal (cold tier).
    pub fn is_cold_tier(&self) -> bool {
        matches!(
            self,
            Self::AlpFastLanesRans | Self::DeltaFastLanesRans | Self::FsstRans
        )
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::AlpFastLanesLz4 => "alp_fastlanes_lz4",
            Self::AlpRdLz4 => "alp_rd_lz4",
            Self::PcodecLz4 => "pcodec_lz4",
            Self::DeltaFastLanesLz4 => "delta_fastlanes_lz4",
            Self::FastLanesLz4 => "fastlanes_lz4",
            Self::FsstLz4 => "fsst_lz4",
            Self::AlpFastLanesRans => "alp_fastlanes_rans",
            Self::DeltaFastLanesRans => "delta_fastlanes_rans",
            Self::FsstRans => "fsst_rans",
            Self::Gorilla => "gorilla",
            Self::DoubleDelta => "double_delta",
            Self::Delta => "delta",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Raw => "raw",
        }
    }
}

impl ColumnCodec {
    /// Resolve `Auto` to a concrete codec using the provided detection result,
    /// or return an error if this is called with `Auto` where a concrete value
    /// is required (i.e. a caller forgot to run detection first).
    ///
    /// For callers that have already run detection and hold a non-`Auto`
    /// codec, this is a zero-cost newtype wrap.
    pub fn try_resolve(self) -> Result<ResolvedColumnCodec, CodecError> {
        match self {
            Self::Auto => Err(CodecError::UnresolvedAuto),
            Self::AlpFastLanesLz4 => Ok(ResolvedColumnCodec::AlpFastLanesLz4),
            Self::AlpRdLz4 => Ok(ResolvedColumnCodec::AlpRdLz4),
            Self::PcodecLz4 => Ok(ResolvedColumnCodec::PcodecLz4),
            Self::DeltaFastLanesLz4 => Ok(ResolvedColumnCodec::DeltaFastLanesLz4),
            Self::FastLanesLz4 => Ok(ResolvedColumnCodec::FastLanesLz4),
            Self::FsstLz4 => Ok(ResolvedColumnCodec::FsstLz4),
            Self::AlpFastLanesRans => Ok(ResolvedColumnCodec::AlpFastLanesRans),
            Self::DeltaFastLanesRans => Ok(ResolvedColumnCodec::DeltaFastLanesRans),
            Self::FsstRans => Ok(ResolvedColumnCodec::FsstRans),
            Self::Gorilla => Ok(ResolvedColumnCodec::Gorilla),
            Self::DoubleDelta => Ok(ResolvedColumnCodec::DoubleDelta),
            Self::Delta => Ok(ResolvedColumnCodec::Delta),
            Self::Lz4 => Ok(ResolvedColumnCodec::Lz4),
            Self::Zstd => Ok(ResolvedColumnCodec::Zstd),
            Self::Raw => Ok(ResolvedColumnCodec::Raw),
        }
    }
}

impl std::fmt::Display for ColumnCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A `ColumnCodec` that has been resolved away from `Auto`.
///
/// Invariant: this type can never hold the `Auto` variant. All on-disk
/// column headers (`ColumnMeta.codec`) and per-column statistics
/// (`ColumnStatistics.codec`) use `ResolvedColumnCodec`, making it a
/// compile-time guarantee that `Auto` never survives to disk.
///
/// The `#[repr(u8)]` discriminants are **identical** to the corresponding
/// `ColumnCodec` discriminants so that on-disk byte values are unchanged.
/// `Auto` (discriminant 0) is intentionally absent.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ResolvedColumnCodec {
    AlpFastLanesLz4 = 1,
    AlpRdLz4 = 2,
    PcodecLz4 = 3,
    DeltaFastLanesLz4 = 4,
    FastLanesLz4 = 5,
    FsstLz4 = 6,
    AlpFastLanesRans = 7,
    DeltaFastLanesRans = 8,
    FsstRans = 9,
    Gorilla = 10,
    DoubleDelta = 11,
    Delta = 12,
    Lz4 = 13,
    Zstd = 14,
    Raw = 15,
}

impl ResolvedColumnCodec {
    /// Convert back to `ColumnCodec` for use with codec pipelines that
    /// accept the full enum (e.g. `encode_i64_pipeline`, `decode_f64_pipeline`).
    pub fn into_column_codec(self) -> ColumnCodec {
        match self {
            Self::AlpFastLanesLz4 => ColumnCodec::AlpFastLanesLz4,
            Self::AlpRdLz4 => ColumnCodec::AlpRdLz4,
            Self::PcodecLz4 => ColumnCodec::PcodecLz4,
            Self::DeltaFastLanesLz4 => ColumnCodec::DeltaFastLanesLz4,
            Self::FastLanesLz4 => ColumnCodec::FastLanesLz4,
            Self::FsstLz4 => ColumnCodec::FsstLz4,
            Self::AlpFastLanesRans => ColumnCodec::AlpFastLanesRans,
            Self::DeltaFastLanesRans => ColumnCodec::DeltaFastLanesRans,
            Self::FsstRans => ColumnCodec::FsstRans,
            Self::Gorilla => ColumnCodec::Gorilla,
            Self::DoubleDelta => ColumnCodec::DoubleDelta,
            Self::Delta => ColumnCodec::Delta,
            Self::Lz4 => ColumnCodec::Lz4,
            Self::Zstd => ColumnCodec::Zstd,
            Self::Raw => ColumnCodec::Raw,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlpFastLanesLz4 => "alp_fastlanes_lz4",
            Self::AlpRdLz4 => "alp_rd_lz4",
            Self::PcodecLz4 => "pcodec_lz4",
            Self::DeltaFastLanesLz4 => "delta_fastlanes_lz4",
            Self::FastLanesLz4 => "fastlanes_lz4",
            Self::FsstLz4 => "fsst_lz4",
            Self::AlpFastLanesRans => "alp_fastlanes_rans",
            Self::DeltaFastLanesRans => "delta_fastlanes_rans",
            Self::FsstRans => "fsst_rans",
            Self::Gorilla => "gorilla",
            Self::DoubleDelta => "double_delta",
            Self::Delta => "delta",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Raw => "raw",
        }
    }
}

impl std::fmt::Display for ResolvedColumnCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Column data type hint for codec auto-detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnTypeHint {
    Timestamp,
    Float64,
    Int64,
    Symbol,
    String,
}

/// Per-column statistics computed at flush time.
///
/// Stored in partition metadata for predicate pushdown and approximate
/// query answers without decompression.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct ColumnStatistics {
    /// Codec used for this column in this partition.
    ///
    /// Always a concrete, resolved codec — never `Auto`.
    pub codec: ResolvedColumnCodec,
    /// Number of non-null values.
    pub count: u64,
    /// Minimum value (as f64 for numeric columns, 0.0 for non-numeric).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Maximum value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Sum of values (for numeric columns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum: Option<f64>,
    /// Number of distinct values (for symbol/tag columns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<u32>,
    /// Compressed size in bytes for this column.
    pub compressed_bytes: u64,
    /// Uncompressed size in bytes.
    pub uncompressed_bytes: u64,
}

impl ColumnStatistics {
    /// Create empty statistics with just the codec.
    pub fn new(codec: ResolvedColumnCodec) -> Self {
        Self {
            codec,
            count: 0,
            min: None,
            max: None,
            sum: None,
            cardinality: None,
            compressed_bytes: 0,
            uncompressed_bytes: 0,
        }
    }

    /// Compute statistics for an i64 column.
    pub fn from_i64(values: &[i64], codec: ResolvedColumnCodec, compressed_bytes: u64) -> Self {
        if values.is_empty() {
            return Self::new(codec);
        }

        let mut min = values[0];
        let mut max = values[0];
        let mut sum: i128 = 0;

        for &v in values {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
            sum += v as i128;
        }

        Self {
            codec,
            count: values.len() as u64,
            min: Some(min as f64),
            max: Some(max as f64),
            sum: Some(sum as f64),
            cardinality: None,
            compressed_bytes,
            uncompressed_bytes: (values.len() * 8) as u64,
        }
    }

    /// Compute statistics for an f64 column.
    pub fn from_f64(values: &[f64], codec: ResolvedColumnCodec, compressed_bytes: u64) -> Self {
        if values.is_empty() {
            return Self::new(codec);
        }

        let mut min = values[0];
        let mut max = values[0];
        let mut sum: f64 = 0.0;

        for &v in values {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
            sum += v;
        }

        Self {
            codec,
            count: values.len() as u64,
            min: Some(min),
            max: Some(max),
            sum: Some(sum),
            cardinality: None,
            compressed_bytes,
            uncompressed_bytes: (values.len() * 8) as u64,
        }
    }

    /// Compute statistics for a symbol column.
    pub fn from_symbols(
        values: &[u32],
        cardinality: u32,
        codec: ResolvedColumnCodec,
        compressed_bytes: u64,
    ) -> Self {
        Self {
            codec,
            count: values.len() as u64,
            min: None,
            max: None,
            sum: None,
            cardinality: Some(cardinality),
            compressed_bytes,
            uncompressed_bytes: (values.len() * 4) as u64,
        }
    }

    /// Compression ratio (uncompressed / compressed). Returns 1.0 if no data.
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            return 1.0;
        }
        self.uncompressed_bytes as f64 / self.compressed_bytes as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frozen canonical codec name surface. Locks the lowercase, snake_case
    /// forms before any user DDL exposes them. Adding a codec means appending
    /// here and to `as_str()`; renaming any existing entry is a wire break.
    #[test]
    fn canonical_codec_names_frozen() {
        let canonical: &[(ColumnCodec, &str)] = &[
            (ColumnCodec::Auto, "auto"),
            (ColumnCodec::AlpFastLanesLz4, "alp_fastlanes_lz4"),
            (ColumnCodec::AlpRdLz4, "alp_rd_lz4"),
            (ColumnCodec::PcodecLz4, "pcodec_lz4"),
            (ColumnCodec::DeltaFastLanesLz4, "delta_fastlanes_lz4"),
            (ColumnCodec::FastLanesLz4, "fastlanes_lz4"),
            (ColumnCodec::FsstLz4, "fsst_lz4"),
            (ColumnCodec::AlpFastLanesRans, "alp_fastlanes_rans"),
            (ColumnCodec::DeltaFastLanesRans, "delta_fastlanes_rans"),
            (ColumnCodec::FsstRans, "fsst_rans"),
            (ColumnCodec::Gorilla, "gorilla"),
            (ColumnCodec::DoubleDelta, "double_delta"),
            (ColumnCodec::Delta, "delta"),
            (ColumnCodec::Lz4, "lz4"),
            (ColumnCodec::Zstd, "zstd"),
            (ColumnCodec::Raw, "raw"),
        ];
        for (codec, expected) in canonical {
            assert_eq!(codec.as_str(), *expected, "codec name drift: {codec:?}");
            assert!(
                expected
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "codec name '{expected}' is not lowercase snake_case"
            );
        }
    }

    // ── ResolvedColumnCodec tests ──────────────────────────────────────────────

    /// Discriminants of ResolvedColumnCodec must exactly match those of the
    /// corresponding ColumnCodec variants so on-disk byte values are unchanged.
    #[test]
    fn resolved_codec_discriminants_match_column_codec() {
        // Verify that the byte values written to disk by ResolvedColumnCodec
        // are the same as those that would be written by ColumnCodec.
        // We test all 15 concrete variants.
        let pairs: &[(ResolvedColumnCodec, ColumnCodec)] = &[
            (
                ResolvedColumnCodec::AlpFastLanesLz4,
                ColumnCodec::AlpFastLanesLz4,
            ),
            (ResolvedColumnCodec::AlpRdLz4, ColumnCodec::AlpRdLz4),
            (ResolvedColumnCodec::PcodecLz4, ColumnCodec::PcodecLz4),
            (
                ResolvedColumnCodec::DeltaFastLanesLz4,
                ColumnCodec::DeltaFastLanesLz4,
            ),
            (ResolvedColumnCodec::FastLanesLz4, ColumnCodec::FastLanesLz4),
            (ResolvedColumnCodec::FsstLz4, ColumnCodec::FsstLz4),
            (
                ResolvedColumnCodec::AlpFastLanesRans,
                ColumnCodec::AlpFastLanesRans,
            ),
            (
                ResolvedColumnCodec::DeltaFastLanesRans,
                ColumnCodec::DeltaFastLanesRans,
            ),
            (ResolvedColumnCodec::FsstRans, ColumnCodec::FsstRans),
            (ResolvedColumnCodec::Gorilla, ColumnCodec::Gorilla),
            (ResolvedColumnCodec::DoubleDelta, ColumnCodec::DoubleDelta),
            (ResolvedColumnCodec::Delta, ColumnCodec::Delta),
            (ResolvedColumnCodec::Lz4, ColumnCodec::Lz4),
            (ResolvedColumnCodec::Zstd, ColumnCodec::Zstd),
            (ResolvedColumnCodec::Raw, ColumnCodec::Raw),
        ];

        for &(resolved, column) in pairs {
            // Serialize both through msgpack (the on-disk format) and check bytes match.
            let resolved_bytes = zerompk::to_msgpack_vec(&resolved).unwrap();
            let column_bytes = zerompk::to_msgpack_vec(&column).unwrap();
            assert_eq!(
                resolved_bytes, column_bytes,
                "discriminant mismatch for {resolved} vs {column}"
            );

            // Also verify round-trip via into_column_codec.
            assert_eq!(
                resolved.into_column_codec(),
                column,
                "into_column_codec mismatch for {resolved}"
            );
        }
    }

    /// Auto resolves to an error; all concrete variants resolve successfully.
    #[test]
    fn try_resolve_auto_returns_error() {
        assert!(
            matches!(
                ColumnCodec::Auto.try_resolve(),
                Err(crate::error::CodecError::UnresolvedAuto)
            ),
            "Auto.try_resolve() must return UnresolvedAuto error"
        );
    }

    #[test]
    fn try_resolve_concrete_succeeds() {
        let concretes = [
            ColumnCodec::AlpFastLanesLz4,
            ColumnCodec::Gorilla,
            ColumnCodec::Delta,
            ColumnCodec::Raw,
            ColumnCodec::Lz4,
        ];
        for codec in concretes {
            assert!(
                codec.try_resolve().is_ok(),
                "{codec} should resolve successfully"
            );
        }
    }

    #[test]
    fn resolved_codec_serde_roundtrip() {
        for codec in [
            ResolvedColumnCodec::AlpFastLanesLz4,
            ResolvedColumnCodec::AlpRdLz4,
            ResolvedColumnCodec::PcodecLz4,
            ResolvedColumnCodec::DeltaFastLanesLz4,
            ResolvedColumnCodec::FastLanesLz4,
            ResolvedColumnCodec::FsstLz4,
            ResolvedColumnCodec::AlpFastLanesRans,
            ResolvedColumnCodec::DeltaFastLanesRans,
            ResolvedColumnCodec::FsstRans,
            ResolvedColumnCodec::Gorilla,
            ResolvedColumnCodec::DoubleDelta,
            ResolvedColumnCodec::Delta,
            ResolvedColumnCodec::Lz4,
            ResolvedColumnCodec::Zstd,
            ResolvedColumnCodec::Raw,
        ] {
            let json = sonic_rs::to_string(&codec).unwrap();
            let back: ResolvedColumnCodec = sonic_rs::from_str(&json).unwrap();
            assert_eq!(codec, back, "serde roundtrip failed for {codec}");
        }
    }

    #[test]
    fn column_codec_serde_roundtrip() {
        for codec in [
            ColumnCodec::Auto,
            ColumnCodec::AlpFastLanesLz4,
            ColumnCodec::AlpRdLz4,
            ColumnCodec::PcodecLz4,
            ColumnCodec::DeltaFastLanesLz4,
            ColumnCodec::FastLanesLz4,
            ColumnCodec::FsstLz4,
            ColumnCodec::AlpFastLanesRans,
            ColumnCodec::DeltaFastLanesRans,
            ColumnCodec::FsstRans,
            ColumnCodec::Gorilla,
            ColumnCodec::DoubleDelta,
            ColumnCodec::Delta,
            ColumnCodec::Lz4,
            ColumnCodec::Zstd,
            ColumnCodec::Raw,
        ] {
            let json = sonic_rs::to_string(&codec).unwrap();
            let back: ColumnCodec = sonic_rs::from_str(&json).unwrap();
            assert_eq!(codec, back, "serde roundtrip failed for {codec}");
        }
    }

    #[test]
    fn column_statistics_i64() {
        let values = vec![10i64, 20, 30, 40, 50];
        let stats = ColumnStatistics::from_i64(&values, ResolvedColumnCodec::Delta, 12);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.min, Some(10.0));
        assert_eq!(stats.max, Some(50.0));
        assert_eq!(stats.sum, Some(150.0));
        assert_eq!(stats.uncompressed_bytes, 40);
        assert_eq!(stats.compressed_bytes, 12);
    }

    #[test]
    fn column_statistics_f64() {
        let values = vec![1.5f64, 2.5, 3.5];
        let stats = ColumnStatistics::from_f64(&values, ResolvedColumnCodec::Gorilla, 8);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min, Some(1.5));
        assert_eq!(stats.max, Some(3.5));
        assert_eq!(stats.sum, Some(7.5));
    }

    #[test]
    fn column_statistics_symbols() {
        let values = vec![0u32, 1, 2, 0, 1];
        let stats = ColumnStatistics::from_symbols(&values, 3, ResolvedColumnCodec::Raw, 20);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.cardinality, Some(3));
        assert!(stats.min.is_none());
    }

    #[test]
    fn compression_ratio_calculation() {
        let stats = ColumnStatistics {
            codec: ResolvedColumnCodec::Delta,
            count: 100,
            min: None,
            max: None,
            sum: None,
            cardinality: None,
            compressed_bytes: 200,
            uncompressed_bytes: 800,
        };
        assert!((stats.compression_ratio() - 4.0).abs() < f64::EPSILON);
    }
}
