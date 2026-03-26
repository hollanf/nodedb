//! Self-decoding partition metadata (AnyBlox pattern).
//!
//! When Origin upgrades its codec pipeline, edge Lite devices with older
//! binaries can't read newly synced partitions. The AnyBlox pattern solves
//! this by embedding a WASM decoder identifier in the partition metadata.
//!
//! The reader checks for embedded decoder info:
//! - If present and the codec is unknown to the reader, it can fetch/use
//!   the WASM decoder module to decode the data.
//! - If absent, falls back to native codec dispatch.
//!
//! This module provides the metadata types and resolution logic. The actual
//! WASM execution is handled by the host runtime (Wasmtime on native,
//! browser WASM engine on web).

use serde::{Deserialize, Serialize};

use nodedb_codec::ColumnCodec;

/// Embedded decoder metadata for a partition.
///
/// Stored in `partition.meta` alongside column stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddedDecoder {
    /// Identifier for the WASM module (e.g., "nodedb-codec-v2-alp-fastlanes-lz4").
    pub module_id: String,
    /// SHA-256 hash of the WASM module bytes (for integrity verification).
    pub module_hash: String,
    /// Size of the WASM module in bytes.
    pub module_size: u64,
    /// Codec chain this decoder handles.
    pub codec: ColumnCodec,
    /// Minimum nodedb-codec version that can decode natively (without WASM).
    /// If the reader's version >= this, WASM is not needed.
    pub min_native_version: String,
}

/// Resolution result: how to decode a partition.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeStrategy {
    /// Native codec dispatch — reader knows this codec.
    Native,
    /// Need WASM decoder — codec is unknown to the reader.
    NeedWasm(String), // module_id
    /// Unknown and no decoder info — cannot decode.
    Unsupported,
}

/// Resolve how to decode a column given its codec and optional decoder metadata.
///
/// `known_codecs` is the set of codecs the current binary supports.
pub fn resolve_decode_strategy(
    codec: ColumnCodec,
    decoder: Option<&EmbeddedDecoder>,
    known_codecs: &[ColumnCodec],
) -> DecodeStrategy {
    // Check if the native reader supports this codec.
    if known_codecs.contains(&codec) {
        return DecodeStrategy::Native;
    }

    // Unknown codec — check for embedded decoder.
    match decoder {
        Some(d) => DecodeStrategy::NeedWasm(d.module_id.clone()),
        None => DecodeStrategy::Unsupported,
    }
}

/// List all codecs supported by the current nodedb-codec build.
pub fn supported_codecs() -> Vec<ColumnCodec> {
    vec![
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
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_codec_resolves() {
        let known = supported_codecs();
        assert_eq!(
            resolve_decode_strategy(ColumnCodec::AlpFastLanesLz4, None, &known),
            DecodeStrategy::Native
        );
    }

    #[test]
    fn unknown_codec_with_decoder() {
        // Simulate a future codec unknown to this binary.
        let decoder = EmbeddedDecoder {
            module_id: "nodedb-codec-v3-future".into(),
            module_hash: "abc123".into(),
            module_size: 4096,
            codec: ColumnCodec::Raw, // placeholder
            min_native_version: "99.0.0".into(),
        };
        // Empty known list → codec is "unknown".
        assert_eq!(
            resolve_decode_strategy(ColumnCodec::Raw, Some(&decoder), &[]),
            DecodeStrategy::NeedWasm("nodedb-codec-v3-future".into())
        );
    }

    #[test]
    fn unknown_codec_no_decoder() {
        assert_eq!(
            resolve_decode_strategy(ColumnCodec::Raw, None, &[]),
            DecodeStrategy::Unsupported
        );
    }

    #[test]
    fn supported_codecs_complete() {
        let codecs = supported_codecs();
        assert!(codecs.len() >= 16);
        assert!(codecs.contains(&ColumnCodec::AlpFastLanesLz4));
        assert!(codecs.contains(&ColumnCodec::FsstRans));
    }
}
