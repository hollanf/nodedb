//! Per-collection codec selection. Wraps the generic `HnswCodecIndex<C>`
//! for codecs other than Sq8 (which retains its specialised fast path
//! in `quantize.rs` / `search.rs`).

use nodedb_codec::vector_quant::bbq::BbqCodec;
use nodedb_codec::vector_quant::rabitq::RaBitQCodec;

use crate::codec_index::HnswCodecIndex;

/// One built codec-index per collection (other than Sq8). Variants match
/// the publicly-selectable quantization choices that route through
/// `HnswCodecIndex`.
pub enum CollectionCodec {
    RaBitQ(HnswCodecIndex<RaBitQCodec>),
    Bbq(HnswCodecIndex<BbqCodec>),
}

impl CollectionCodec {
    /// Forwarding `search` so the collection layer doesn't have to match
    /// on the variant for the common case.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Vec<crate::codec_index::CodecSearchResult> {
        match self {
            Self::RaBitQ(idx) => idx.search(query, k, ef_search),
            Self::Bbq(idx) => idx.search(query, k, ef_search),
        }
    }

    /// Forwarding `insert`.
    pub fn insert(&mut self, id: u32, v: &[f32]) {
        match self {
            Self::RaBitQ(idx) => idx.insert(id, v),
            Self::Bbq(idx) => idx.insert(id, v),
        }
    }

    /// Total nodes (including deleted).
    pub fn len(&self) -> usize {
        match self {
            Self::RaBitQ(idx) => idx.len(),
            Self::Bbq(idx) => idx.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Quantization tag for stats reporting.
    pub fn quantization(&self) -> &'static str {
        match self {
            Self::RaBitQ(_) => "rabitq",
            Self::Bbq(_) => "bbq",
        }
    }
}

/// Build a `CollectionCodec` from a quantization tag and training vectors.
///
/// Returns `None` for unsupported or unrecognised tags (e.g. "sq8", "pq",
/// "none") — those variants use separate per-segment code paths.
pub fn build_collection_codec(
    quantization: &str,
    vectors: &[Vec<f32>],
    dim: usize,
    m: usize,
    ef_construction: usize,
    seed: u64,
) -> Option<CollectionCodec> {
    if vectors.is_empty() {
        return None;
    }
    let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();
    match quantization {
        "rabitq" => {
            let codec = RaBitQCodec::calibrate(&refs, dim, seed);
            let mut idx = HnswCodecIndex::new(dim, m, ef_construction, codec, seed);
            for (i, v) in vectors.iter().enumerate() {
                idx.insert(i as u32, v);
            }
            Some(CollectionCodec::RaBitQ(idx))
        }
        "bbq" => {
            let codec = BbqCodec::calibrate(&refs, dim, 3);
            let mut idx = HnswCodecIndex::new(dim, m, ef_construction, codec, seed);
            for (i, v) in vectors.iter().enumerate() {
                idx.insert(i as u32, v);
            }
            Some(CollectionCodec::Bbq(idx))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| (0..dim).map(|d| (i * dim + d) as f32 * 0.01).collect())
            .collect()
    }

    #[test]
    fn build_rabitq_returns_some() {
        let vecs = make_vectors(50, 8);
        let result = build_collection_codec("rabitq", &vecs, 8, 16, 100, 42);
        assert!(
            matches!(result, Some(CollectionCodec::RaBitQ(_))),
            "expected RaBitQ variant"
        );
    }

    #[test]
    fn build_bbq_returns_some() {
        let vecs = make_vectors(50, 8);
        let result = build_collection_codec("bbq", &vecs, 8, 16, 100, 42);
        assert!(
            matches!(result, Some(CollectionCodec::Bbq(_))),
            "expected Bbq variant"
        );
    }

    #[test]
    fn unknown_codec_returns_none() {
        let vecs = make_vectors(50, 8);
        let result = build_collection_codec("unknown_codec", &vecs, 8, 16, 100, 42);
        assert!(result.is_none(), "unknown codec should return None");
    }

    #[test]
    fn sq8_tag_returns_none() {
        let vecs = make_vectors(50, 8);
        let result = build_collection_codec("sq8", &vecs, 8, 16, 100, 42);
        assert!(
            result.is_none(),
            "sq8 tag should fall through to per-segment path"
        );
    }

    #[test]
    fn empty_vectors_returns_none() {
        let result = build_collection_codec("rabitq", &[], 8, 16, 100, 42);
        assert!(result.is_none(), "empty vectors should return None");
    }

    #[test]
    fn len_and_is_empty() {
        let vecs = make_vectors(20, 4);
        let codec = build_collection_codec("bbq", &vecs, 4, 8, 50, 1).unwrap();
        assert_eq!(codec.len(), 20);
        assert!(!codec.is_empty());
    }

    #[test]
    fn quantization_tag() {
        let vecs = make_vectors(10, 4);
        let rabitq = build_collection_codec("rabitq", &vecs, 4, 8, 50, 1).unwrap();
        assert_eq!(rabitq.quantization(), "rabitq");
        let bbq = build_collection_codec("bbq", &vecs, 4, 8, 50, 1).unwrap();
        assert_eq!(bbq.quantization(), "bbq");
    }
}
