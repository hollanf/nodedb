//! VectorCollection search: multi-segment merging with SQ8 reranking.

use crate::distance::{DistanceMetric, distance};
use crate::hnsw::SearchResult;

use super::lifecycle::VectorCollection;

impl VectorCollection {
    /// Search across all segments, merging results by distance.
    pub fn search(&self, query: &[f32], top_k: usize, ef: usize) -> Vec<SearchResult> {
        let mut all: Vec<SearchResult> = Vec::new();

        // Search growing segment (brute-force).
        let growing_results = self.growing.search(query, top_k);
        for mut r in growing_results {
            r.id += self.growing_base_id;
            all.push(r);
        }

        // Search sealed segments.
        for seg in &self.sealed {
            let results = if let Some((codec, sq8_data)) = &seg.sq8 {
                // Quantized two-phase search.
                let rerank_k = top_k.saturating_mul(3).max(20);
                let mut candidates: Vec<(u32, f32)> = Vec::with_capacity(seg.index.len());
                let dim = seg.index.dim();
                for i in 0..seg.index.len() {
                    if seg.index.is_deleted(i as u32) {
                        continue;
                    }
                    let sq8_vec = &sq8_data[i * dim..(i + 1) * dim];
                    let d = match self.params.metric {
                        DistanceMetric::L2 => codec.asymmetric_l2(query, sq8_vec),
                        DistanceMetric::Cosine => codec.asymmetric_cosine(query, sq8_vec),
                        DistanceMetric::InnerProduct => codec.asymmetric_ip(query, sq8_vec),
                        _ => {
                            let dequant = codec.dequantize(sq8_vec);
                            distance(query, &dequant, self.params.metric)
                        }
                    };
                    candidates.push((i as u32, d));
                }
                if candidates.len() > rerank_k {
                    candidates.select_nth_unstable_by(rerank_k, |a, b| {
                        a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    candidates.truncate(rerank_k);
                }

                // Prefetch FP32 vectors for reranking candidates.
                if let Some(mmap) = &seg.mmap_vectors {
                    let ids: Vec<u32> = candidates.iter().map(|&(id, _)| id).collect();
                    mmap.prefetch_batch(&ids);
                }

                // Phase 2: Rerank with exact FP32 distance.
                let mut reranked: Vec<SearchResult> = candidates
                    .iter()
                    .filter_map(|&(id, _)| {
                        let v = if let Some(mmap) = &seg.mmap_vectors {
                            mmap.get_vector(id)?
                        } else {
                            seg.index.get_vector(id)?
                        };
                        Some(SearchResult {
                            id,
                            distance: distance(query, v, self.params.metric),
                        })
                    })
                    .collect();
                reranked.sort_by(|a, b| {
                    a.distance
                        .partial_cmp(&b.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                reranked.truncate(top_k);
                reranked
            } else {
                seg.index.search(query, top_k, ef)
            };
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        // Search building segments (brute-force while HNSW builds).
        for seg in &self.building {
            let results = seg.flat.search(query, top_k);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        all.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(top_k);
        all
    }

    /// Search with a pre-filter bitmap (byte-array format).
    pub fn search_with_bitmap_bytes(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        bitmap: &[u8],
    ) -> Vec<SearchResult> {
        let mut all: Vec<SearchResult> = Vec::new();

        let growing_results = self.growing.search_filtered(query, top_k, bitmap);
        for mut r in growing_results {
            r.id += self.growing_base_id;
            all.push(r);
        }

        for seg in &self.sealed {
            let results = seg.index.search_with_bitmap_bytes(query, top_k, ef, bitmap);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        for seg in &self.building {
            let results = seg.flat.search_filtered(query, top_k, bitmap);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        all.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(top_k);
        all
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::lifecycle::VectorCollection;
    use crate::collection::segment::DEFAULT_SEAL_THRESHOLD;
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn make_collection() -> VectorCollection {
        VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        )
    }

    #[test]
    fn insert_and_search() {
        let mut coll = make_collection();
        for i in 0..100u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]);
        }
        assert_eq!(coll.len(), 100);
        let results = coll.search(&[50.0, 0.0, 0.0], 3, 64);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 50);
    }

    #[test]
    fn seal_moves_to_building() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..DEFAULT_SEAL_THRESHOLD {
            coll.insert(vec![i as f32, 0.0]);
        }
        assert!(coll.needs_seal());

        let req = coll.seal("test_key").unwrap();
        assert_eq!(req.vectors.len(), DEFAULT_SEAL_THRESHOLD);
        assert_eq!(coll.building.len(), 1);
        assert_eq!(coll.growing.len(), 0);

        let results = coll.search(&[100.0, 0.0], 1, 64);
        assert!(!results.is_empty());
    }

    #[test]
    fn complete_build_promotes_to_sealed() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("test").unwrap();

        let mut index = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            index.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, index);

        assert_eq!(coll.building.len(), 0);
        assert_eq!(coll.sealed.len(), 1);

        let results = coll.search(&[50.0, 0.0], 3, 64);
        assert!(!results.is_empty());
    }

    #[test]
    fn multi_segment_search_merges() {
        let mut coll = VectorCollection::new(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );

        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("test").unwrap();
        let mut idx = HnswIndex::new(2, req.params);
        for v in &req.vectors {
            idx.insert(v.clone()).unwrap();
        }
        coll.complete_build(req.segment_id, idx);

        for i in 100..200 {
            coll.insert(vec![i as f32, 0.0]);
        }

        let results = coll.search(&[150.0, 0.0], 3, 64);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 150);
    }

    #[test]
    fn delete_across_segments() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..10 {
            coll.insert(vec![i as f32, 0.0]);
        }
        assert!(coll.delete(5));
        assert_eq!(coll.live_count(), 9);

        let results = coll.search(&[5.0, 0.0], 10, 64);
        assert!(results.iter().all(|r| r.id != 5));
    }
}
