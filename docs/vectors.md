# Vector Search

NodeDB's vector engine is built for production semantic search — not vectors shoehorned through a SQL planner. It uses a custom HNSW index with multiple quantization levels and hardware-accelerated distance math.

## When to Use

- Semantic search over embeddings (text, images, audio)
- RAG pipelines for AI agents
- Recommendation systems
- Similarity matching at scale

## Key Features

- **HNSW index** — Hierarchical Navigable Small World graph for approximate nearest neighbor search. Construction uses full precision (FP32/FP16) for structural integrity; traversal uses quantized payloads for cache residency.
- **Quantization** — Three levels depending on your recall/memory tradeoff:
  - **SQ8** — Scalar quantization to 8-bit. ~4x memory reduction, minimal recall loss.
  - **PQ** — Product quantization. ~4-8x memory reduction, ~95% recall.
  - **IVF-PQ** — Inverted file with product quantization. ~16 bytes/vector, best for >10M vectors.
- **Adaptive pre-filtering** — Roaring Bitmap-based filtering with automatic strategy selection (pre-filter, post-filter, or brute-force) based on selectivity.
- **Distance metrics** — L2 (Euclidean), cosine similarity, negative inner product.
- **Cross-engine fusion** — Combine vector search with graph traversal ([GraphRAG](graph.md)), full-text BM25 ([hybrid search](full-text-search.md)), or spatial filtering ([geo-aware search](spatial.md)) in a single query.

## Examples

```sql
-- Create a collection with a vector index
CREATE COLLECTION articles TYPE document;
CREATE VECTOR INDEX idx_articles_embedding ON articles METRIC cosine DIM 384;

-- Insert documents with embeddings
INSERT INTO articles {
    title: 'Understanding Transformers',
    embedding: [0.12, -0.34, 0.56, ...]
};

-- Nearest neighbor search
SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2, ...], 10);

-- Filtered vector search (adaptive pre-filtering kicks in)
SELECT title, vector_distance(embedding, ARRAY[0.1, 0.3, -0.2, ...]) AS score
FROM articles
WHERE category = 'machine-learning'
  AND id IN (
    SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2, ...], 10)
  );

-- Hybrid vector + full-text search (RRF fusion)
SELECT title, rrf_score() AS score
FROM articles
WHERE id IN (
    SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, ...], 10)
  )
  AND text_match(body, 'transformer attention mechanism')
LIMIT 10;
```

## Quantization Selection

| Index Type  | Memory per Vector (384d) | Recall  | Best For                   |
| ----------- | ------------------------ | ------- | -------------------------- |
| HNSW (FP32) | ~1.5 KB                  | ~99%    | < 1M vectors, max accuracy |
| HNSW + SQ8  | ~384 B                   | ~98%    | 1-10M vectors              |
| HNSW + PQ   | ~96 B                    | ~95%    | 10-50M vectors             |
| IVF-PQ      | ~16 B                    | ~85-95% | 50M+ vectors               |

## How It Works

Vectors are indexed in the HNSW graph at full precision to maintain structural integrity. During search, the engine traverses quantized copies of the graph for speed, then re-ranks the top candidates against full-precision vectors for accuracy.

When a query includes metadata filters (e.g., `WHERE category = 'ml'`), the engine builds a Roaring Bitmap of matching document IDs and selects the optimal filtering strategy automatically — if the filter is highly selective, it pre-filters before HNSW traversal; if broad, it post-filters after.

## Temporal Vector Search

The Vector engine is an index. It points at records; it does not carry temporal columns itself. To query embeddings at a point in time, attach a Vector index to a Document collection that has `bitemporal = true`. The Document collection holds the embedding payload and temporal columns; the Vector index returns candidate IDs; `AS OF` filtering happens at the Document layer.

See [Bitemporal Queries — Bitemporal Vector Search](bitemporal.md#bitemporal-vector-search) for a worked example.

## Related

- [Graph](graph.md) — GraphRAG combines vector similarity with graph traversal
- [Full-Text Search](full-text-search.md) — Hybrid vector + BM25 fusion
- [Spatial](spatial.md) — Hybrid spatial-vector search
- [Bitemporal Queries](bitemporal.md) — Temporal composition pattern

[Back to docs](README.md)
