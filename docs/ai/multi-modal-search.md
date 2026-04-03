# Multi-Modal Search

NodeDB supports multiple vector columns per collection, each with its own HNSW index and distance metric. This lets you store text, image, audio, and video embeddings for the same document and search across modalities — or fuse them with RRF for multi-modal retrieval.

## Multiple Vector Columns

Each vector column gets its own HNSW index. Columns can have different dimensions and metrics since different embedding models produce different-shaped vectors.

```sql
-- Collection with text and image embeddings
CREATE COLLECTION media TYPE strict (
    id              STRING PRIMARY KEY,
    title           STRING NOT NULL,
    description     STRING,
    text_embedding  VECTOR(1536),
    clip_embedding  VECTOR(512),
    audio_embedding VECTOR(256),
    modality        STRING,
    created_at      DATETIME DEFAULT NOW()
);

-- Each vector column gets its own index
CREATE VECTOR INDEX idx_text ON media FIELDS text_embedding DIMENSION 1536 METRIC cosine;
CREATE VECTOR INDEX idx_clip ON media FIELDS clip_embedding DIMENSION 512 METRIC cosine;
CREATE VECTOR INDEX idx_audio ON media FIELDS audio_embedding DIMENSION 256 METRIC cosine;

-- Track which models produced which embeddings
ALTER COLLECTION media SET VECTOR METADATA ON text_embedding (
    model = 'text-embedding-3-large', dimensions = 1536
);
ALTER COLLECTION media SET VECTOR METADATA ON clip_embedding (
    model = 'clip-vit-large-patch14', dimensions = 512
);
ALTER COLLECTION media SET VECTOR METADATA ON audio_embedding (
    model = 'clap-general', dimensions = 256
);
```

## Per-Modality Search

Search a single modality when you know which type of content you're looking for.

```sql
-- Text search: find documents by semantic text similarity
SELECT id, title, vector_distance() AS score
FROM media
WHERE text_embedding <-> $text_query_embedding
LIMIT 10;

-- Image search: find visually similar items
SELECT id, title, vector_distance() AS score
FROM media
WHERE clip_embedding <-> $image_query_embedding
LIMIT 10;

-- Audio search: find similar-sounding content
SELECT id, title, vector_distance() AS score
FROM media
WHERE audio_embedding <-> $audio_query_embedding
LIMIT 10;
```

## Cross-Modal Search (CLIP)

CLIP-style models encode text and images into the same embedding space. A text query can find images, and an image query can find text descriptions.

```sql
-- Text-to-image: user types a text query, find matching images
-- (Your app embeds the text query using CLIP's text encoder)
SELECT id, title, modality, vector_distance() AS score
FROM media
WHERE modality = 'image'
  AND clip_embedding <-> $clip_text_embedding
LIMIT 10;

-- Image-to-text: user uploads an image, find matching descriptions
-- (Your app embeds the image using CLIP's image encoder)
SELECT id, title, description, vector_distance() AS score
FROM media
WHERE modality = 'text'
  AND clip_embedding <-> $clip_image_embedding
LIMIT 10;
```

## Multi-Modal RRF Fusion

When the same document has multiple embeddings, fuse search results from different modalities using RRF. This catches items that are relevant in one modality even if they score lower in another.

```sql
-- Fuse text + image + audio similarity for a comprehensive search
-- (Your app generates query embeddings for each modality)
SELECT id, title, rrf_score(
    vector_distance(text_embedding, $text_query),
    vector_distance(clip_embedding, $clip_query),
    60, 60
) AS score
FROM media
ORDER BY score DESC
LIMIT 10;
```

**When to use multi-modal fusion:**

- E-commerce: product search combines text description match + visual similarity + user review sentiment
- Media libraries: find content that matches across description, thumbnail, and audio track
- Research: find papers where both the abstract (text) and figures (image) match the query

**When single-modality is better:**

- When the query is clearly in one modality ("show me pictures of cats" → image search only)
- When modalities have very different quality (strong text embeddings, weak audio → text search with audio as a secondary signal)

## Filtered Multi-Modal Search

Combine modality-specific search with metadata filters.

```sql
-- Find recent images similar to a query, with text relevance boost
SELECT id, title, rrf_score(
    vector_distance(clip_embedding, $clip_query),
    vector_distance(text_embedding, $text_query),
    60, 60
) AS score
FROM media
WHERE modality IN ('image', 'video')
  AND created_at > '2025-01-01'
ORDER BY score DESC
LIMIT 20;
```

## ColBERT-Style Multi-Vector Search

For token-level embeddings (ColBERT, ColPali), use the `multi_vector_score` function to aggregate per-token similarities:

```sql
-- Insert a document with per-token embeddings
-- (Your app generates one embedding per token using ColBERT)
-- Use the multi-vector insert API with shared doc_id

-- Search with MaxSim aggregation (ColBERT scoring)
SELECT id, content,
       multi_vector_score(token_vectors, $query_vector, 'max_sim') AS score
FROM chunks
ORDER BY multi_vector_score(token_vectors, $query_vector, 'max_sim')
LIMIT 10;
```

MaxSim finds the best-matching token in each document for the query, making it robust to partial matches and paraphrasing.
