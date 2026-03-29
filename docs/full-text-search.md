# Full-Text Search

NodeDB's full-text search engine provides BM25-ranked search with 15-language stemming, fuzzy matching, and native hybrid fusion with vector search — all in a single query.

## When to Use

- Text search across documents (articles, products, logs)
- Search-as-you-type with fuzzy matching
- Multilingual content search
- Hybrid retrieval: combine keyword matching with semantic similarity

## Key Features

- **BM25 ranking** — Standard relevance scoring with term frequency and inverse document frequency
- **15-language stemming** — Snowball stemmers for Arabic, Danish, Dutch, English, Finnish, French, German, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish
- **Fuzzy matching** — Levenshtein distance-based matching for typo tolerance
- **Synonyms** — Define synonym groups for query expansion
- **Highlighting** — Return matched terms with surrounding context
- **N-gram and edge-ngram** — Partial-word matching for autocomplete
- **OR mode** — Match any term (default is AND)
- **Hybrid vector fusion** — Reciprocal Rank Fusion (RRF) merges BM25 text results with vector similarity in one query

## Examples

```sql
-- Create a full-text index
CREATE COLLECTION articles TYPE document;
CREATE SEARCH INDEX ON articles FIELDS title, body
    ANALYZER 'english'
    FUZZY true;

-- Basic search
SELECT title, search_score() AS score
FROM articles
WHERE MATCH(body, 'distributed database rust')
ORDER BY score DESC
LIMIT 20;

-- Fuzzy search (handles typos)
SELECT title FROM articles
WHERE MATCH(title, 'databse', { fuzzy: true, distance: 2 });

-- Hybrid search: BM25 + vector similarity (RRF fusion)
SELECT title, rrf_score() AS score
FROM articles
WHERE MATCH(body, 'distributed systems')
  AND embedding <-> $query_vec
LIMIT 10;

-- Highlighting
SELECT title, search_highlight(body) AS snippet
FROM articles
WHERE MATCH(body, 'graph traversal')
LIMIT 10;

-- Search with synonyms
CREATE SYNONYM GROUP db_terms AS ('database', 'db', 'datastore');

SELECT title FROM articles
WHERE MATCH(body, 'db performance');
-- Also matches "database performance" and "datastore performance"
```

## Analyzers

Full-text indexes use analyzers to process text. An analyzer is a pipeline of tokenizer + filters:

| Analyzer       | What it does                             |
| -------------- | ---------------------------------------- |
| `standard`     | Unicode tokenization, lowercase          |
| `english`      | Standard + English stemming + stop words |
| `simple`       | Whitespace split, lowercase              |
| (15 languages) | Standard + language-specific stemming    |

## Hybrid Search

The most powerful feature is combining BM25 with vector search. This handles the common RAG problem where keyword matching catches exact terms that embeddings miss, and embeddings catch semantic meaning that keywords miss.

NodeDB fuses both result sets using Reciprocal Rank Fusion (RRF) — no application-level merging needed. The fusion happens inside the query engine in a single pass.

## Related

- [Vector Search](vectors.md) — Hybrid vector + text fusion
- [Graph](graph.md) — Graph context can further refine search results (GraphRAG)

[Back to docs](README.md)
