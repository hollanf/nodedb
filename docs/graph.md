# Graph Engine

NodeDB's graph engine uses a native CSR (Compressed Sparse Row) adjacency index — not recursive JOINs pretending to be a graph. It's optimized for cache-resident working sets and supports sub-millisecond multi-hop traversals.

## When to Use

- Knowledge graphs and ontologies
- Social networks and relationship modeling
- Fraud detection (pattern matching across relationships)
- Recommendation systems (collaborative filtering via graph)
- AI agent knowledge bases (GraphRAG)

## Key Features

- **CSR adjacency index** — Cache-optimized sparse row format for fast traversal
- **13 native algorithms** — PageRank, Weakly Connected Components (WCC), Label Propagation, Local Clustering Coefficient (LCC), Single-Source Shortest Path (SSSP), Betweenness Centrality, Closeness Centrality, Harmonic Centrality, Degree Centrality, Louvain (community detection), Triangle Count, Diameter, k-Core Decomposition
- **MATCH pattern engine** — Cypher-subset pattern matching with parser, optimizer, and executor
- **GraphRAG** — Native vector + graph fusion with Reciprocal Rank Fusion (RRF). Vector similarity finds relevant documents, graph traversal expands context along relationships, RRF merges results.
- **Graph OLAP** — Snapshots, parallel execution, property column store for analytics on graph data
- **Distributed BSP** — Bulk Synchronous Parallel execution across shards (PageRank, WCC, pattern matching)
- **Weighted edges** — Edge properties with statistics tracking

## Examples

```sql
-- Create a graph collection
CREATE COLLECTION knows TYPE graph;

-- Add edges between nodes
INSERT INTO knows { from: 'users:alice', to: 'users:bob', weight: 1.0, since: 2020 };
INSERT INTO knows { from: 'users:bob', to: 'users:carol', weight: 0.8, since: 2021 };
INSERT INTO knows { from: 'users:carol', to: 'users:alice', weight: 0.5, since: 2022 };

-- Traverse relationships
SELECT * FROM knows WHERE from = 'users:alice' DEPTH 1..3;

-- Pattern matching (Cypher-subset)
MATCH (a:users)-[:knows]->(b:users)-[:knows]->(c:users)
WHERE a.name = 'Alice'
RETURN b.name, c.name;

-- Run algorithms
SELECT * FROM graph::pagerank('knows', { iterations: 20, damping: 0.85 });
SELECT * FROM graph::shortest_path('knows', 'users:alice', 'users:carol');
SELECT * FROM graph::louvain('knows');

-- GraphRAG: combine vector search with graph context
SELECT title, rrf_score() AS score
FROM articles
WHERE embedding <-> $query_vec
  AND graph::connected('cites', id, DEPTH 1..2)
LIMIT 10;
```

## Algorithms

| Algorithm         | What it does                                           |
| ----------------- | ------------------------------------------------------ |
| PageRank          | Ranks nodes by importance based on incoming links      |
| WCC               | Finds connected components                             |
| Label Propagation | Community detection via label spreading                |
| LCC               | Measures how tightly a node's neighbors are connected  |
| SSSP              | Shortest path from one node to all others              |
| Betweenness       | Identifies bridge nodes (high traffic)                 |
| Closeness         | Measures how close a node is to all others             |
| Harmonic          | Like closeness, but handles disconnected graphs        |
| Degree            | Counts connections per node                            |
| Louvain           | Community detection via modularity optimization        |
| Triangles         | Counts triangles each node participates in             |
| Diameter          | Longest shortest path in the graph                     |
| k-Core            | Finds the k-core subgraph (all nodes with degree >= k) |

## Related

- [Vector Search](vectors.md) — GraphRAG combines graph traversal with vector similarity
- [Full-Text Search](full-text-search.md) — BM25 results can be re-ranked using graph context

[Back to docs](README.md)
