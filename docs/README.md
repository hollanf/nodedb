# NodeDB Documentation

Welcome to the NodeDB docs. These guides explain what each engine does, when to use it, and how to get started. For full API reference, see the [cargo docs](https://docs.rs/nodedb) or the [API reference](https://github.com/NodeDB-Lab/nodedb-docs) (coming soon).

## Getting Started

- [Getting Started](getting-started.md) — Prerequisites, build, run, first queries
- [Architecture](architecture.md) — How the hybrid execution model works
- [Query Language](query-language.md) — Full SQL reference (DDL, DML, engine-specific syntax, functions)
- [Protocols](protocols.md) — Six wire protocols (pgwire, NDB, HTTP, RESP, ILP, Sync)

## Engine Guides

- [Vector Search](vectors.md) — HNSW index, quantization, adaptive filtering, hybrid search
- [Graph](graph.md) — CSR adjacency, 13 algorithms, MATCH patterns, GraphRAG
- [Documents](documents.md) — Schemaless (MessagePack + CRDT) and Strict (Binary Tuples, OLTP)
- [Columnar](columnar.md) — Per-column compression, predicate pushdown, HTAP bridge
- [Timeseries](timeseries.md) — ILP ingest, continuous aggregation, PromQL, approximate aggregation
- [Spatial](spatial.md) — R\*-tree, geohash, H3, OGC predicates, hybrid spatial-vector
- [Key-Value](kv.md) — O(1) lookups, TTL, secondary indexes, SQL-queryable
- [Full-Text Search](full-text-search.md) — BM25, stemming, fuzzy, hybrid vector fusion

## Platform & Operations

- [NodeDB-Lite](lite.md) — Embedded database for phones, browsers, desktops
- [Security](security.md) — Authentication, RBAC, RLS, encryption
- [Real-Time](real-time.md) — LIVE SELECT, CDC, pub/sub, WebSocket
- [CLI (`ndb`)](cli.md) — Terminal client usage and configuration
