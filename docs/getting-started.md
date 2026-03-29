# Getting Started

This guide walks you through building NodeDB, starting a server, and running your first queries.

## Prerequisites

- **Rust 1.89+** (edition 2024)
- **Linux** recommended — the data plane uses io_uring for storage I/O
- macOS and Windows work for development (without io_uring acceleration)

## Build from Source

```bash
git clone https://github.com/NodeDB-Lab/nodedb.git
cd nodedb

# Release build (all crates)
cargo build --release

# Run tests
cargo test --all-features
```

The build produces two binaries:

- `target/release/nodedb` — the database server
- `target/release/ndb` — the terminal client

## Start the Server

```bash
# Single-node, default ports
./target/release/nodedb
```

By default, NodeDB listens on:

- **6432** — PostgreSQL wire protocol (pgwire)
- **6433** — Native MessagePack protocol
- **6480** — HTTP API (REST, SSE, WebSocket)
- **6381** — RESP (Redis-compatible KV protocol) — optional
- **8086** — ILP (InfluxDB Line Protocol for timeseries ingest) — optional
- **9090** — WebSocket sync (NodeDB-Lite clients)

## Connect

### With the `ndb` CLI

```bash
./target/release/ndb
```

You get a full TUI with syntax highlighting, tab completion, and history search. See the [CLI guide](cli.md) for details.

### With psql

```bash
psql -h localhost -p 6432
```

NodeDB speaks PostgreSQL's wire protocol, so standard tools like `psql`, ORMs, and BI tools work out of the box.

## First Queries

### Documents (schemaless)

```sql
-- Create a schemaless collection
CREATE COLLECTION users TYPE document;

-- Insert some data
INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 };
INSERT INTO users { name: 'Bob', email: 'bob@example.com', role: 'admin' };

-- Query
SELECT * FROM users WHERE name = 'Alice';
SELECT name, email FROM users WHERE age > 25;
```

### Strict Documents (schema-enforced)

```sql
-- Create a strict collection with a defined schema
CREATE COLLECTION orders TYPE strict (
    id UUID DEFAULT gen_uuid_v7(),
    customer_id UUID,
    total DECIMAL,
    status STRING,
    created_at DATETIME DEFAULT now()
);

INSERT INTO orders (customer_id, total, status)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 99.99, 'pending');

SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC;
```

### Vector Search

```sql
-- Create a collection with a vector index
CREATE COLLECTION articles TYPE document;
CREATE VECTOR INDEX ON articles FIELDS embedding DIMENSION 384 METRIC cosine;

-- Insert documents with embeddings
INSERT INTO articles { title: 'Intro to AI', embedding: [0.1, 0.2, ...] };

-- Search by similarity
SELECT title, vector_distance() AS score
FROM articles
WHERE embedding <-> [0.1, 0.3, ...]
LIMIT 10;
```

### Graph

```sql
-- Create a graph collection
CREATE COLLECTION knows TYPE graph;

-- Add edges
INSERT INTO knows { from: 'users:alice', to: 'users:bob', since: 2020 };
INSERT INTO knows { from: 'users:bob', to: 'users:carol', since: 2021 };

-- Traverse
SELECT * FROM knows WHERE from = 'users:alice' DEPTH 1..3;

-- Run an algorithm
SELECT * FROM graph::pagerank('knows', { iterations: 20 });
```

### Key-Value

```sql
-- Create a KV collection
CREATE COLLECTION sessions TYPE kv;

-- Set with TTL
INSERT INTO sessions { key: 'sess_abc', user_id: 'alice', ttl: 3600 };

-- Get by key
SELECT * FROM sessions WHERE key = 'sess_abc';
```

## What's Next

- [Architecture](architecture.md) — understand how the hybrid execution model works
- Engine deep dives: [Vectors](vectors.md) | [Graph](graph.md) | [Documents](documents.md) | [Columnar](columnar.md) | [Timeseries](timeseries.md) | [Spatial](spatial.md) | [KV](kv.md) | [Full-Text](full-text-search.md)
- [NodeDB-Lite](lite.md) — embed NodeDB in your app (mobile, WASM, desktop)
- [Security](security.md) — set up authentication and access control

[Back to docs](README.md)
