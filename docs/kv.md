# Key-Value Engine

NodeDB's KV engine is a purpose-built hash-indexed store with O(1) point lookups, native TTL, and optional secondary indexes. Unlike a standalone KV store, data here is SQL-queryable, joinable with other collections, and syncable to edge devices via CRDTs.

## When to Use

- Session state and tokens
- Feature flags and configuration
- Rate limiters and counters
- Caching (without needing an external cache)
- Any workload dominated by primary-key access

## Why Not Just Use Redis?

Running an external KV store alongside your database means a second deployment, a second failure domain, application-level cache invalidation, data duplication, and no ability to query or join KV data with the rest of your system.

NodeDB's KV engine eliminates this:
- Hot reads serve from mmap'd memory at sub-millisecond latency — there's no slow database that needs a cache in front
- Real-time is native (LIVE SELECT, CDC, pub/sub) — no Redis Streams sidecar
- KV data is SQL-queryable: `SELECT region, COUNT(*) FROM sessions GROUP BY region`
- KV data joins with other collections, appears in CDC, and syncs to edge devices

## Key Features

- **O(1) point lookups** — Hash-indexed by user-defined key
- **Typed value fields** — Not serialized blobs. Fields have types and are individually accessible.
- **Native TTL** — Index-backed expiry wheel. Set TTL per key, auto-expired.
- **Secondary indexes** — Optional indexes on value fields for filtered scans
- **SQL-queryable** — Full SQL works on KV collections (aggregations, joins, WHERE clauses)
- **Wire protocol** — Dedicated GET/SET/DEL/EXPIRE/SCAN for workloads that just need fast point operations

## Examples

```sql
-- Create a KV collection
CREATE COLLECTION sessions TYPE kv;

-- Set with TTL (seconds)
INSERT INTO sessions { key: 'sess_abc123', user_id: 'alice', role: 'admin', ttl: 3600 };

-- Get by key
SELECT * FROM sessions WHERE key = 'sess_abc123';

-- Update
UPDATE sessions SET role = 'superadmin' WHERE key = 'sess_abc123';

-- Delete
DELETE FROM sessions WHERE key = 'sess_abc123';

-- Secondary index for filtered queries
CREATE INDEX ON sessions FIELDS role;
SELECT key, user_id FROM sessions WHERE role = 'admin';

-- Analytical query on KV data
SELECT role, COUNT(*) AS active_sessions
FROM sessions
GROUP BY role;

-- Join KV data with other collections
SELECT u.name, s.role, s.key
FROM users u
JOIN sessions s ON u.id = s.user_id
WHERE s.role = 'admin';
```

## Converting to KV

```sql
-- Convert an existing collection when access pattern is key-dominant
CONVERT COLLECTION cache TO kv;
```

## Related

- [Documents](documents.md) — For richer document structures
- [Real-Time](real-time.md) — KV changes appear in CDC, LIVE SELECT, pub/sub

[Back to docs](README.md)
