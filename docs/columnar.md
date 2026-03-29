# Columnar Engine

NodeDB's columnar engine stores data in typed columns with per-column compression — the same approach as ClickHouse and DuckDB, but living alongside your OLTP data in the same database.

## When to Use

- Analytical queries (GROUP BY, aggregations, window functions)
- Reporting dashboards
- Data science workloads
- Any scan-heavy workload where you read a few columns from many rows
- HTAP: pair with strict documents for combined OLTP + OLAP

## Key Features

- **Per-column compression** — Each column gets a codec chain tuned for its data type:
  - **ALP** — Lossless floating-point to integer conversion
  - **FastLanes** — SIMD bit-packing for integers
  - **FSST** — Substring dictionary compression for strings
  - **Gorilla** — XOR-based compression for metrics
  - **Pcodec** — Complex numeric compression
  - **rANS** — Entropy coding for cold-tier data
  - **LZ4** — Terminal stage compression
- **Block statistics** — 1024-row blocks with min/max/null-count stats. The query engine skips entire blocks that can't match the predicate — no decompression needed.
- **Predicate pushdown** — Filters push down to the block level, reading only what's needed.
- **Delete bitmaps** — Roaring Bitmaps track deleted rows. Three-phase crash-safe compaction reclaims space.
- **20-40x compression** — Multi-stage pipeline achieves significantly better ratios than single-codec approaches on typical workloads.

## Profiles

Columnar collections can specialize via profiles:

| Profile                         | Optimized for        | Extra features                                                      |
| ------------------------------- | -------------------- | ------------------------------------------------------------------- |
| **Plain**                       | General analytics    | Full UPDATE/DELETE support                                          |
| **[Timeseries](timeseries.md)** | Time-ordered metrics | Append-only, retention policies, continuous aggregation, ILP ingest |
| **[Spatial](spatial.md)**       | Geospatial data      | Auto R\*-tree indexing, geohash proximity queries                   |

## Examples

```sql
-- Create a columnar collection
CREATE COLLECTION web_events TYPE columnar (
    event_id UUID,
    user_id UUID,
    page STRING,
    duration_ms INT,
    created_at DATETIME
);

-- Analytical query — only reads the columns it needs
SELECT page, AVG(duration_ms), COUNT(*)
FROM web_events
WHERE created_at > '2024-01-01'
GROUP BY page
ORDER BY COUNT(*) DESC
LIMIT 20;

-- Window functions
SELECT user_id, page, duration_ms,
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS visit_rank
FROM web_events;
```

## HTAP Bridge

The real power of columnar is combining it with strict documents for hybrid transactional/analytical processing (HTAP).

```sql
-- OLTP: strict collection for real-time writes
CREATE COLLECTION orders TYPE strict (
    id UUID DEFAULT gen_uuid_v7(),
    customer_id UUID,
    total DECIMAL,
    status STRING,
    created_at DATETIME DEFAULT now()
);

-- OLAP: materialized view auto-replicates to columnar via CDC
CREATE MATERIALIZED VIEW order_analytics AS
SELECT status, DATE_TRUNC('day', created_at) AS day, COUNT(*), SUM(total)
FROM orders
GROUP BY status, day;

-- Point lookups hit the strict engine (fast)
SELECT * FROM orders WHERE id = '...';

-- Analytical scans hit the columnar engine (fast)
SELECT * FROM order_analytics WHERE day > '2024-06-01';
```

The query planner routes automatically — no application logic needed. Changes to the strict collection propagate to the materialized view via CDC with staleness tracking.

## Converting to Columnar

```sql
-- Convert an existing collection
CONVERT COLLECTION logs TO columnar;

-- Convert with a profile
CONVERT COLLECTION metrics TO columnar PROFILE timeseries;
```

## Related

- [Timeseries](timeseries.md) — Columnar profile for time-ordered data
- [Spatial](spatial.md) — Columnar profile for geospatial data
- [Documents (strict)](documents.md) — HTAP bridge source for OLTP + OLAP

[Back to docs](README.md)
