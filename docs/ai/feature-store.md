# Feature Store

NodeDB's columnar engine stores training features with 20-40x compression and predicate pushdown. The strict engine serves online features at sub-millisecond latency. Together they cover the full ML feature lifecycle: ingest, temporal query, batch export, and online serving — in one database.

## Columnar Engine for Training Features

Store feature data in columnar collections with typed columns. Block-level statistics enable fast scans over time ranges without decompressing irrelevant data.

```sql
-- Create a feature collection with time partitioning
CREATE COLLECTION user_features (
    ts          TIMESTAMP TIME_KEY,
    user_id     VARCHAR,
    page_views  INT,
    session_dur FLOAT,
    cart_value  FLOAT,
    embedding   VARCHAR,
    region      VARCHAR
) WITH (engine='timeseries', partition_by='1d', retention='365d');

-- Ingest features (batch or streaming)
INSERT INTO user_features VALUES (
    NOW(), 'user-42', 15, 342.5, 89.99, '[0.12, -0.34, ...]', 'us-west'
);

-- Analytical query: feature distributions
SELECT region,
       AVG(page_views) AS avg_pv,
       AVG(session_dur) AS avg_dur,
       COUNT(*) AS samples
FROM user_features
WHERE ts BETWEEN '2025-01-01' AND '2025-03-01'
GROUP BY region;
```

## Point-in-Time Lookups (No Data Leakage)

The most critical requirement for ML feature stores: when constructing training data, you must only use features that were available at the time of the training event. Using future features causes data leakage and inflated offline metrics that don't hold in production.

```sql
-- Training events table (when each prediction was made)
CREATE COLLECTION training_events (
    ts       TIMESTAMP TIME_KEY,
    event_id VARCHAR,
    user_id  VARCHAR,
    label    INT
) WITH (engine='timeseries');

-- Point-in-time join: for each training event, get the most recent
-- feature values that were available BEFORE the event timestamp.
-- This prevents data leakage.
SELECT
    te.event_id,
    te.user_id,
    te.label,
    uf.page_views,
    uf.session_dur,
    uf.cart_value
FROM training_events te
JOIN user_features uf
    ON te.user_id = uf.user_id
    AND uf.ts <= te.ts
    AND uf.ts > te.ts - INTERVAL '7 days'
ORDER BY te.event_id, uf.ts DESC;

-- For each event, take the most recent feature row only.
-- Your application deduplicates by (event_id, user_id) after this query,
-- keeping the row with the latest uf.ts per event.
```

**Why this works:** The `uf.ts <= te.ts` predicate ensures no future features leak into training data. The 7-day window bounds the lookback to avoid stale features. Block-level time statistics in the columnar engine make this range scan efficient even over billions of rows.

## Batch Export for Training

The columnar engine produces Parquet-compatible scan output. Export feature datasets for model training frameworks (PyTorch, XGBoost, scikit-learn).

```sql
-- Export features for a specific time range and cohort
SELECT user_id, page_views, session_dur, cart_value, region
FROM user_features
WHERE ts BETWEEN '2025-01-01' AND '2025-03-01'
  AND region IN ('us-west', 'us-east');

-- Aggregated features: compute rolling statistics per user
SELECT
    user_id,
    AVG(page_views) AS avg_pv_30d,
    AVG(session_dur) AS avg_dur_30d,
    MAX(cart_value) AS max_cart_30d,
    COUNT(*) AS visit_count_30d
FROM user_features
WHERE ts > NOW() - INTERVAL '30 days'
GROUP BY user_id;
```

Connect via pgwire (any PostgreSQL client library) and stream results directly into your training pipeline:

```python
# Python example using psycopg2
import psycopg2
import pandas as pd

conn = psycopg2.connect("host=localhost port=5432 dbname=nodedb")
df = pd.read_sql("""
    SELECT user_id, page_views, session_dur, cart_value
    FROM user_features
    WHERE ts BETWEEN '2025-01-01' AND '2025-03-01'
""", conn)
# df is ready for model training
```

## Online Serving

For real-time inference, serve the latest feature values from a strict (OLTP) collection. Strict collections use binary tuples with O(1) field extraction — no parsing overhead.

```sql
-- Online feature store: latest features per user (strict collection)
CREATE COLLECTION user_features_live TYPE strict (
    user_id     STRING PRIMARY KEY,
    page_views  INT,
    session_dur FLOAT,
    cart_value  FLOAT,
    region      STRING,
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- Upsert latest features (from your streaming pipeline)
UPSERT INTO user_features_live VALUES (
    'user-42', 18, 412.3, 129.99, 'us-west', NOW()
);

-- Online lookup: sub-ms latency for a single user
SELECT page_views, session_dur, cart_value
FROM user_features_live
WHERE user_id = 'user-42';
```

**HTAP bridge:** Use a CDC change stream to materialize columnar features into the online store automatically:

```sql
-- Stream columnar inserts into the online store
CREATE CHANGE STREAM feature_updates ON user_features;

-- Your app subscribes and upserts into the live store:
-- SELECT * FROM STREAM feature_updates CONSUMER GROUP online_serving LIMIT 100;
-- For each row: UPSERT INTO user_features_live VALUES (...);
```

This gives you a single source of truth (columnar) with an automatically maintained online serving layer (strict).
