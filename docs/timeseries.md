# Timeseries

Timeseries is a columnar profile specialized for time-ordered data — metrics, telemetry, sensor readings, financial ticks. It adds retention policies, continuous aggregation, ILP ingest, and 12 dedicated SQL functions on top of the columnar engine's compression and predicate pushdown.

## When to Use

- Application and infrastructure metrics
- IoT sensor telemetry
- Financial market data
- Log analytics
- Any time-ordered append-mostly workload

## Key Features

- **Cascading compression** — ALP + FastLanes + FSST + Pcodec + rANS + LZ4. Achieves 20-40x compression on typical telemetry, compared to 3-5x from single-pass approaches.
- **ILP ingest** — InfluxDB Line Protocol for high-throughput metric ingestion with adaptive batching and per-series core routing.
- **Continuous aggregation** — Incrementally maintained materialized views with watermarks, multi-tier chaining, and out-of-order handling. No full re-scan on refresh.
- **Retention policies** — Automatic expiry of old data by time partition.
- **Approximate aggregation** — HyperLogLog (cardinality), t-digest (percentiles), SpaceSaving (top-K), Count-Min Sketch (frequency). All mergeable across shards and usable in continuous aggregation.
- **Block-level skip** — Sparse primary index with min/max statistics per block. Queries skip irrelevant time ranges without decompression.
- **PromQL** — Full Prometheus query engine (Tier 1+2+3 functions) at `/obsv/api`. Point Grafana at NodeDB directly.
- **Prometheus remote write/read** — Use NodeDB as a long-term storage backend for Prometheus.
- **OTLP ingest** — OpenTelemetry metrics, traces, and logs via HTTP (port 4318) and gRPC (port 4317).

## Examples

```sql
-- Create a timeseries collection
CREATE COLLECTION cpu_metrics TYPE columnar PROFILE timeseries (
    host STRING,
    region STRING,
    cpu_usage FLOAT,
    mem_usage FLOAT,
    ts DATETIME
) PARTITION BY TIME(ts, '1 day')
  RETENTION '90 days';

-- Insert metrics
INSERT INTO cpu_metrics (host, region, cpu_usage, mem_usage, ts)
VALUES ('web-01', 'us-east', 72.5, 84.2, now());

-- Time-bucketed aggregation
SELECT time_bucket('5 minutes', ts) AS bucket,
       host,
       AVG(cpu_usage) AS avg_cpu,
       MAX(mem_usage) AS max_mem
FROM cpu_metrics
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY bucket, host
ORDER BY bucket DESC;

-- Continuous aggregation (incremental, no full re-scan)
CREATE MATERIALIZED VIEW cpu_hourly AS
SELECT time_bucket('1 hour', ts) AS hour,
       host,
       AVG(cpu_usage) AS avg_cpu,
       ts_percentile(cpu_usage, 0.99) AS p99_cpu
FROM cpu_metrics
GROUP BY hour, host
WITH (refresh = 'incremental');

-- Approximate aggregation
SELECT approx_count_distinct(host) AS unique_hosts,
       approx_percentile(cpu_usage, 0.95) AS p95_cpu,
       approx_topk(host, 5) AS busiest_hosts
FROM cpu_metrics
WHERE ts > now() - INTERVAL '24 hours';
```

## Timeseries SQL Functions

| Function         | What it does                          |
| ---------------- | ------------------------------------- |
| `ts_rate`        | Per-second rate of change             |
| `ts_delta`       | Difference between consecutive values |
| `ts_moving_avg`  | Moving average over a window          |
| `ts_ema`         | Exponential moving average            |
| `ts_interpolate` | Fill gaps with interpolated values    |
| `ts_percentile`  | Percentile calculation                |
| `ts_correlate`   | Correlation between two series        |
| `ts_lag`         | Previous value in a series            |
| `ts_lead`        | Next value in a series                |
| `ts_rank`        | Rank within a series                  |
| `ts_stddev`      | Standard deviation                    |
| `ts_derivative`  | Rate of change                        |

## Grafana Integration

NodeDB works as a native Grafana data source:

1. Add a Prometheus data source in Grafana
2. Point it at `http://nodedb-host:6480/obsv/api`
3. Use PromQL queries directly in Grafana dashboards

NodeDB also supports Grafana's health check, metadata, annotations, and label discovery endpoints.

## Related

- [Columnar](columnar.md) — Timeseries is a columnar profile
- [Spatial](spatial.md) — Combine time and location (fleet tracking, IoT)
- [Architecture](architecture.md) — How storage tiers handle hot/warm/cold data

[Back to docs](README.md)
