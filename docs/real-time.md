# Real-Time Features

NodeDB is a real-time database. Every committed mutation publishes to an internal event bus. No external message broker needed — real-time is part of the database, not a sidecar.

The event infrastructure is built on the **Event Plane** — a third architectural layer alongside the Control Plane (query planning) and Data Plane (storage I/O). The Data Plane emits `WriteEvent` and `DeleteEvent` records to the Event Plane via per-core bounded ring buffers after each WAL commit. The Event Plane routes these to change stream consumers, trigger dispatch, and webhook delivery. WAL-backed crash recovery ensures no events are lost across restarts.

## LIVE SELECT

Register a query and receive matching changes as they happen. No polling.

```sql
-- Subscribe to all new orders over $100
LIVE SELECT * FROM orders WHERE total > 100.00;

-- Subscribe to status changes
LIVE SELECT id, status FROM orders WHERE status != 'pending';
```

The server pushes matching inserts, updates, and deletes to the client in real time.

**Protocol support:**

- **pgwire** — Subscription is stored in the session. Notifications are delivered as `NotificationResponse` messages between query responses, using the standard pgwire async notification channel. Standard `psql` and JDBC clients receive them without any additional configuration.
- **WebSocket** — Delivered as JSON frames over the `/ws` endpoint.
- **NDB (native)** — Delivered as MessagePack frames on the native protocol connection.

On pgwire, `LIVE SELECT` stores the subscription in session state. Each subsequent command response may be preceded by one or more `NotificationResponse` frames carrying the matching change payloads. The subscription remains active until the session ends or `CANCEL LIVE SELECT <id>` is executed.

## SHOW CHANGES

Pull-based CDC with cursor pagination. Useful for batch consumers or point-in-time replay.

```sql
-- Get changes since a cursor
SHOW CHANGES FOR orders SINCE '2024-01-15T00:00:00Z' LIMIT 1000;
```

## Triggers

SQL blocks that fire on data mutations. Three execution modes let you choose the trade-off between write latency and atomicity:

```sql
-- ASYNC (default): fires after commit via Event Plane — zero write-latency impact
-- Side effects are eventually consistent; failures retry with backoff, then go to DLQ
CREATE TRIGGER audit_orders AFTER INSERT ON orders FOR EACH ROW
BEGIN
    INSERT INTO audit_log { collection: 'orders', action: 'INSERT', row_id: NEW.id, ts: now() };
END;

-- SYNC: fires in the same transaction on the Data Plane (ACID, write latency += trigger time)
CREATE TRIGGER enforce_balance AFTER UPDATE ON accounts FOR EACH ROW
WITH (EXECUTION = SYNC)
BEGIN
    IF NEW.balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot go negative';
    END IF;
END;

-- DEFERRED: fires at COMMIT time, batched (ACID, COMMIT is slower)
CREATE TRIGGER validate_totals AFTER INSERT ON line_items FOR EACH ROW
WITH (EXECUTION = DEFERRED)
BEGIN
    -- cross-row validation at statement boundary
END;

DROP TRIGGER audit_orders ON orders;
SHOW TRIGGERS;
```

| Mode | Atomicity | Write latency | Rollback on failure |
| ---- | --------- | ------------- | ------------------- |
| `ASYNC` (default) | Eventually consistent | None | No |
| `SYNC` | Same transaction (ACID) | Trigger time added | Yes |
| `DEFERRED` | Same transaction, batched | At COMMIT time | Yes |

## CDC Change Streams

Change streams provide durable, cursor-tracked access to the mutation log for a collection. Unlike `LIVE SELECT` (push to a session), change streams survive reconnects, support consumer groups, and can deliver to external systems via webhook.

```sql
-- Basic change stream
CREATE CHANGE STREAM order_changes ON orders;

-- With webhook delivery (HMAC-signed POST to an external endpoint)
CREATE CHANGE STREAM order_events ON orders
WITH (
    WEBHOOK_URL = 'https://hooks.example.com/orders',
    WEBHOOK_SECRET = 'whsec_abc123'
);

-- With log compaction: only the latest mutation per key is retained
CREATE CHANGE STREAM user_state ON users
WITH (COMPACTION = 'key', KEY = 'id');

DROP CHANGE STREAM order_changes;
SHOW CHANGE STREAMS;
```

### Consumer Groups

Consumer groups track read positions independently, enabling multiple consumers to process the same stream at their own pace.

```sql
-- Create a consumer group
CREATE CONSUMER GROUP analytics FOR STREAM order_changes;
CREATE CONSUMER GROUP billing FOR STREAM order_changes;

-- Commit offset after successfully processing up to event 42
COMMIT OFFSET FOR STREAM order_changes GROUP analytics TO 42;

DROP CONSUMER GROUP analytics FOR STREAM order_changes;
```

## Durable Topics

Durable topics provide persistent pub/sub backed by the change stream infrastructure.

```sql
-- Create a topic on an existing change stream
CREATE TOPIC order_events ON STREAM order_changes;

-- Subscribe (with consumer group for load balancing)
SUBSCRIBE TO order_events GROUP 'processors';

DROP TOPIC order_events;
```

## Cron Scheduler

The Event Plane includes a distributed cron scheduler. Jobs are stored in the catalog, evaluated per-second, and dispatched through the normal Control Plane → Data Plane execution path.

```sql
-- Run a cleanup job at 2 AM UTC daily
CREATE SCHEDULE nightly_cleanup
CRON '0 2 * * *'
AS BEGIN
    DELETE FROM sessions WHERE expires_at < now();
    INSERT INTO maintenance_log { task: 'nightly_cleanup', ran_at: now() };
END;

-- Run a 5-minute aggregate refresh
CREATE SCHEDULE refresh_stats
CRON '*/5 * * * *'
AS BEGIN
    REFRESH CONTINUOUS AGGREGATE order_stats;
END;

DROP SCHEDULE nightly_cleanup;
SHOW SCHEDULES;
```

## SSE Streaming

HTTP Server-Sent Events for CDC consumers that can't use WebSocket:

```
GET /api/v1/collections/orders/changes?since=2024-01-15T00:00:00Z
Accept: text/event-stream
```

## WebSocket RPC

JSON-RPC over WebSocket for SQL execution, live query delivery, and session management:

- Execute SQL queries
- Receive LIVE SELECT updates
- Session reconnect with event replay
- Auth token refresh during long-lived connections

## Related

- [Key-Value](kv.md) — KV changes appear in CDC and LIVE SELECT
- [Documents](documents.md) — Document mutations trigger events
- [NodeDB-Lite](lite.md) — CRDT sync uses the same CDC infrastructure

[Back to docs](README.md)
