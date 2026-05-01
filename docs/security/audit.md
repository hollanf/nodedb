# Audit Log & Change Tracking

NodeDB maintains a tamper-evident, hash-chained audit log. Every entry includes a SHA-256 hash of the previous entry — if any record is modified or deleted, the chain breaks.

## Viewing the Audit Log

```sql
-- Show recent entries (superuser only)
SHOW AUDIT LOG;
SHOW AUDIT LOG LIMIT 50;
```

**Columns:** `seq`, `timestamp`, `event`, `tenant_id`, `source`, `detail`

Each entry also records `auth_user_id`, `auth_user_name`, and `session_id` for correlation.

## Audit Events

| Event               | Level    | Description                 |
| ------------------- | -------- | --------------------------- |
| `AuthSuccess`       | Minimal  | Successful authentication   |
| `AuthFailure`       | Minimal  | Failed login attempt        |
| `AuthzDenied`       | Minimal  | Permission denied           |
| `PrivilegeChange`   | Standard | GRANT, REVOKE, role changes |
| `SessionConnect`    | Standard | New connection              |
| `SessionDisconnect` | Standard | Connection closed           |
| `AdminAction`       | Standard | DDL, config changes         |
| `TenantCreated`     | Standard | New tenant                  |
| `TenantDeleted`     | Standard | Tenant removed              |
| `SnapshotBegin/End` | Standard | Backup lifecycle            |
| `RestoreBegin/End`  | Standard | Restore lifecycle           |
| `CertRotation`      | Standard | TLS cert rotated            |
| `KeyRotation`       | Standard | Encryption key rotated      |
| `NodeJoined/Left`   | Standard | Cluster membership          |
| `QueryExec`         | Full     | Every query executed        |
| `RlsDenied`         | Full     | Row filtered by RLS         |
| `RowChange`         | Forensic | Individual row mutations    |

## Audit Levels

```toml
# nodedb.toml
[audit]
level = "standard"
```

| Level      | What's recorded                                |
| ---------- | ---------------------------------------------- |
| `minimal`  | Auth events only (login, failure, denial)      |
| `standard` | + admin actions, DDL, sessions, config changes |
| `full`     | + every query, RLS denials                     |
| `forensic` | + row-level mutations, CRDT deltas             |

Higher levels include everything from lower levels.

## Hash Chain Integrity

Every `AuditEntry` contains `prev_hash` — the SHA-256 of the previous entry. This creates a tamper-evident chain verified on startup. If an entry is modified or deleted, the chain breaks and is flagged.

## SIEM Export

Export audit events to external security information and event management systems via CDC webhook:

```sql
CREATE CHANGE STREAM audit_export ON _system.audit
    DELIVERY WEBHOOK 'https://siem.example.com/ingest'
    WITH (format = 'json', hmac_secret = 'your-secret');
```

HMAC signatures allow the receiving system to verify event authenticity.

---

# Document Change Tracking

NodeDB does NOT have automatic `updated_at` / `created_at` columns like PostgreSQL. Here's how to track changes:

## Typeguard DEFAULT + VALUE (Schemaless)

The recommended approach for schemaless collections:

```sql
CREATE TYPEGUARD ON users (
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP VALUE now()
);
```

- `DEFAULT now()` — set once on INSERT, never overwritten
- `VALUE now()` — set on every write (INSERT, UPSERT, UPDATE)

```sql
INSERT INTO users { id: 'u1', name: 'Alice' };
-- created_at = 2026-04-10T..., updated_at = 2026-04-10T...

UPSERT INTO users { id: 'u1', name: 'Alice Updated' };
-- created_at unchanged, updated_at refreshed
```

## Strict Schema DEFAULT

For strict collections:

```sql
CREATE COLLECTION orders (
    id TEXT PRIMARY KEY DEFAULT gen_uuid_v7(),
    customer TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
) WITH (engine='document_strict');

-- created_at auto-filled on INSERT
INSERT INTO orders (customer) VALUES ('Alice');
```

## BEFORE Triggers

For custom logic:

```sql
CREATE TRIGGER set_timestamps BEFORE INSERT ON events FOR EACH ROW
BEGIN
    SET NEW.created_at = now();
    SET NEW.updated_at = now();
END;

CREATE TRIGGER update_timestamp BEFORE UPDATE ON events FOR EACH ROW
BEGIN
    SET NEW.updated_at = now();
END;
```

## CDC Change Streams

For full change history without modifying documents:

```sql
CREATE CHANGE STREAM user_changes ON users;

-- Consume changes (returns old + new values for each mutation)
SELECT * FROM CHANGES('user_changes') LIMIT 10;
```

Change streams capture INSERT, UPDATE, and DELETE events with full before/after payloads. Use consumer groups for durable, at-least-once processing.

[Back to security](README.md)
