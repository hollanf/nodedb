# Multi-Tenancy

Each tenant has fully isolated storage, indexes, and security policies. Cross-tenant data access is impossible by design.

## Creating Tenants

```sql
-- Superuser only
CREATE TENANT acme;
```

## Quotas

```sql
-- Set resource limits
ALTER TENANT acme SET QUOTA max_qps = 5000;
ALTER TENANT acme SET QUOTA max_storage_bytes = 53687091200;  -- 50 GB
ALTER TENANT acme SET QUOTA max_connections = 50;

-- Inspect
SHOW TENANT USAGE FOR acme;
SHOW TENANT QUOTA FOR acme;

-- Export for billing
EXPORT USAGE FOR TENANT acme PERIOD '2026-03' FORMAT 'json';
```

## Creating Users for a Tenant

```sql
-- Superuser creates a user scoped to a tenant
CREATE USER alice WITH PASSWORD 'secret' ROLE readwrite TENANT 42;
```

## Tenant Backup/Restore

Backup bytes flow over the pgwire COPY framing. The client redirects
output to (or reads input from) a file under the operator's UID; the
database never touches a caller-named filesystem path.

```sql
-- Grant backup permission
GRANT BACKUP ON TENANT acme TO ops_user;

-- Backup: bytes stream to STDOUT over the wire.
COPY (BACKUP TENANT acme) TO STDOUT;

-- Validate a backup blob before restoring.
COPY tenant_restore(acme) FROM STDIN DRY RUN;

-- Restore.
COPY tenant_restore(acme) FROM STDIN;
```

Backups cover all 7 engines: documents, indexes, vectors, graph edges, KV tables, timeseries, and CRDT state. Payloads are encrypted with AES-256-GCM under the tenant WAL key.

## Tenant Purge (GDPR Erasure)

```sql
-- Remove catalog metadata only (data remains on disk until compaction)
DROP TENANT acme;

-- Remove ALL data across all engines and caches (permanent)
PURGE TENANT acme CONFIRM;
```

`PURGE` is idempotent and safe to re-run after a crash. WAL records are retained (append-only) but are inert after purge.

## Isolation Model

| Layer   | Isolation                                               |
| ------- | ------------------------------------------------------- |
| Storage | Separate key prefixes per tenant in redb                |
| Indexes | Tenant-scoped — no cross-tenant index overlap           |
| WAL     | Per-tenant segments with per-tenant encryption keys     |
| Queries | Tenant ID injected at plan time, enforced in Data Plane |
| RLS     | Policies scoped to tenant                               |
| Audit   | Per-tenant audit entries with tenant_id field           |

[Back to security](README.md)
