# Security

NodeDB has a defense-in-depth security model covering authentication, authorization, encryption, and audit. This overview covers the main features — full configuration details will be in the API documentation.

## Authentication

Multiple auth methods, usable together:

- **JWKS auto-discovery** — Multi-provider support (Auth0, Clerk, Supabase, Firebase, Keycloak, Cognito). ES256/ES384/RS256 algorithms. Built-in cache with disk fallback and circuit breaker for provider outages.
- **mTLS** — Mutual TLS with certificate revocation list (CRL) support
- **API keys** — For service-to-service communication
- **SCRAM-SHA-256** — Password-based auth compatible with PostgreSQL clients

## Authorization (RBAC + RLS)

**Role-Based Access Control** — Define roles with fine-grained permissions on collections, fields, and operations.

**Row-Level Security** — Policies filter data based on the authenticated user's context. RLS predicates use `$auth.*` session variables (user ID, roles, org, scopes) and are substituted at query plan time — they apply across all seven engines.

```sql
-- Only show users their own data
DEFINE RLS ON orders WHERE customer_id = $auth.user_id;

-- Org-scoped access
DEFINE RLS ON projects WHERE org_id = $auth.org_id;

-- Debug: see the full permission resolution chain
EXPLAIN PERMISSION SELECT ON orders FOR CURRENT USER;
```

## Scopes and Organizations

- **Scopes** — Define and grant fine-grained permissions with time-bound expiry and grace periods
- **Organizations** — Membership management with JIT provisioning from JWT claims. Tenant isolation.
- **Impersonation & delegation** — Time-limited, scope-subset impersonation for support workflows

## Rate Limiting and Metering

- **Rate limiting** — Token bucket algorithm with per-user, per-org, and per-API-key hierarchy and tier resolution
- **Usage metering** — Per-operation cost tracking with quotas (hard/soft/throttle enforcement)
- **Tenant ceilings** — Hard resource limits per tenant

## Security Controls

- **Blacklists** — Block users and IP ranges (CIDR) with TTL. Kill active sessions on blacklist.
- **Conditional permissions** — Permissions gated on temporal windows, MFA status, IP range, or device trust level
- **Risk scoring** — Signal combination with adaptive thresholds for step-up auth
- **Emergency lockdown** — Two-party authorization with break-glass key for incident response
- **Column-level redaction** — Mask or hash-pseudonymize sensitive fields per role

## Encryption

- **At rest** — AES-256-XTS for data volumes, AES-256-GCM for WAL segments, per-file data encryption keys
- **In transit** — TLS for all protocols (pgwire, HTTP, WebSocket, native)
- **Lite devices** — AES-256-GCM + Argon2id key derivation for on-device encryption

## Audit

- **Hash-chain audit log** — Tamper-evident append-only log of all auth and access events
- **SIEM export** — CDC-based webhook export with HMAC signature verification
- **Auth observability** — Prometheus metrics for auth events, anomaly detection

[Back to docs](README.md)
