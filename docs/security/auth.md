# Authentication

NodeDB supports multiple authentication methods, usable together.

## Password Auth (SCRAM-SHA-256)

Compatible with any PostgreSQL client (`psql`, `pgcli`, application drivers).

```sql
-- Create a user with password
CREATE USER alice WITH PASSWORD 'strong_password_here';

-- Create with a specific role
CREATE USER bob WITH PASSWORD 'secret' ROLE readonly;

-- Create for a specific tenant (superuser only)
CREATE USER service_bot WITH PASSWORD 'key' ROLE readwrite TENANT 42;

-- View all users
SHOW USERS;
```

**Roles:** `readonly`, `readwrite`, `admin`, `tenant_admin`, `superuser`

Connect via psql:

```bash
psql -h localhost -p 6432 -U alice
```

## API Keys

For service-to-service communication without passwords.

```sql
-- Create an API key (returns the key once — store it securely)
CREATE API KEY 'my-service' ROLE readwrite;

-- Revoke
DROP API KEY 'my-service';
```

Use in HTTP requests:

```bash
curl -H "Authorization: Bearer <api-key>" http://localhost:6480/v1/query
```

## JWKS (JWT Auto-Discovery)

Multi-provider support for Auth0, Clerk, Supabase, Firebase, Keycloak, and Cognito.

Configure in `nodedb.toml`:

```toml
[auth.jwks]
providers = [
    { issuer = "https://your-domain.auth0.com/", audience = "your-api" },
]
```

JWT claims map to `$auth.*` session variables:

| JWT Claim         | Session Variable | Usage                                            |
| ----------------- | ---------------- | ------------------------------------------------ |
| `sub`             | `$auth.id`       | RLS: `WHERE user_id = $auth.id`                  |
| `role` / custom   | `$auth.role`     | RLS: `WHERE $auth.role = 'admin'`                |
| `org_id` / custom | `$auth.org_id`   | RLS: `WHERE org_id = $auth.org_id`               |
| `scope`           | `$auth.scopes`   | RLS: `WHERE $auth.scopes CONTAINS 'read:orders'` |

Supported algorithms: ES256, ES384, RS256. Built-in JWKS cache with disk fallback and circuit breaker for provider outages.

## mTLS (Mutual TLS)

For zero-trust environments. Both client and server present certificates.

Configure in `nodedb.toml`:

```toml
[tls]
cert = "/path/to/server.crt"
key = "/path/to/server.key"
client_ca = "/path/to/ca.crt"     # enables mTLS
crl = "/path/to/revocation.crl"   # optional CRL
```

## Auth Priority

When multiple methods are configured, NodeDB checks in order:

1. mTLS (if client certificate present)
2. JWT Bearer token (if `Authorization` header present)
3. API key (if `Authorization: Bearer` matches a key)
4. SCRAM-SHA-256 (pgwire password auth)

[Back to security](README.md)
