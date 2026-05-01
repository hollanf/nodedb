# Row-Level Security (RLS)

RLS policies filter rows based on the authenticated user's context. Policies apply transparently to all queries — no application code changes needed. Works across all eight engines.

## Creating Policies

```sql
-- Users see only their own orders
CREATE RLS POLICY user_orders ON orders FOR READ
    USING (customer_id = $auth.id);

-- Users can only modify their own data
CREATE RLS POLICY user_write ON orders FOR WRITE
    USING (customer_id = $auth.id);

-- Combined read + write
CREATE RLS POLICY own_data ON profiles FOR ALL
    USING (user_id = $auth.id);

-- Org-scoped access
CREATE RLS POLICY org_access ON projects FOR ALL
    USING (org_id = $auth.org_id);

-- Role-based bypass: admins see everything
CREATE RLS POLICY admin_bypass ON orders FOR READ
    USING ($auth.role = 'admin' OR customer_id = $auth.id);
```

## Policy Types

| Type    | Applies to             |
| ------- | ---------------------- |
| `READ`  | SELECT queries         |
| `WRITE` | INSERT, UPDATE, DELETE |
| `ALL`   | Both read and write    |

## Permissive vs Restrictive

By default, multiple policies on the same collection are **OR-combined** (permissive). If ANY policy passes, the row is visible.

Use `RESTRICTIVE` to AND-combine. ALL restrictive policies must pass.

```sql
-- Both must pass: same org AND not deleted
CREATE RLS POLICY org_filter ON docs FOR READ
    USING (org_id = $auth.org_id) RESTRICTIVE;

CREATE RLS POLICY not_deleted ON docs FOR READ
    USING (status != 'deleted') RESTRICTIVE;
```

## Session Variables

RLS predicates use `$auth.*` variables populated from the authenticated session (JWT claims, DB user, API key context):

| Variable          | Source                    | Example                               |
| ----------------- | ------------------------- | ------------------------------------- |
| `$auth.id`   | JWT `sub` or DB user ID   | `customer_id = $auth.id`         |
| `$auth.role`      | JWT role claim or DB role | `$auth.role = 'admin'`                |
| `$auth.org_id`    | JWT org claim             | `org_id = $auth.org_id`               |
| `$auth.tenant_id` | Tenant context            | `tenant_id = $auth.tenant_id`         |
| `$auth.scopes`    | JWT scopes                | `$auth.scopes CONTAINS 'read:orders'` |

## Managing Policies

```sql
-- View all policies
SHOW RLS POLICIES;

-- Drop a policy
DROP RLS POLICY user_orders ON orders;
```

## How It Works

1. User authenticates (SCRAM, JWT, API key, mTLS)
2. `$auth.*` variables populated from credentials
3. On every query, RLS predicates injected into WHERE clause at plan time
4. Data Plane never sees unfiltered data
5. Applies to all engines: document, vector, graph, columnar, KV, FTS, spatial

## Cross-Engine Behavior

RLS filters are injected at the SQL plan level, before engine-specific dispatch. This means:

- **Vector search**: `SEARCH articles USING VECTOR(...)` respects RLS — unauthorized vectors excluded from results
- **Graph traversal**: `GRAPH TRAVERSE FROM 'a'` skips edges to nodes the user can't see
- **FTS**: `text_match(body, 'query')` returns only documents passing the RLS predicate
- **KV**: `SELECT * FROM cache WHERE key = 'k1'` returns empty if RLS blocks the row

[Back to security](README.md)
