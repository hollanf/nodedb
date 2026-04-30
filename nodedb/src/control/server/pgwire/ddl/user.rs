use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::AlterUserOp;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::{parse_role, require_admin, sqlstate_error};

/// CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]
pub fn create_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    username: &str,
    password: &str,
    role_name: Option<&str>,
    tenant_id_override: Option<u64>,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create users")?;

    if username.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]",
        ));
    }

    if password.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "password must be a single-quoted string",
        ));
    }

    let role = role_name.map(parse_role).unwrap_or(Role::ReadWrite);
    let tenant_id = if let Some(tid) = tenant_id_override {
        if !identity.is_superuser {
            return Err(sqlstate_error("42501", "only superuser can assign tenants"));
        }
        TenantId::new(tid)
    } else {
        identity.tenant_id
    };

    // Build the full `StoredUser` locally (hash + salt + user_id).
    // Followers cannot reproduce the random salt, so this step
    // MUST happen on the proposer node. The computed record is
    // then replicated verbatim.
    let stored = state
        .credentials
        .prepare_user(username, password, tenant_id, vec![role])
        .map_err(|e| sqlstate_error("42710", &e.to_string()))?;

    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        // Single-node / no-cluster fallback: install into the
        // in-memory cache so subsequent reads see the user.
        // Persist to redb when a catalog is wired up — the
        // catalog write is best-effort durability, not a gate
        // on the cache update. Test fixtures (and any future
        // fully-in-memory deployment) can run without a redb
        // catalog and still get correct read-after-write.
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_user(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state.credentials.install_replicated_user(&stored);
    } else {
        // Cluster mode: `propose_catalog_entry` waits for the
        // entry to be applied on THIS node, which runs the
        // synchronous post_apply (`install_replicated_user`)
        // inline BEFORE the applied-index watermark bumps. So if
        // our entry really committed, `get_user` must see it now.
        //
        // If `get_user` returns None, the Raft log entry at the
        // index our leader assigned has been truncated and
        // overwritten with a noop from a new leader term (a known
        // Raft subtlety: `propose` returns the assigned log index
        // without waiting for commit; if leadership changes
        // before the quorum ack, the entry is dropped). Return a
        // retryable error so `exec_ddl_on_any_leader` re-proposes
        // on the next attempt against whoever is now leader.
        if state.credentials.get_user(username).is_none() {
            return Err(sqlstate_error(
                "40001",
                "transient: metadata entry truncated by leader change, retry",
            ));
        }
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(tenant_id),
        &identity.username,
        &format!("created user '{username}' in tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE USER"))])
}

/// ALTER USER <name> ... — typed dispatch for all AlterUserOp forms.
pub fn alter_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    username: &str,
    op: &AlterUserOp,
) -> PgWireResult<Vec<Response>> {
    if username.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER USER <name> SET PASSWORD '<password>' | SET ROLE <role> | MUST CHANGE PASSWORD | PASSWORD NEVER EXPIRES | PASSWORD EXPIRES ...",
        ));
    }

    // Users can change their own password; admin required for anything else.
    let is_self = username == identity.username;
    let can_alter = is_self || identity.is_superuser || identity.has_role(&Role::TenantAdmin);

    match op {
        AlterUserOp::SetPassword { password } => {
            if !can_alter {
                return Err(sqlstate_error(
                    "42501",
                    "permission denied: can only alter your own user, or be superuser/tenant_admin",
                ));
            }
            if password.is_empty() {
                return Err(sqlstate_error(
                    "42601",
                    "password must be a non-empty single-quoted string",
                ));
            }
            let stored = state
                .credentials
                .prepare_user_update(username, Some(password.as_str()), None)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            propose_and_install(state, stored)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("changed password for user '{username}'"),
            );
            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }

        AlterUserOp::SetRole { role } => {
            if is_self && !identity.is_superuser {
                return Err(sqlstate_error("42501", "cannot change your own role"));
            }
            require_admin(identity, "change roles")?;
            if role.is_empty() {
                return Err(sqlstate_error("42601", "expected role name after SET ROLE"));
            }
            let parsed_role: Role = parse_role(role);
            let stored = state
                .credentials
                .prepare_user_update(username, None, Some(vec![parsed_role.clone()]))
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            propose_and_install(state, stored)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set role '{parsed_role}' for user '{username}'"),
            );
            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }

        AlterUserOp::MustChangePassword => {
            require_admin(identity, "set must_change_password")?;
            let stored = state
                .credentials
                .prepare_set_must_change_password(username, true)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            propose_and_install(state, stored)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set must_change_password for user '{username}'"),
            );
            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }

        AlterUserOp::PasswordNeverExpires => {
            require_admin(identity, "set password expiry")?;
            let stored = state
                .credentials
                .prepare_set_password_expires_at(username, 0)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            propose_and_install(state, stored)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD NEVER EXPIRES for user '{username}'"),
            );
            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }

        AlterUserOp::PasswordExpiresAt { iso8601 } => {
            require_admin(identity, "set password expiry")?;
            let expires_at = parse_iso8601_to_unix(iso8601).map_err(|e| {
                sqlstate_error(
                    "22007",
                    &format!("invalid ISO-8601 datetime '{iso8601}': {e}"),
                )
            })?;
            let stored = state
                .credentials
                .prepare_set_password_expires_at(username, expires_at)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            propose_and_install(state, stored)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD EXPIRES '{iso8601}' for user '{username}'"),
            );
            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }

        AlterUserOp::PasswordExpiresInDays { days } => {
            require_admin(identity, "set password expiry")?;
            if *days == 0 {
                return Err(sqlstate_error(
                    "22003",
                    "PASSWORD EXPIRES IN requires a positive day count",
                ));
            }
            let expires_at = crate::control::security::time::now_secs() + (*days as u64) * 86400;
            let stored = state
                .credentials
                .prepare_set_password_expires_at(username, expires_at)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            propose_and_install(state, stored)?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD EXPIRES IN {days} DAYS for user '{username}'"),
            );
            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }
    }
}

/// Propose a `StoredUser` via Raft and install it locally on single-node.
fn propose_and_install(
    state: &SharedState,
    stored: crate::control::security::catalog::StoredUser,
) -> PgWireResult<()> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_user(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state.credentials.install_replicated_user(&stored);
    }
    Ok(())
}

/// Parse an ISO-8601 datetime string to a Unix timestamp (seconds).
///
/// Accepts formats like:
/// - `2026-12-31T00:00:00Z`
/// - `2026-12-31T00:00:00+00:00`
/// - `2026-12-31` (interpreted as midnight UTC)
fn parse_iso8601_to_unix(s: &str) -> Result<u64, String> {
    // Parse as RFC 3339 or ISO 8601 date. Manually handle common forms
    // without pulling in an external datetime crate.
    let s = s.trim();

    // Date-only: YYYY-MM-DD
    if s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
        return parse_date_to_unix(s);
    }

    // Full datetime: YYYY-MM-DDTHH:MM:SSZ or +offset
    if s.len() >= 19 {
        let date_part = &s[..10];
        let ts_secs = parse_date_to_unix(date_part)?;
        // Parse time part: THH:MM:SS
        let time_part = &s[11..];
        let clean = time_part
            .trim_end_matches('Z')
            .trim_end_matches(|c: char| c == '+' || c == '-' || c.is_ascii_digit() || c == ':');
        let hms: Vec<&str> = clean.split(':').collect();
        let h: u64 = hms.first().and_then(|s| s.trim().parse().ok()).unwrap_or(0);
        let m: u64 = hms.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(0);
        let sec: u64 = hms
            .get(2)
            .and_then(|s| {
                let raw = s.trim().trim_end_matches(|c: char| !c.is_ascii_digit());
                raw.parse().ok()
            })
            .unwrap_or(0);
        return Ok(ts_secs + h * 3600 + m * 60 + sec);
    }

    Err(format!("unrecognised datetime format: '{s}'"))
}

/// Parse YYYY-MM-DD to midnight UTC Unix timestamp.
fn parse_date_to_unix(s: &str) -> Result<u64, String> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(format!("expected YYYY-MM-DD, got '{s}'"));
    }
    let y: i64 = parts[0].parse().map_err(|_| format!("bad year in '{s}'"))?;
    let mo: u64 = parts[1]
        .parse()
        .map_err(|_| format!("bad month in '{s}'"))?;
    let d: u64 = parts[2].parse().map_err(|_| format!("bad day in '{s}'"))?;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return Err(format!("date out of range in '{s}'"));
    }
    // Simplified Julian Day → Unix: good for dates after 1970-01-01.
    // Uses the civil calendar formula.
    let days = days_since_epoch(y, mo, d)?;
    Ok(days * 86400)
}

fn days_since_epoch(y: i64, mo: u64, d: u64) -> Result<u64, String> {
    // JDN formula for Gregorian calendar
    let a = (14_i64 - mo as i64) / 12;
    let yr = y + 4800 - a;
    let m = mo as i64 + 12 * a - 3;
    let jdn = d as i64 + (153 * m + 2) / 5 + 365 * yr + yr / 4 - yr / 100 + yr / 400 - 32045;
    // Unix epoch = 1970-01-01 = JDN 2440588
    let unix_days = jdn - 2_440_588;
    if unix_days < 0 {
        return Err(format!("date before Unix epoch: {y}-{mo:02}-{d:02}"));
    }
    Ok(unix_days as u64)
}

/// DROP USER <name>
pub fn drop_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop users")?;

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP USER <name>"));
    }

    let username = parts[2];

    if username == identity.username {
        return Err(sqlstate_error("42501", "cannot drop your own user"));
    }

    // Look up user's tenant before dropping (for ownership reassignment).
    let user_tenant = state
        .credentials
        .get_user(username)
        .map(|u| u.tenant_id)
        .unwrap_or(identity.tenant_id);

    // Pre-check existence so a DROP USER on a missing user is a
    // clean error that doesn't touch raft.
    let exists_before = state.credentials.get_user(username).is_some();
    if !exists_before {
        return Err(sqlstate_error(
            "42704",
            &format!("user '{username}' does not exist"),
        ));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeactivateUser {
        username: username.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    let dropped = if log_index == 0 {
        // Single-node fallback.
        state
            .credentials
            .deactivate_user(username)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
    } else {
        // Cluster mode: the raft entry committed, so the
        // deactivation WILL be applied on every node. The
        // `post_apply` hook that updates the local in-memory
        // cache runs in a spawned tokio task and may not be
        // visible by the time this function returns — trust the
        // log index rather than re-reading the cache.
        true
    };

    if dropped {
        // Reassign owned collections to the tenant_admin of the
        // user's tenant. Mutating the parent `StoredCollection`
        // and re-proposing `PutCollection` is the durable path —
        // a bare `PutOwner` would be silently overwritten the
        // next time anyone re-proposed the parent (see
        // `pgwire/ddl/ownership.rs` for the same pattern).
        let admin_name = format!("{}_admin", user_tenant.as_u64());
        let grants = state.permissions.grants_for(&format!("user:{username}"));
        if let Some(catalog) = state.credentials.catalog() {
            for grant in &grants {
                let Some(owner_obj) = extract_collection_from_target(&grant.target) else {
                    continue;
                };
                if state
                    .permissions
                    .get_owner("collection", user_tenant, owner_obj)
                    .as_deref()
                    != Some(username)
                {
                    continue;
                }
                let mut stored = match catalog.get_collection(user_tenant.as_u64(), owner_obj) {
                    Ok(Some(c)) => c,
                    _ => continue,
                };
                stored.owner = admin_name.clone();
                let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(
                    stored.clone(),
                ));
                if let Ok(idx) =
                    crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
                    && idx == 0
                {
                    let _ = catalog.put_collection(&stored);
                    state.permissions.install_replicated_owner(
                        &crate::control::security::catalog::StoredOwner {
                            object_type: "collection".into(),
                            object_name: stored.name.clone(),
                            tenant_id: stored.tenant_id,
                            owner_username: stored.owner.clone(),
                        },
                    );
                }
            }
        }

        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped user '{username}' (ownership reassigned to '{admin_name}')"),
        );
        Ok(vec![Response::Execution(Tag::new("DROP USER"))])
    } else {
        Err(sqlstate_error(
            "42704",
            &format!("user '{username}' does not exist"),
        ))
    }
}

/// Extract collection name from a permission target like "collection:1:users".
fn extract_collection_from_target(target: &str) -> Option<&str> {
    let parts: Vec<&str> = target.splitn(3, ':').collect();
    if parts.len() == 3 && parts[0] == "collection" {
        Some(parts[2])
    } else {
        None
    }
}
