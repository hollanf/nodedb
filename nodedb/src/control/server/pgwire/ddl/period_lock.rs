//! DDL handlers for ADD/DROP PERIOD LOCK.
//!
//! Syntax:
//! ```sql
//! ALTER COLLECTION journal_entries
//!     ADD PERIOD LOCK ON fiscal_period
//!     REFERENCES fiscal_periods(period_key)
//!     STATUS status
//!     ALLOW WRITE WHEN status IN ('OPEN', 'ADJUSTING');
//!
//! ALTER COLLECTION journal_entries DROP PERIOD LOCK;
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::catalog::PeriodLockDef;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `ALTER COLLECTION x ADD PERIOD LOCK ON col REFERENCES table(pk) ...`
pub fn add_period_lock(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.to_uppercase();

    // ALTER COLLECTION <name> ADD PERIOD LOCK ON <col> REFERENCES <table>(<pk>) ...
    let name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    // Find the period column: "ON <col>"
    let on_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| sqlstate_error("42601", "ADD PERIOD LOCK requires ON <column>"))?;
    let period_column = parts
        .get(on_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing column name after ON"))?
        .to_lowercase();

    // Find REFERENCES <table>(<pk>)
    let ref_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("REFERENCES"))
        .ok_or_else(|| {
            sqlstate_error("42601", "ADD PERIOD LOCK requires REFERENCES <table>(<pk>)")
        })?;
    let ref_spec = parts
        .get(ref_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing table(pk) after REFERENCES"))?;
    let (ref_table, ref_pk) = parse_table_pk(ref_spec)?;

    // Find STATUS <col> (optional, defaults to "status").
    let status_column =
        if let Some(si) = parts.iter().position(|p| p.eq_ignore_ascii_case("STATUS")) {
            parts
                .get(si + 1)
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "status".into())
        } else {
            "status".into()
        };

    // Find ALLOW WRITE WHEN status IN ('OPEN', 'ADJUSTING')
    let allowed_statuses = parse_allowed_statuses(&upper)?;

    let def = PeriodLockDef {
        period_column,
        ref_table,
        ref_pk,
        status_column,
        allowed_statuses,
    };

    // Load the collection, update, and re-persist.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    coll.period_lock = Some(def);

    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.schema_version.bump();

    // Audit: ConfigChange → Standard audit level.
    state.audit_record(
        crate::control::security::audit::AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ADD PERIOD LOCK on {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Handle `ALTER COLLECTION x DROP PERIOD LOCK`.
pub fn drop_period_lock(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();
    let name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    coll.period_lock = None;

    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.schema_version.bump();

    // Audit: ConfigChange → Standard audit level.
    state.audit_record(
        crate::control::security::audit::AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP PERIOD LOCK on {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Parse `table(pk)` into `(table, pk)`.
fn parse_table_pk(spec: &str) -> PgWireResult<(String, String)> {
    let spec = spec.trim();
    if let Some(paren_start) = spec.find('(') {
        let table = spec[..paren_start].trim().to_lowercase();
        let pk = spec[paren_start + 1..]
            .trim_end_matches(')')
            .trim()
            .to_lowercase();
        if table.is_empty() || pk.is_empty() {
            return Err(sqlstate_error(
                "42601",
                "REFERENCES requires table(pk) format",
            ));
        }
        Ok((table, pk))
    } else {
        Err(sqlstate_error(
            "42601",
            "REFERENCES requires table(pk) format, e.g. fiscal_periods(period_key)",
        ))
    }
}

/// Parse `ALLOW WRITE WHEN status IN ('OPEN', 'ADJUSTING')`.
fn parse_allowed_statuses(upper: &str) -> PgWireResult<Vec<String>> {
    // Find "IN" keyword followed by "(" with optional whitespace.
    let in_kw = upper.find("IN").ok_or_else(|| {
        sqlstate_error(
            "42601",
            "ADD PERIOD LOCK requires ALLOW WRITE WHEN ... IN ('status1', ...)",
        )
    })?;
    let after_in = upper[in_kw + 2..].trim_start();
    let after = after_in
        .strip_prefix('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '(' after IN keyword"))?;
    let Some(close) = after.find(')') else {
        return Err(sqlstate_error("42601", "missing closing ')' in IN clause"));
    };
    let inner = &after[..close];
    let statuses: Vec<String> = inner
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if statuses.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "IN clause must list at least one status value",
        ));
    }

    Ok(statuses)
}

fn sqlstate_error(code: &str, msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}
