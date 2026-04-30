//! `ALTER COLLECTION accounts ADD COLUMN balance DECIMAL DEFAULT 0 AS MATERIALIZED_SUM ...`
//! — ADD COLUMN variant that binds a computed balance to another collection's
//! per-row contribution. Atomically maintained on INSERT into the source side.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::expr_eval::SqlExpr;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

pub fn add_materialized_sum(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    target_collection: &str,
    target_column: &str,
    source_collection: &str,
    join_column: &str,
    value_expr: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();

    let expr = parse_value_expression(value_expr)?;

    let def = crate::control::security::catalog::types::MaterializedSumDef {
        target_collection: target_collection.to_string(),
        target_column: target_column.to_string(),
        source_collection: source_collection.to_string(),
        join_column: join_column.to_string(),
        value_expr: expr,
    };

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, target_collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| {
            sqlstate_error(
                "42P01",
                &format!("collection '{target_collection}' not found"),
            )
        })?;

    if coll
        .materialized_sums
        .iter()
        .any(|m| m.target_column == target_column)
    {
        return Err(sqlstate_error(
            "42710",
            &format!("materialized sum already defined for column '{target_column}'"),
        ));
    }

    coll.materialized_sums.push(def);
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0 {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    state.schema_version.bump();

    state.audit_record(
        AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ADD MATERIALIZED_SUM {target_column} on {target_collection}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Convert a pre-validated value expression string into [`SqlExpr`].
///
/// The parser in `nodedb-sql` already rejects complex expressions and strips
/// any collection prefix, so only simple alphanumeric column names arrive here.
fn parse_value_expression(value_expr: &str) -> PgWireResult<SqlExpr> {
    if value_expr.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Ok(SqlExpr::Column(value_expr.to_string()))
    } else {
        Err(sqlstate_error(
            "0A000",
            &format!(
                "complex VALUE expressions not yet supported; use a pre-computed column. Got: '{value_expr}'"
            ),
        ))
    }
}
