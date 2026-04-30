//! `SHOW CONSTRAINTS ON <collection>` — unified view of all constraint kinds.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::text_field;
use crate::control::state::SharedState;

use super::err;

/// Handle `SHOW CONSTRAINTS ON <collection>`.
///
/// Returns a unified view of all constraint kinds:
/// - `transition` — state transition constraints (ON COLUMN ... TRANSITIONS)
/// - `transition_check` — OLD/NEW predicate constraints
/// - `typeguard` — per-field type + CHECK constraints
/// - `check` — general CHECK constraints (may have subqueries)
pub fn show_constraints(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let coll_name = extract_collection_after_on(sql)?;

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let tenant_id = identity.tenant_id.as_u64();
    let coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("kind"),
        text_field("field"),
        text_field("detail"),
    ]);

    let mut rows = Vec::new();

    // State transition constraints.
    for sc in &coll.state_constraints {
        let detail = sc
            .transitions
            .iter()
            .map(|t| {
                if let Some(role) = &t.required_role {
                    format!("'{}' -> '{}' BY ROLE '{}'", t.from, t.to, role)
                } else {
                    format!("'{}' -> '{}'", t.from, t.to)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&sc.name);
        let _ = encoder.encode_field(&"transition");
        let _ = encoder.encode_field(&sc.column);
        let _ = encoder.encode_field(&detail);
        rows.push(Ok(encoder.take_row()));
    }

    // Transition check constraints.
    for tc in &coll.transition_checks {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&tc.name);
        let _ = encoder.encode_field(&"transition_check");
        let _ = encoder.encode_field(&""); // no specific field
        let _ = encoder.encode_field(&format!("{:?}", tc.predicate));
        rows.push(Ok(encoder.take_row()));
    }

    // Typeguard constraints.
    for guard in &coll.type_guards {
        let detail = {
            let mut parts = Vec::new();
            parts.push(format!("type={}", guard.type_expr));
            if guard.required {
                parts.push("REQUIRED".to_string());
            }
            if let Some(check) = &guard.check_expr {
                parts.push(format!("CHECK ({check})"));
            }
            parts.join(", ")
        };
        let auto_name = format!("_guard_{}", guard.field);
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&auto_name);
        let _ = encoder.encode_field(&"typeguard");
        let _ = encoder.encode_field(&guard.field);
        let _ = encoder.encode_field(&detail);
        rows.push(Ok(encoder.take_row()));
    }

    // General CHECK constraints.
    for cc in &coll.check_constraints {
        let kind_str = if cc.has_subquery {
            "check (subquery)"
        } else {
            "check"
        };
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&cc.name);
        let _ = encoder.encode_field(&kind_str);
        let _ = encoder.encode_field(&""); // general, not field-specific
        let _ = encoder.encode_field(&cc.check_sql);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Extract collection name from `SHOW CONSTRAINTS ON <collection>`.
fn extract_collection_after_on(sql: &str) -> PgWireResult<String> {
    let upper = sql.to_uppercase();
    let on_pos = upper
        .find(" ON ")
        .ok_or_else(|| err("42601", "SHOW CONSTRAINTS requires ON <collection>"))?;
    let after = sql[on_pos + 4..].trim();
    let end = after
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(after.len());
    let name = after[..end].trim().to_lowercase();
    if name.is_empty() {
        return Err(err("42601", "missing collection name after ON"));
    }
    Ok(name)
}
