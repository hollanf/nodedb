//! DDL handlers for type guard field constraints on schemaless collections.
//!
//! Syntax:
//! ```sql
//! CREATE TYPEGUARD ON users (
//!     email STRING REQUIRED,
//!     age   INT CHECK (age > 0),
//!     bio   STRING|NULL
//! );
//!
//! CREATE OR REPLACE TYPEGUARD ON users ( ... );
//!
//! ALTER TYPEGUARD ON users ADD score FLOAT REQUIRED CHECK (score >= 0.0);
//! ALTER TYPEGUARD ON users DROP email;
//!
//! DROP TYPEGUARD ON users;
//! DROP TYPEGUARD IF EXISTS ON users;
//!
//! SHOW TYPEGUARD ON users;
//! SHOW TYPEGUARDS;
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use crate::control::server::pgwire::types::text_field;

use super::parse::{
    err, extract_collection_name, extract_outer_parens, parse_field_list, parse_single_field,
};

// ── CREATE TYPEGUARD ──────────────────────────────────────────────────────────

/// Handle `CREATE [OR REPLACE] TYPEGUARD ON <collection> ( field TYPE [REQUIRED] [CHECK (expr)], ... )`.
pub fn create_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let or_replace = upper.contains("OR REPLACE");

    let coll_name = extract_collection_name(sql)?;
    let field_list = extract_outer_parens(sql)?;
    let guards = parse_field_list(&field_list)?;

    if guards.is_empty() {
        return Err(err(
            "42601",
            "TYPEGUARD requires at least one field definition",
        ));
    }

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let tenant_id = identity.tenant_id.as_u32();
    let mut coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if !coll.collection_type.is_schemaless() {
        return Err(err(
            "0A000",
            &format!(
                "TYPEGUARD is only supported on schemaless collections; '{coll_name}' is not schemaless"
            ),
        ));
    }

    if !or_replace && !coll.type_guards.is_empty() {
        return Err(err(
            "42710",
            &format!(
                "type guards already exist on '{coll_name}'; use CREATE OR REPLACE TYPEGUARD to overwrite"
            ),
        ));
    }

    coll.type_guards = guards;
    catalog
        .put_collection(&coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("CREATE TYPEGUARD"))])
}

// ── ALTER TYPEGUARD ───────────────────────────────────────────────────────────

/// Handle `ALTER TYPEGUARD ON <collection> ADD field TYPE [REQUIRED] [CHECK (expr)]`.
pub fn alter_typeguard_add(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    let coll_name = extract_collection_name(sql)?;

    let add_pos = upper
        .find(" ADD ")
        .ok_or_else(|| err("42601", "ALTER TYPEGUARD requires ADD <field> <type>"))?;
    let after_add = sql[add_pos + 5..].trim();

    let guard = parse_single_field(after_add)?;

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let tenant_id = identity.tenant_id.as_u32();
    let mut coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if !coll.collection_type.is_schemaless() {
        return Err(err(
            "0A000",
            &format!(
                "TYPEGUARD is only supported on schemaless collections; '{coll_name}' is not schemaless"
            ),
        ));
    }

    if coll.type_guards.iter().any(|g| g.field == guard.field) {
        return Err(err(
            "42710",
            &format!(
                "type guard for field '{}' already exists on '{coll_name}'",
                guard.field
            ),
        ));
    }

    coll.type_guards.push(guard);
    catalog
        .put_collection(&coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("ALTER TYPEGUARD"))])
}

/// Handle `ALTER TYPEGUARD ON <collection> DROP field`.
pub fn alter_typeguard_drop(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    let coll_name = extract_collection_name(sql)?;

    let drop_pos = upper
        .find(" DROP ")
        .ok_or_else(|| err("42601", "ALTER TYPEGUARD requires DROP <field>"))?;
    let field_name = sql[drop_pos + 6..].trim().to_lowercase();

    if field_name.is_empty() {
        return Err(err("42601", "ALTER TYPEGUARD DROP requires a field name"));
    }

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let tenant_id = identity.tenant_id.as_u32();
    let mut coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let before_len = coll.type_guards.len();
    coll.type_guards.retain(|g| g.field != field_name);

    if coll.type_guards.len() == before_len {
        return Err(err(
            "42704",
            &format!("type guard for field '{field_name}' not found on '{coll_name}'"),
        ));
    }

    catalog
        .put_collection(&coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("ALTER TYPEGUARD"))])
}

/// Dispatch `ALTER TYPEGUARD ON <collection> ADD|DROP ...`.
pub fn alter_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    if upper.contains(" ADD ") {
        alter_typeguard_add(state, identity, sql)
    } else if upper.contains(" DROP ") {
        alter_typeguard_drop(state, identity, sql)
    } else {
        Err(err(
            "42601",
            "ALTER TYPEGUARD requires ADD <field> <type> or DROP <field>",
        ))
    }
}

// ── DROP TYPEGUARD ────────────────────────────────────────────────────────────

/// Handle `DROP TYPEGUARD [IF EXISTS] ON <collection>`.
pub fn drop_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let if_exists = upper.contains("IF EXISTS");

    let coll_name = extract_collection_name(sql)?;

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let tenant_id = identity.tenant_id.as_u32();
    let mut coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    if coll.type_guards.is_empty() {
        if if_exists {
            return Ok(vec![Response::Execution(Tag::new("DROP TYPEGUARD"))]);
        }
        return Err(err(
            "42704",
            &format!("no type guards defined on '{coll_name}'"),
        ));
    }

    coll.type_guards.clear();
    catalog
        .put_collection(&coll)
        .map_err(|e| err("XX000", &e.to_string()))?;

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("DROP TYPEGUARD"))])
}

// ── SHOW TYPEGUARD / SHOW TYPEGUARDS ──────────────────────────────────────────

/// Handle `SHOW TYPEGUARD ON <collection>`.
pub fn show_typeguard(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let coll_name = extract_collection_name(sql)?;

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let tenant_id = identity.tenant_id.as_u32();
    let coll = catalog
        .get_collection(tenant_id, &coll_name)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{coll_name}' not found")))?;

    let schema = Arc::new(vec![
        text_field("field"),
        text_field("type"),
        text_field("required"),
        text_field("check"),
    ]);

    let mut rows = Vec::with_capacity(coll.type_guards.len());
    for guard in &coll.type_guards {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&guard.field);
        let _ = encoder.encode_field(&guard.type_expr);
        let _ = encoder.encode_field(&guard.required.to_string());
        let check_str = guard.check_expr.clone().unwrap_or_default();
        let _ = encoder.encode_field(&check_str);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Handle `SHOW TYPEGUARDS` — list all collections with active type guards.
pub fn show_typeguards(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
) -> PgWireResult<Vec<Response>> {
    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let tenant_id = identity.tenant_id.as_u32();
    let collections = catalog
        .load_collections_for_tenant(tenant_id)
        .map_err(|e| err("XX000", &e.to_string()))?;

    let schema = Arc::new(vec![text_field("collection"), text_field("fields")]);

    let mut rows = Vec::new();
    for coll in collections {
        if coll.type_guards.is_empty() {
            continue;
        }
        let field_names: Vec<&str> = coll.type_guards.iter().map(|g| g.field.as_str()).collect();
        let fields_str = field_names.join(", ");
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&coll.name);
        let _ = encoder.encode_field(&fields_str);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
