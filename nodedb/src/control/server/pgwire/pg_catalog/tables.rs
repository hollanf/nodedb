//! Virtual table row generators for each pg_catalog table.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use nodedb_types::columnar::ColumnType;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::{bool_field, int4_field, int8_field, text_field};
use crate::control::state::SharedState;

/// `pg_database` — one row: the current database.
pub fn pg_database() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("oid"),
        text_field("datname"),
        text_field("datdba"),
        text_field("encoding"),
    ]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder.encode_field(&1i64)?;
    encoder.encode_field(&"nodedb")?;
    encoder.encode_field(&"nodedb")?;
    encoder.encode_field(&"UTF8")?;
    let rows = vec![Ok(encoder.take_row())];
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `pg_namespace` — schemas: `public` + `pg_catalog`.
pub fn pg_namespace() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("oid"),
        text_field("nspname"),
        int8_field("nspowner"),
    ]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let mut rows = Vec::new();

    encoder.encode_field(&11i64)?;
    encoder.encode_field(&"pg_catalog")?;
    encoder.encode_field(&10i64)?;
    rows.push(Ok(encoder.take_row()));

    encoder.encode_field(&2200i64)?;
    encoder.encode_field(&"public")?;
    encoder.encode_field(&10i64)?;
    rows.push(Ok(encoder.take_row()));

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `pg_type` — common Postgres type OIDs that client drivers need.
pub fn pg_type() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("oid"),
        text_field("typname"),
        int8_field("typnamespace"),
        int4_field("typlen"),
        text_field("typtype"),
    ]);

    let types: &[(i64, &str, i32, &str)] = &[
        (16, "bool", 1, "b"),
        (17, "bytea", -1, "b"),
        (20, "int8", 8, "b"),
        (21, "int2", 2, "b"),
        (23, "int4", 4, "b"),
        (25, "text", -1, "b"),
        (114, "json", -1, "b"),
        (700, "float4", 4, "b"),
        (701, "float8", 8, "b"),
        (1021, "float4[]", -1, "b"),
        (1043, "varchar", -1, "b"),
        (1082, "date", 4, "b"),
        (1114, "timestamp", 8, "b"),
        (1184, "timestamptz", 8, "b"),
        (1186, "interval", 16, "b"),
        (1700, "numeric", -1, "b"),
        (2950, "uuid", 16, "b"),
        (3802, "jsonb", -1, "b"),
    ];

    let mut rows = Vec::with_capacity(types.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for &(oid, name, len, typtype) in types {
        encoder.encode_field(&oid)?;
        encoder.encode_field(&name)?;
        encoder.encode_field(&11i64)?;
        encoder.encode_field(&len)?;
        encoder.encode_field(&typtype)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `pg_class` — one row per active collection (mapped as relation).
pub fn pg_class(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("oid"),
        text_field("relname"),
        int8_field("relnamespace"),
        text_field("relkind"),
        int8_field("relowner"),
    ]);

    let collections = load_collections(state, identity);

    let mut rows = Vec::with_capacity(collections.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for (i, coll) in collections.iter().enumerate() {
        let oid = 16384i64 + i as i64;
        encoder.encode_field(&oid)?;
        encoder.encode_field(&coll.name.as_str())?;
        encoder.encode_field(&2200i64)?;
        encoder.encode_field(&"r")?;
        encoder.encode_field(&10i64)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `pg_attribute` — one row per field in strict-schema collections.
pub fn pg_attribute(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("attrelid"),
        text_field("attname"),
        int8_field("atttypid"),
        int4_field("attnum"),
        int4_field("attlen"),
        bool_field("attnotnull"),
    ]);

    let collections = load_collections(state, identity);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for (i, coll) in collections.iter().enumerate() {
        let rel_oid = 16384i64 + i as i64;
        for (col_num, (field_name, field_type)) in coll.fields.iter().enumerate() {
            let type_oid = field_type_to_oid(field_type);
            encoder.encode_field(&rel_oid)?;
            encoder.encode_field(&field_name.as_str())?;
            encoder.encode_field(&type_oid)?;
            encoder.encode_field(&((col_num + 1) as i32))?;
            encoder.encode_field(&(-1i32))?;
            encoder.encode_field(&false)?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `pg_index` — secondary indexes.
///
/// Returns an empty result set with the correct schema. Structured
/// index metadata is not yet surfaced through `StoredCollection`;
/// once it is, this function will take `(state, identity)` and
/// populate rows from the catalog.
pub fn pg_index() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("indexrelid"),
        int8_field("indrelid"),
        bool_field("indisunique"),
        bool_field("indisprimary"),
    ]);

    let rows: Vec<Result<_, pgwire::error::PgWireError>> = Vec::new();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `pg_authid` — users / roles.
pub fn pg_authid(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("oid"),
        text_field("rolname"),
        bool_field("rolsuper"),
        bool_field("rolcanlogin"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    let users = state.credentials.list_users();
    for (i, user) in users.iter().enumerate() {
        let oid = 10i64 + i as i64;
        let is_super = identity.is_superuser && user == &identity.username;
        encoder.encode_field(&oid)?;
        encoder.encode_field(&user.as_str())?;
        encoder.encode_field(&is_super)?;
        encoder.encode_field(&true)?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

fn load_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Vec<crate::control::security::catalog::types::StoredCollection> {
    let Some(catalog) = state.credentials.catalog() else {
        return Vec::new();
    };
    if identity.is_superuser {
        catalog
            .load_all_collections()
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.is_active)
            .collect()
    } else {
        catalog
            .load_collections_for_tenant(identity.tenant_id.as_u32())
            .unwrap_or_default()
    }
}

fn field_type_to_oid(field_type: &str) -> i64 {
    // Delegate to the canonical ColumnType mapping wherever possible.
    if let Ok(ct) = field_type.parse::<ColumnType>() {
        return ct.to_pg_oid() as i64;
    }
    // Handle legacy / DataFusion aliases that ColumnType::from_str doesn't cover.
    match field_type.to_lowercase().as_str() {
        "int" | "integer" | "int4" => 23,
        "smallint" | "int2" => 21,
        "float" | "float4" | "real" => 700,
        "double" | "float8" => 701,
        "varchar" => 1043,
        "date" => 1082,
        "timestamptz" => 1184,
        _ => 25, // TEXT fallback
    }
}
