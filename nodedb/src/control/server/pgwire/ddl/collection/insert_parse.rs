//! Shared parsing and helpers for INSERT/UPSERT dispatch.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::KvOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use nodedb_sql::parser::object_literal::parse_object_literal;

use super::super::sql_parse::{parse_sql_value, split_values};
use crate::control::server::pgwire::types::sqlstate_error;

/// Parsed INSERT/UPSERT statement fields.
pub(super) struct ParsedInsert {
    pub coll_name: String,
    pub doc_id: String,
    pub fields: std::collections::HashMap<String, nodedb_types::Value>,
    pub value_bytes: Vec<u8>,
    pub has_returning: bool,
    /// Collection type looked up from the catalog. Drives the write plan.
    pub collection_type: Option<nodedb_types::CollectionType>,
}

pub(super) fn extract_vector_fields(
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Vec<(String, Vec<f32>)> {
    fields
        .iter()
        .filter_map(|(field_name, value)| match value {
            nodedb_types::Value::Array(items) => {
                let vector: Vec<f32> = items
                    .iter()
                    .map(|item| match item {
                        nodedb_types::Value::Float(v) => Some(*v as f32),
                        nodedb_types::Value::Integer(v) => Some(*v as f32),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some((field_name.clone(), vector))
            }
            _ => None,
        })
        .collect()
}

/// Parse an INSERT/UPSERT SQL statement into structured fields.
///
/// `keyword` is the SQL prefix to match (e.g., "INSERT INTO " or "UPSERT INTO ").
/// Returns `None` if the collection has a typed schema (let DataFusion handle it).
pub(super) fn parse_write_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    keyword: &str,
) -> Option<PgWireResult<ParsedInsert>> {
    let upper = sql.to_uppercase();
    let kw_pos = upper.find(keyword)?;
    let after_into = sql[kw_pos + keyword.len()..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name = coll_name_str.to_lowercase();

    // Check if collection is schemaless. Let DataFusion handle typed INSERT
    // with VALUES syntax, but always handle here for:
    // - UPSERT (DataFusion doesn't understand UPSERT)
    // - { } object literal syntax (DataFusion doesn't understand { })
    let tenant_id = identity.tenant_id;
    let is_upsert = keyword.starts_with("UPSERT");
    let after_coll_trimmed = after_into[coll_name_str.len()..].trim_start();
    let is_object_literal = after_coll_trimmed.starts_with('{');
    let mut coll_type: Option<nodedb_types::CollectionType> = None;
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), &coll_name)
    {
        // Skip non-schemaless collections for standard VALUES INSERT (let DataFusion handle).
        // But always handle here for: UPSERT, { } object literal (any collection type).
        if !is_upsert && !is_object_literal && !coll.collection_type.is_schemaless() {
            return None;
        }
        coll_type = Some(coll.collection_type.clone());
    }

    // Determine which form this statement uses: { } object literal or (cols) VALUES (vals).
    let after_coll_name = after_into[coll_name_str.len()..].trim_start();

    if after_coll_name.starts_with('{') {
        return parse_object_literal_form(&coll_name, after_coll_name, &upper, coll_type);
    }

    parse_values_form(sql, &upper, keyword, &coll_name, coll_type)
}

/// Parse the `{ key: val, ... }` object literal form.
fn parse_object_literal_form(
    coll_name: &str,
    after_coll_name: &str,
    upper: &str,
    coll_type: Option<nodedb_types::CollectionType>,
) -> Option<PgWireResult<ParsedInsert>> {
    // Strip trailing semicolon before parsing.
    let obj_str = after_coll_name.trim_end_matches(';').trim_end();

    let fields = match parse_object_literal(obj_str) {
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                "expected object literal starting with '{'",
            )));
        }
        Some(Err(msg)) => {
            return Some(Err(sqlstate_error(
                "42601",
                &format!("object literal parse error: {msg}"),
            )));
        }
        Some(Ok(f)) => f,
    };

    let doc_id = extract_doc_id(&fields);
    let value_bytes = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(fields.clone()))
        .unwrap_or_default();
    let has_returning = upper.contains("RETURNING");

    Some(Ok(ParsedInsert {
        coll_name: coll_name.to_string(),
        doc_id,
        fields,
        value_bytes,
        has_returning,
        collection_type: coll_type,
    }))
}

/// Parse the `(cols) VALUES (vals)` form.
fn parse_values_form(
    sql: &str,
    upper: &str,
    keyword: &str,
    coll_name: &str,
    coll_type: Option<nodedb_types::CollectionType>,
) -> Option<PgWireResult<ParsedInsert>> {
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                &format!("missing column list in {}", keyword.trim()),
            )));
        }
    };
    let values_kw = match upper.find("VALUES") {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES clause"))),
    };
    let first_close = match sql[first_open..values_kw].rfind(')') {
        Some(p) => first_open + p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                "missing closing ) for column list",
            )));
        }
    };
    let cols_str = &sql[first_open + 1..first_close];
    let columns: Vec<&str> = cols_str.split(',').map(|c| c.trim()).collect();

    let after_values = sql[values_kw + 6..].trim_start();
    let vals_open = match after_values.find('(') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES (...)"))),
    };
    let vals_close = match after_values.rfind(')') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing closing ) for VALUES"))),
    };
    let vals_str = &after_values[vals_open + 1..vals_close];
    let values: Vec<&str> = split_values(vals_str);

    if columns.len() != values.len() {
        return Some(Err(sqlstate_error(
            "42601",
            &format!(
                "column count ({}) doesn't match value count ({})",
                columns.len(),
                values.len()
            ),
        )));
    }

    let mut doc_id = String::new();
    let mut fields = std::collections::HashMap::new();

    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id")
            || col.eq_ignore_ascii_case("document_id")
            || col.eq_ignore_ascii_case("key")
        {
            doc_id = val.trim_matches('\'').to_string();
        }
        fields.insert(col.to_string(), parse_sql_value(val));
    }

    if doc_id.is_empty() {
        doc_id = nodedb_types::id_gen::uuid_v7();
    }

    let value_bytes = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(fields.clone()))
        .unwrap_or_default();
    let has_returning = upper.contains("RETURNING");

    Some(Ok(ParsedInsert {
        coll_name: coll_name.to_string(),
        doc_id,
        fields,
        value_bytes,
        has_returning,
        collection_type: coll_type,
    }))
}

/// Extract document ID from parsed fields, generating one if absent.
fn extract_doc_id(fields: &std::collections::HashMap<String, nodedb_types::Value>) -> String {
    for key in ["id", "document_id", "key"] {
        if let Some(nodedb_types::Value::String(s)) = fields.get(key) {
            return s.clone();
        }
    }
    nodedb_types::id_gen::uuid_v7()
}

/// Build a KV PUT plan from parsed insert fields.
pub(super) fn build_kv_plan(
    collection: &str,
    doc_id: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> crate::bridge::envelope::PhysicalPlan {
    let key = doc_id.as_bytes().to_vec();
    let mut value_fields = fields.clone();
    for k in ["key", "id", "document_id"] {
        value_fields.remove(k);
    }
    let value = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(value_fields))
        .unwrap_or_default();
    crate::bridge::envelope::PhysicalPlan::Kv(KvOp::Put {
        collection: collection.to_string(),
        key,
        value,
        ttl_ms: 0,
    })
}

/// Format a RETURNING response from parsed fields.
pub(super) fn returning_response(
    doc_id: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> PgWireResult<Vec<Response>> {
    use futures::stream;
    use pgwire::api::results::{DataRowEncoder, QueryResponse};

    let mut result_doc = fields.clone();
    result_doc.insert(
        "id".to_string(),
        nodedb_types::Value::String(doc_id.to_string()),
    );
    let json_str =
        sonic_rs::to_string(&nodedb_types::Value::Object(result_doc)).unwrap_or_default();
    let schema = std::sync::Arc::new(vec![crate::control::server::pgwire::types::text_field(
        "result",
    )]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    let _ = encoder.encode_field(&json_str);
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// Dispatch a plan to WAL + Data Plane, returning an error response on failure.
pub(super) async fn dispatch_plan(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    vshard_id: crate::types::VShardId,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Option<PgWireResult<Vec<Response>>> {
    if let Err(e) = crate::control::server::dispatch_utils::wal_append_if_write(
        &state.wal, tenant_id, vshard_id, &plan,
    ) {
        return Some(Err(sqlstate_error("XX000", &e.to_string())));
    }
    if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard_id, plan, 0,
    )
    .await
    {
        return Some(Err(sqlstate_error("XX000", &e.to_string())));
    }
    None
}

/// Fire SYNC AFTER INSERT triggers, returning an error response on failure.
pub(super) async fn fire_sync_after_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Option<PgWireResult<Vec<Response>>> {
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    if let Err(e) = crate::control::trigger::fire::fire_after_insert(
        state,
        identity,
        tenant_id,
        coll_name,
        fields,
        0,
        Some(TriggerExecutionMode::Sync),
    )
    .await
    {
        return Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}"))));
    }
    None
}

/// Fire INSTEAD OF INSERT triggers, returning the result.
pub(super) async fn fire_instead_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
    tag: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    match crate::control::trigger::fire_instead::fire_instead_of_insert(
        state, identity, tenant_id, coll_name, fields, 0,
    )
    .await
    {
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::Handled) => {
            Some(Ok(vec![Response::Execution(Tag::new(tag))]))
        }
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::NoTrigger) => None,
        Err(e) => Some(Err(sqlstate_error("XX000", &format!("trigger error: {e}")))),
    }
}

/// Fire BEFORE INSERT triggers, returning mutated fields or an error.
pub(super) async fn fire_before_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, PgWireResult<Vec<Response>>> {
    match crate::control::trigger::fire_before::fire_before_insert(
        state, identity, tenant_id, coll_name, fields, 0,
    )
    .await
    {
        Ok(f) => Ok(f),
        Err(e) => Err(Err(sqlstate_error(
            "XX000",
            &format!("BEFORE trigger error: {e}"),
        ))),
    }
}
