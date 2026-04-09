//! INSERT INTO dispatch for schemaless, KV, and columnar collections.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::{ColumnarOp, DocumentOp, VectorOp};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::insert_parse::{
    build_kv_plan, dispatch_plan, extract_vector_fields, fire_before_triggers,
    fire_instead_triggers, fire_sync_after_triggers, parse_write_statement, returning_response,
};
use crate::control::server::pgwire::types::sqlstate_error;

/// INSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
pub async fn insert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let parsed = match parse_write_statement(state, identity, sql, "INSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

    let tenant_id = identity.tenant_id;
    // Route by collection name so INSERT and subsequent PointGet/PointUpdate/PointDelete
    // all land on the same core.
    let vshard_id = crate::types::VShardId::from_collection(&parsed.coll_name);

    // Fire INSTEAD OF INSERT triggers — if handled, skip normal dispatch.
    if let Some(result) = fire_instead_triggers(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        "INSERT",
    )
    .await
    {
        return Some(result);
    }

    // Fire BEFORE INSERT triggers — may reject via RAISE EXCEPTION, may mutate NEW fields.
    let fields = match fire_before_triggers(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    // Auto-generate sequence values for fields with sequence_name where the
    // INSERT didn't provide an explicit value.
    let mut fields = fields;
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll_def)) = catalog.get_collection(tenant_id.as_u32(), &parsed.coll_name)
    {
        for field_def in &coll_def.field_defs {
            if let Some(ref seq_name) = field_def.sequence_name
                && !fields.contains_key(&field_def.name)
            {
                match state.sequence_registry.nextval_formatted(
                    tenant_id.as_u32(),
                    seq_name,
                    "",
                    &std::collections::HashMap::new(),
                ) {
                    Ok(val) => {
                        let typed_val = match val {
                            crate::control::sequence::registry::SequenceValue::Int(i) => {
                                nodedb_types::Value::Integer(i)
                            }
                            crate::control::sequence::registry::SequenceValue::Formatted(s) => {
                                nodedb_types::Value::String(s)
                            }
                        };
                        fields.insert(field_def.name.clone(), typed_val);
                    }
                    Err(e) => {
                        return Some(Err(sqlstate_error(
                            "XX000",
                            &format!("sequence '{seq_name}' error: {e}"),
                        )));
                    }
                }
            }
        }
    }

    // Enforce type guards for schemaless collections (after BEFORE trigger + sequence injection).
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll_def)) = catalog.get_collection(tenant_id.as_u32(), &parsed.coll_name)
        && !coll_def.type_guards.is_empty()
    {
        let doc = nodedb_types::Value::Object(fields.clone());
        if let Err(violation) = crate::data::executor::enforcement::typeguard::check_type_guards(
            &parsed.coll_name,
            &coll_def.type_guards,
            &doc,
            None,
        ) {
            use crate::control::server::pgwire::types::error_code_to_sqlstate;
            let (severity, code, message) = error_code_to_sqlstate(&violation);
            return Some(Err(pgwire::error::PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(severity.to_owned(), code.to_owned(), message),
            ))));
        }
    }

    // Rebuild value bytes (sequence injection or BEFORE trigger may have mutated fields).
    let value_bytes = if fields != parsed.fields {
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(fields.clone()))
            .unwrap_or(parsed.value_bytes)
    } else {
        parsed.value_bytes
    };

    // Build the write plan driven by collection type.
    let plan = match &parsed.collection_type {
        Some(ct) if ct.is_kv() => build_kv_plan(&parsed.coll_name, &parsed.doc_id, &fields),
        Some(ct) if ct.is_columnar() => {
            crate::bridge::envelope::PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: parsed.coll_name.clone(),
                payload: value_bytes.clone(),
                format: "msgpack".into(),
            })
        }
        _ => crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::PointPut {
            collection: parsed.coll_name.clone(),
            document_id: parsed.doc_id.clone(),
            value: value_bytes,
        }),
    };

    if let Some(err) = dispatch_plan(state, tenant_id, vshard_id, plan).await {
        return Some(err);
    }

    // Track field names in catalog for schemaless collections.
    if parsed
        .collection_type
        .as_ref()
        .is_none_or(|ct| ct.is_schemaless())
        && let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), &parsed.coll_name)
    {
        let mut changed = false;
        for (name, val) in &fields {
            if name == "id" {
                continue;
            }
            if !coll.fields.iter().any(|(n, _)| n == name) {
                let type_str = match val {
                    nodedb_types::Value::Float(_) => "FLOAT",
                    nodedb_types::Value::Integer(_) => "INT",
                    nodedb_types::Value::Bool(_) => "BOOL",
                    _ => "TEXT",
                };
                coll.fields.push((name.clone(), type_str.to_string()));
                changed = true;
            }
        }
        if changed {
            let _ = catalog.put_collection(&coll);
        }
    }

    // Fire SYNC AFTER INSERT triggers.
    if let Some(err) =
        fire_sync_after_triggers(state, identity, tenant_id, &parsed.coll_name, &fields).await
    {
        return Some(err);
    }

    // Dispatch VectorInsert for vector fields.
    let vec_vshard = crate::types::VShardId::from_collection(&parsed.coll_name);
    for (field_name, vector) in extract_vector_fields(&fields) {
        let dim = vector.len();

        if let Some(catalog) = state.credentials.catalog() {
            let col = if field_name.is_empty() {
                "embedding"
            } else {
                field_name.as_str()
            };
            if let Ok(Some(entry)) =
                catalog.get_vector_model(tenant_id.as_u32(), &parsed.coll_name, col)
                && entry.metadata.strict_dimensions
                && entry.metadata.dimensions != dim
            {
                return Some(Err(sqlstate_error(
                    "23514",
                    &format!(
                        "strict_dimensions: vector has {} dimensions, model '{}' requires {}",
                        dim, entry.metadata.model, entry.metadata.dimensions
                    ),
                )));
            }
        }
        let vec_plan = crate::bridge::envelope::PhysicalPlan::Vector(VectorOp::Insert {
            collection: parsed.coll_name.clone(),
            vector,
            dim,
            field_name: field_name.clone(),
            doc_id: Some(parsed.doc_id.clone()),
        });

        if let Some(err) = dispatch_plan(state, tenant_id, vec_vshard, vec_plan).await {
            return Some(err);
        }
    }

    if parsed.has_returning {
        return Some(returning_response(&parsed.doc_id, &fields));
    }

    Some(Ok(vec![Response::Execution(Tag::new("INSERT"))]))
}

#[cfg(test)]
mod tests {
    use super::super::insert_parse::extract_vector_fields;

    #[test]
    fn extract_vector_fields_keeps_named_numeric_arrays() {
        let fields = std::collections::HashMap::from([
            (
                "embedding".to_string(),
                nodedb_types::Value::Array(vec![
                    nodedb_types::Value::Float(1.0),
                    nodedb_types::Value::Integer(2),
                    nodedb_types::Value::Float(3.5),
                ]),
            ),
            (
                "tags".to_string(),
                nodedb_types::Value::Array(vec![nodedb_types::Value::String("rust".into())]),
            ),
        ]);

        let vectors = extract_vector_fields(&fields);

        assert_eq!(
            vectors,
            vec![("embedding".to_string(), vec![1.0, 2.0, 3.5])]
        );
    }
}
