//! UPSERT INTO dispatch for schemaless and KV collections.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::DocumentOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::insert_parse::{
    build_kv_plan, dispatch_plan, fire_before_triggers, fire_instead_triggers,
    fire_sync_after_triggers, parse_write_statement,
};
use crate::control::server::pgwire::types::sqlstate_error;

/// UPSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
///
/// Same parsing as INSERT but dispatches the `Upsert` plan variant:
/// if a document with the given ID exists, its fields are merged.
pub async fn upsert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let parsed = match parse_write_statement(state, identity, sql, "UPSERT INTO ")? {
        Ok(p) => p,
        Err(e) => return Some(Err(e)),
    };

    let tenant_id = identity.tenant_id;
    let vshard_id = crate::types::VShardId::from_collection(&parsed.coll_name);

    // Fire INSTEAD OF INSERT triggers (upsert treated as INSERT for triggers).
    if let Some(result) = fire_instead_triggers(
        state,
        identity,
        tenant_id,
        &parsed.coll_name,
        &parsed.fields,
        "UPSERT",
    )
    .await
    {
        return Some(result);
    }

    // Fire BEFORE INSERT triggers — may mutate NEW fields.
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

    // Enforce type guards for schemaless collections (after BEFORE trigger).
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

    // Rebuild value bytes if BEFORE trigger mutated NEW fields.
    let value_bytes = if fields != parsed.fields {
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(fields.clone()))
            .unwrap_or(parsed.value_bytes)
    } else {
        parsed.value_bytes
    };

    // Build the write plan driven by collection type.
    // KV PUT is naturally an upsert (overwrites). Columnar upsert is unsupported.
    let plan = match &parsed.collection_type {
        Some(ct) if ct.is_kv() => build_kv_plan(&parsed.coll_name, &parsed.doc_id, &fields),
        Some(ct) if ct.is_columnar() => {
            return Some(Err(sqlstate_error(
                "0A000",
                "UPSERT is not supported for columnar collections",
            )));
        }
        _ => crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::Upsert {
            collection: parsed.coll_name.clone(),
            document_id: parsed.doc_id.clone(),
            value: value_bytes,
        }),
    };

    if let Some(err) = dispatch_plan(state, tenant_id, vshard_id, plan).await {
        return Some(err);
    }

    // Fire SYNC AFTER INSERT triggers.
    if let Some(err) =
        fire_sync_after_triggers(state, identity, tenant_id, &parsed.coll_name, &fields).await
    {
        return Some(err);
    }

    Some(Ok(vec![Response::Execution(Tag::new("UPSERT"))]))
}
