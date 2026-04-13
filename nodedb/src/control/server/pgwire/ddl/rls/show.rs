//! `SHOW RLS POLICIES [ON <collection>] [TENANT <id>]` handler.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

pub fn show_rls_policies(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let collection = parts
        .iter()
        .position(|p| p.to_uppercase() == "ON")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.to_string());

    let tenant_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(identity.tenant_id.as_u32());

    let policies = if let Some(coll) = &collection {
        state.rls.all_policies(tenant_id, coll)
    } else {
        state.rls.all_policies_for_tenant(tenant_id)
    };

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("collection"),
        text_field("type"),
        text_field("mode"),
        text_field("has_auth_refs"),
        text_field("enabled"),
        text_field("created_by"),
    ]);

    let rows: Vec<_> = policies
        .iter()
        .map(|p| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&p.name);
            let _ = enc.encode_field(&p.collection);
            let _ = enc.encode_field(&format!("{:?}", p.policy_type));
            let _ = enc.encode_field(&format!("{:?}", p.mode));
            let _ = enc.encode_field(&p.compiled_predicate.is_some().to_string());
            let _ = enc.encode_field(&p.enabled.to_string());
            let _ = enc.encode_field(&p.created_by);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
