//! `CREATE TABLE` DDL handler — strict-default Postgres-style syntax.
//!
//! `CREATE TABLE foo (col_list)` creates a strict relational collection by
//! default. An explicit `WITH (engine='...')` overrides the engine type.
//! All fields arrive pre-parsed from the `nodedb-sql` AST layer.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;
use super::super::super::schema_validation::{
    extract_vector_fields, parse_fields_clause_from_pairs,
};
use super::enforcement::parse_balanced_clause_from_raw;
use super::engine_option::validate_engine_name;

/// Handle `CREATE [IF NOT EXISTS] TABLE <name> (<col_list>) [WITH (engine='...')]`.
///
/// All fields are pre-parsed from the `nodedb-sql` AST:
/// - `engine`: value of `engine=` from the WITH clause (lowercased), or `None` for default
///   (which is `document_strict` for CREATE TABLE).
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: free-standing modifier keywords: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
/// - `balanced_raw`: raw inner content of `BALANCED ON (...)`, if present.
pub async fn create_table(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &super::request::CreateCollectionRequest<'_>,
) -> PgWireResult<Vec<Response>> {
    let super::request::CreateCollectionRequest {
        name,
        engine,
        columns,
        options,
        flags,
        balanced_raw,
    } = *req;
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(sqlstate_error(
            "42601",
            &format!("invalid table name '{name}': only letters, digits, '-', and '_' are allowed"),
        ));
    }

    // CREATE TABLE requires columns by convention.
    if columns.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "CREATE TABLE requires a column list; for schemaless collections use CREATE COLLECTION",
        ));
    }

    let tenant_id = identity.tenant_id;

    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(existing)) = catalog.get_collection(tenant_id.as_u32(), name)
        && existing.is_active
    {
        return Err(sqlstate_error(
            "42P07",
            &format!("table '{name}' already exists"),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let bitemporal_flag = flags.iter().any(|f| f == "BITEMPORAL");

    // Validate engine name. Default for CREATE TABLE is document_strict (None maps to strict).
    let canonical_engine = validate_engine_name(engine, options)?;

    // Build CollectionType from the canonical engine name.
    // CREATE TABLE default (engine=None) → document_strict.
    let (collection_type, columnar_schema_columns) = nodedb_sql::ddl_ast::build_collection_type(
        canonical_engine,
        columns,
        options,
        bitemporal_flag,
        true, // CREATE TABLE: None → document_strict
    )
    .map_err(|e| sqlstate_error("42601", &e.to_string()))?;

    let (mut fields, serial_fields) = parse_fields_clause_from_pairs(columns);
    if fields.is_empty() && !columnar_schema_columns.is_empty() {
        fields = columnar_schema_columns;
    }

    let schema_json = match &collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            sonic_rs::to_string(schema).ok()
        }
        nodedb_types::CollectionType::KeyValue(config) => sonic_rs::to_string(config).ok(),
        _ => None,
    };

    let (primary, vector_primary) = {
        match nodedb_sql::ddl_ast::parse::vector_primary::parse_vector_primary_options_from_kvs(
            options,
        ) {
            Ok(Some(mut vp_cfg)) => {
                let col_list: Vec<(String, String)> = if fields.is_empty() {
                    columns.to_vec()
                } else {
                    fields.clone()
                };
                nodedb_sql::ddl_ast::parse::vector_primary::validate_vector_field(
                    &vp_cfg, &col_list,
                )
                .map_err(|e| sqlstate_error("42601", &e.to_string()))?;
                nodedb_sql::ddl_ast::parse::vector_primary::validate_payload_indexes(
                    &mut vp_cfg,
                    &col_list,
                )
                .map_err(|e| sqlstate_error("42601", &e.to_string()))?;
                if let Some((_, type_str)) = col_list
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(&vp_cfg.vector_field))
                {
                    let upper_t = type_str.to_uppercase();
                    if let Some(inner) = upper_t
                        .strip_prefix("VECTOR(")
                        .and_then(|s| s.strip_suffix(')'))
                        && let Ok(d) = inner.trim().parse::<u32>()
                    {
                        if vp_cfg.dim == 0 {
                            vp_cfg.dim = d;
                        } else if vp_cfg.dim != d {
                            return Err(sqlstate_error(
                                "42601",
                                &format!(
                                    "vector dim mismatch: WITH clause specifies {}, column type VECTOR({}) specifies {}",
                                    vp_cfg.dim, d, d
                                ),
                            ));
                        }
                    }
                }
                (nodedb_types::PrimaryEngine::Vector, Some(vp_cfg))
            }
            Ok(None) => (
                nodedb_types::PrimaryEngine::infer_from_collection_type(&collection_type),
                None,
            ),
            Err(e) => return Err(sqlstate_error("42601", &e.to_string())),
        }
    };

    let append_only = flags.iter().any(|f| f == "APPEND_ONLY");
    let hash_chain = flags.iter().any(|f| f == "HASH_CHAIN");
    let bitemporal = bitemporal_flag;
    if hash_chain && !append_only {
        return Err(sqlstate_error("42601", "HASH_CHAIN requires APPEND_ONLY"));
    }
    let balanced = parse_balanced_clause_from_raw(balanced_raw.unwrap_or(""))
        .map_err(|e| sqlstate_error("42601", &e))?;

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u32(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
        fields,
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type,
        timeseries_config: schema_json,
        is_active: true,
        append_only,
        hash_chain,
        balanced,
        last_chain_hash: None,
        period_lock: None,
        retention_period: None,
        legal_holds: Vec::new(),
        state_constraints: Vec::new(),
        transition_checks: Vec::new(),
        type_guards: Vec::new(),
        check_constraints: Vec::new(),
        materialized_sums: Vec::new(),
        lvc_enabled: false,
        bitemporal,
        permission_tree_def: None,
        indexes: Vec::new(),
        size_bytes_estimate: 0,
        primary,
        vector_primary,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
    {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    let vector_fields = extract_vector_fields(&coll.fields);
    if !vector_fields.is_empty() {
        for (field_name, _dim, metric) in &vector_fields {
            tracing::info!(
                %name,
                field = %field_name,
                %metric,
                "auto-configuring vector field"
            );
        }
    }

    for field_name in &serial_fields {
        let seq_name = format!("{name}_{field_name}_seq");
        let mut seq_def = crate::control::security::catalog::sequence_types::StoredSequence::new(
            tenant_id.as_u32(),
            seq_name.clone(),
            identity.username.clone(),
        );
        seq_def.created_at = now;
        if let Some(catalog) = state.credentials.catalog() {
            let _ = catalog.put_sequence(&seq_def);
        }
        let _ = state.sequence_registry.create(seq_def);
        tracing::info!(
            collection = %name,
            field = %field_name,
            sequence = %seq_name,
            "auto-created SERIAL sequence"
        );
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created table '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))])
}
