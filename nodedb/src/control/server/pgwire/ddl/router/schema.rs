use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // Triggers: CREATE [OR REPLACE] [SYNC|DEFERRED] TRIGGER ...
    // CREATE TRIGGER is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP TRIGGER ") {
        return Some(super::super::trigger::drop_trigger(state, identity, parts));
    }
    // ALTER TRIGGER is fully dispatched via typed AST (ast.rs).
    if upper == "SHOW TRIGGERS" || upper.starts_with("SHOW TRIGGERS") {
        return Some(super::super::trigger::show_triggers(state, identity, parts));
    }

    // Schema introspection.
    if upper.starts_with("DESCRIBE SEQUENCE ") {
        let name = parts.get(2).unwrap_or(&"");
        return Some(super::super::sequence::describe_sequence(
            state, identity, name,
        ));
    }
    if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
        return Some(super::super::collection::describe_collection(
            state, identity, parts,
        ));
    }

    // CREATE TABLE — fully dispatched via typed AST (ast.rs).
    // CREATE COLLECTION — fully dispatched via typed AST (ast.rs).
    // CREATE INDEX / CREATE UNIQUE INDEX — fully dispatched via typed AST (ast.rs).
    // CREATE SEQUENCE — fully dispatched via typed AST (ast.rs).
    // CREATE RLS POLICY — fully dispatched via typed AST (ast.rs).
    // ALTER COLLECTION (all sub-operations) — fully dispatched via typed AST (ast.rs).

    if upper.starts_with("DROP COLLECTION ") {
        return Some(super::super::collection::drop_collection(
            state, identity, parts,
        ));
    }
    if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
        return Some(super::super::collection::undrop_collection(
            state, identity, parts,
        ));
    }
    if upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(super::super::collection::show_collections(state, identity));
    }

    if upper.starts_with("DROP INDEX ") {
        return Some(super::super::collection::drop_index(state, identity, parts).await);
    }
    if upper.starts_with("SHOW INDEXES") || upper.starts_with("SHOW INDEX") {
        return Some(super::super::collection::show_indexes(
            state, identity, parts,
        ));
    }

    // ALTER TABLE ADD COLUMN — handled via typed AST (ast.rs AlterCollection).
    // The only remaining ALTER TABLE path is undirected fallthrough.

    if upper.starts_with("DROP RLS POLICY ") {
        return Some(super::super::rls::drop_rls_policy(state, identity, parts));
    }
    if upper.starts_with("SHOW RLS POLICIES") || upper.starts_with("SHOW RLS POLICY") {
        return Some(super::super::rls::show_rls_policies(state, identity, parts));
    }

    // DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>] [VALUE <expr>] [ASSERT <expr>] [READONLY]
    if upper.starts_with("DEFINE FIELD ") {
        return Some(super::super::field_def::define_field(state, identity, sql));
    }

    // DEFINE EVENT <name> ON <collection> WHEN <condition> THEN <action>
    if upper.starts_with("DEFINE EVENT ") {
        return Some(super::super::field_def::define_event(state, identity, sql));
    }

    // CREATE SPATIAL INDEX
    if upper.starts_with("CREATE SPATIAL INDEX ") {
        return Some(super::super::spatial::create_spatial_index(
            state, identity, parts,
        ));
    }

    None
}
