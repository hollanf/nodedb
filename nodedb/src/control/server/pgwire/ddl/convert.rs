//! DDL handler for CONVERT COLLECTION.
//!
//! Syntax:
//! - `CONVERT COLLECTION <name> TO document`
//! - `CONVERT COLLECTION <name> TO strict (col1 TYPE, col2 TYPE, ...)`
//! - `CONVERT COLLECTION <name> TO kv`

use std::time::Duration;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::MetaOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

/// CONVERT COLLECTION <name> TO <target_type> [(<col_defs>)]
pub async fn convert_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, target_type, schema_json) = parse_convert_sql(sql)?;
    let tenant_id = identity.tenant_id;

    // Validate collection exists.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };

    let mut coll = catalog
        .get_collection(tenant_id.as_u32(), &collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| {
            sqlstate_error(
                "42P01",
                &format!("collection '{collection}' does not exist"),
            )
        })?;

    // Dispatch to Data Plane: re-encode if needed (strict = Binary Tuple).
    let plan = PhysicalPlan::Meta(MetaOp::ConvertCollection {
        collection: collection.clone(),
        target_type: target_type.clone(),
        schema_json: schema_json.clone(),
    });

    super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        &collection,
        plan,
        Duration::from_secs(60),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("conversion failed: {e}")))?;

    // Update catalog collection type.
    let new_type = match target_type.as_str() {
        "document" => nodedb_types::CollectionType::document(),
        "strict" | "kv" => {
            let columns: Vec<nodedb_types::columnar::ColumnDef> =
                serde_json::from_str(&schema_json)
                    .map_err(|e| sqlstate_error("XX000", &format!("schema parse error: {e}")))?;
            let schema = nodedb_types::columnar::StrictSchema {
                columns,
                version: 1,
            };
            if target_type == "kv" {
                nodedb_types::CollectionType::kv(schema)
            } else {
                nodedb_types::CollectionType::strict(schema)
            }
        }
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("unsupported target type: {target_type}"),
            ));
        }
    };

    coll.collection_type = new_type;
    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    tracing::info!(
        %collection,
        target_type,
        tenant = tenant_id.as_u32(),
        "collection converted"
    );

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "CONVERT COLLECTION",
    ))])
}

// ── SQL Parsing ──────────────────────────────────────────────────────────

/// Parse CONVERT COLLECTION SQL.
///
/// Returns `(collection_name, target_type, schema_json)`.
/// `schema_json` is empty for `TO document`, JSON-serialized `Vec<ColumnDef>`
/// for `TO strict` and `TO kv`.
fn parse_convert_sql(sql: &str) -> PgWireResult<(String, String, String)> {
    let upper = sql.to_uppercase();

    // Extract collection name: CONVERT COLLECTION <name> TO ...
    let coll_pos = upper
        .find("COLLECTION ")
        .ok_or_else(|| sqlstate_error("42601", "expected COLLECTION keyword"))?;
    let after_coll = sql[coll_pos + 11..].trim_start();
    let collection = after_coll
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    // Extract target type: TO <type>
    let to_pos = upper[coll_pos + 11..]
        .find(" TO ")
        .ok_or_else(|| sqlstate_error("42601", "expected TO <type> clause"))?;
    let after_to = sql[coll_pos + 11 + to_pos + 4..].trim_start();
    let target_type = after_to
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing target type after TO"))?
        .to_lowercase()
        .trim_matches('(')
        .to_string();

    match target_type.as_str() {
        "document" => Ok((collection, "document".into(), String::new())),
        "strict" | "kv" => {
            // Parse column definitions from parenthesized list.
            let schema_json = parse_column_defs_to_json(after_to)?;
            Ok((collection, target_type, schema_json))
        }
        other => Err(sqlstate_error(
            "42601",
            &format!("unsupported target type: {other} (expected document, strict, kv)"),
        )),
    }
}

/// Parse `(col1 TYPE, col2 TYPE, ...)` into JSON-serialized `Vec<ColumnDef>`.
fn parse_column_defs_to_json(s: &str) -> PgWireResult<String> {
    let open = s
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected (column definitions) after type"))?;
    let close = s
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", "missing closing parenthesis"))?;
    if close <= open {
        return Err(sqlstate_error("42601", "empty column definitions"));
    }

    let inner = &s[open + 1..close];
    let mut columns: Vec<serde_json::Value> = Vec::new();

    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(sqlstate_error(
                "42601",
                &format!("expected 'name TYPE' in column def: {part}"),
            ));
        }
        let col_name = tokens[0].to_lowercase();
        let col_type = tokens[1].to_uppercase();
        let nullable = !tokens
            .windows(2)
            .any(|w| w[0].eq_ignore_ascii_case("NOT") && w[1].eq_ignore_ascii_case("NULL"))
            && !tokens.iter().any(|t| t.eq_ignore_ascii_case("NOTNULL"));

        columns.push(serde_json::json!({
            "name": col_name,
            "column_type": map_sql_type(&col_type),
            "nullable": nullable,
        }));
    }

    if columns.is_empty() {
        return Err(sqlstate_error("42601", "at least one column required"));
    }

    serde_json::to_string(&columns)
        .map_err(|e| sqlstate_error("XX000", &format!("schema serialization: {e}")))
}

/// Map SQL type names to ColumnType string representation.
fn map_sql_type(sql_type: &str) -> &'static str {
    match sql_type {
        "INT" | "INTEGER" | "INT8" | "BIGINT" => "Int64",
        "INT4" | "INT2" | "SMALLINT" => "Int64",
        "FLOAT" | "FLOAT8" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => "Float64",
        "BOOL" | "BOOLEAN" => "Boolean",
        "TIMESTAMP" | "TIMESTAMPTZ" => "Timestamp",
        "BLOB" | "BYTEA" | "BINARY" => "Binary",
        _ => "Utf8", // VARCHAR, TEXT, and anything else → Utf8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_convert_to_document() {
        let (coll, target, schema) =
            parse_convert_sql("CONVERT COLLECTION users TO document").unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "document");
        assert!(schema.is_empty());
    }

    #[test]
    fn parse_convert_to_strict() {
        let sql = "CONVERT COLLECTION users TO strict (name VARCHAR, age INT, active BOOLEAN)";
        let (coll, target, schema_json) = parse_convert_sql(sql).unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "strict");

        let cols: Vec<serde_json::Value> = serde_json::from_str(&schema_json).unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0]["name"], "name");
        assert_eq!(cols[0]["column_type"], "Utf8");
        assert_eq!(cols[1]["name"], "age");
        assert_eq!(cols[1]["column_type"], "Int64");
        assert_eq!(cols[2]["name"], "active");
        assert_eq!(cols[2]["column_type"], "Boolean");
    }

    #[test]
    fn parse_convert_to_kv() {
        let sql = "CONVERT COLLECTION cache TO kv (key VARCHAR, value BLOB)";
        let (coll, target, schema_json) = parse_convert_sql(sql).unwrap();
        assert_eq!(coll, "cache");
        assert_eq!(target, "kv");
        assert!(!schema_json.is_empty());
    }

    #[test]
    fn parse_convert_not_null_constraint() {
        let sql = "CONVERT COLLECTION users TO strict (id INT NOT NULL, name VARCHAR)";
        let (_, _, schema_json) = parse_convert_sql(sql).unwrap();
        let cols: Vec<serde_json::Value> = serde_json::from_str(&schema_json).unwrap();
        assert_eq!(cols[0]["nullable"], false);
        assert_eq!(cols[1]["nullable"], true);
    }

    #[test]
    fn parse_convert_missing_to_errors() {
        assert!(parse_convert_sql("CONVERT COLLECTION users").is_err());
    }

    #[test]
    fn parse_convert_unknown_type_errors() {
        assert!(parse_convert_sql("CONVERT COLLECTION users TO graph").is_err());
    }
}
