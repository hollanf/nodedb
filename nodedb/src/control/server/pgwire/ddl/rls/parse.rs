//! RLS predicate compilation helpers.
//!
//! SQL-text parsing for `CREATE RLS POLICY` is done by the `nodedb-sql`
//! AST layer (`nodedb-sql/src/ddl_ast/parse/rls.rs`). This module is
//! responsible only for the predicate compilation step — turning the
//! raw predicate text into a `ScanFilter` blob or a rich `RlsPredicate`
//! — and for parsing the `ON DENY` clause.

use pgwire::error::PgWireResult;

use crate::control::security::deny::{self, DenyMode};
use crate::control::security::predicate::RlsPredicate;
use crate::control::security::predicate_parser::{parse_predicate, validate_auth_refs};

use super::super::super::types::sqlstate_error;

/// Result of compiling an RLS predicate string.
pub struct CompiledPredicate {
    /// Serialised `ScanFilter` bytes (empty when `compiled_predicate` is set).
    pub predicate: Vec<u8>,
    /// Rich compiled predicate (when the expression uses `$auth.*` or set ops).
    pub compiled_predicate: Option<RlsPredicate>,
    /// Deny-mode derived from the `ON DENY` clause.
    pub on_deny: DenyMode,
}

/// Compile a predicate string and optional `ON DENY` raw clause into a
/// `CompiledPredicate`. Called by the `create_rls_policy` handler after
/// the typed AST fields have been validated.
pub fn compile_rls_predicate(
    predicate_str: &str,
    on_deny_raw: Option<&str>,
) -> PgWireResult<CompiledPredicate> {
    let has_rich_syntax = predicate_str.contains("$auth")
        || predicate_str.to_uppercase().contains("CONTAINS")
        || predicate_str.to_uppercase().contains("INTERSECTS")
        || predicate_str.to_uppercase().contains(" AND ")
        || predicate_str.to_uppercase().contains(" OR ")
        || predicate_str.to_uppercase().contains("NOT ");

    let (predicate, compiled_predicate) = if has_rich_syntax {
        let compiled = parse_predicate(predicate_str)
            .map_err(|e| sqlstate_error("42601", &format!("predicate parse error: {e}")))?;
        validate_auth_refs(&compiled).map_err(|e| sqlstate_error("42601", &e))?;
        (Vec::new(), Some(compiled))
    } else {
        let pred_parts: Vec<&str> = predicate_str.split_whitespace().collect();
        if pred_parts.len() < 3 {
            return Err(sqlstate_error(
                "42601",
                "USING predicate must be: (<field> <op> <value>) or a rich expression with $auth.*",
            ));
        }

        let field = pred_parts[0];
        let op = pred_parts[1];
        let value_str = strip_single_quotes(&pred_parts[2..].join(" "));

        let filter = crate::bridge::scan_filter::ScanFilter {
            field: field.to_string(),
            op: op.into(),
            value: nodedb_types::Value::String(value_str),
            clauses: Vec::new(),
            expr: None,
        };
        let predicate = zerompk::to_msgpack_vec(&vec![filter])
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

        (predicate, None)
    };

    let on_deny = if let Some(deny_text) = on_deny_raw {
        let deny_parts: Vec<&str> = deny_text.split_whitespace().collect();
        // Strip leading DENY token if present.
        let slice = if deny_parts
            .first()
            .map(|s| s.eq_ignore_ascii_case("DENY"))
            .unwrap_or(false)
        {
            &deny_parts[1..]
        } else {
            &deny_parts[..]
        };
        deny::parse_on_deny(slice).map_err(|e| sqlstate_error("42601", &e))?
    } else {
        DenyMode::default()
    };

    Ok(CompiledPredicate {
        predicate,
        compiled_predicate,
        on_deny,
    })
}

fn strip_single_quotes(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}
