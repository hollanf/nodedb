//! RLS predicate compilation helpers.
//!
//! SQL-text parsing for `CREATE RLS POLICY` is done by the `nodedb-sql`
//! AST layer (`nodedb-sql/src/ddl_ast/parse/rls.rs`). This module is
//! responsible only for the predicate compilation step — turning the
//! raw predicate text into a rich `RlsPredicate` AST — and for parsing
//! the `ON DENY` clause.

use pgwire::error::PgWireResult;

use crate::control::security::deny::{self, DenyMode};
use crate::control::security::predicate::RlsPredicate;
use crate::control::security::predicate_parser::{parse_predicate, validate_auth_refs};

use super::super::super::types::sqlstate_error;

/// Result of compiling an RLS predicate string.
pub struct CompiledPredicate {
    /// Compiled predicate AST. Substituted at query time via `AuthContext`.
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
    let compiled = parse_predicate(predicate_str)
        .map_err(|e| sqlstate_error("42601", &format!("predicate parse error: {e}")))?;
    validate_auth_refs(&compiled).map_err(|e| sqlstate_error("42601", &e))?;

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
        compiled_predicate: Some(compiled),
        on_deny,
    })
}
