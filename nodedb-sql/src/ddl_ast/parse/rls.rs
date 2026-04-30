//! Parse CREATE/DROP/SHOW RLS POLICY.

use super::helpers::{extract_after_keyword, extract_name_after_if_exists};
use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE RLS POLICY ") {
            return Ok(Some(parse_create_rls_policy(upper, parts, trimmed)));
        }
        if upper.starts_with("DROP RLS POLICY ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "POLICY") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let collection = extract_after_keyword(parts, "ON").unwrap_or_default();
            return Ok(Some(NodedbStatement::DropRlsPolicy {
                name,
                collection,
                if_exists,
            }));
        }
        if upper.starts_with("SHOW RLS POLI") {
            let collection = parts.get(3).map(|s| s.to_string());
            return Ok(Some(NodedbStatement::ShowRlsPolicies { collection }));
        }
        Ok(None)
    })()
    .transpose()
}

/// `CREATE RLS POLICY <name> ON <collection> FOR <type> USING (<predicate>)
///     [RESTRICTIVE] [TENANT <id>] [ON DENY ...]`
///
/// Extracts all token-level fields. Predicate compilation (AST parse,
/// auth-ref validation, ScanFilter serialization) is left to the
/// handler in `nodedb` which has access to the required security types.
fn parse_create_rls_policy(upper: &str, parts: &[&str], _trimmed: &str) -> NodedbStatement {
    // parts: CREATE RLS POLICY <name> ON <collection> FOR <type> USING (<pred>) ...
    // Indices (0-based):  0      1    2      3         4     5     6     7    8     9+

    let name = parts.get(3).unwrap_or(&"").to_lowercase();
    let collection = parts
        .iter()
        .position(|p| p.to_uppercase() == "ON")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.to_lowercase())
        .unwrap_or_default();

    // FOR keyword → next token is the policy type.
    let policy_type = parts
        .iter()
        .position(|p| p.to_uppercase() == "FOR")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.to_uppercase())
        .unwrap_or_else(|| "ALL".to_string());

    // USING keyword → everything up to the next keyword (RESTRICTIVE/TENANT/ON)
    // is the predicate (including its outer parentheses which we strip).
    let predicate_raw =
        if let Some(using_idx) = parts.iter().position(|p| p.to_uppercase() == "USING") {
            let end = parts[using_idx + 1..]
                .iter()
                .position(|p| {
                    let u = p.to_uppercase();
                    u == "RESTRICTIVE" || u == "TENANT" || u == "ON"
                })
                .map(|i| using_idx + 1 + i)
                .unwrap_or(parts.len());
            strip_outer_parens(&parts[using_idx + 1..end].join(" "))
        } else {
            // Fall back: look in the upper string between USING( and ) for simple cases.
            extract_using_from_upper(upper)
        };

    let is_restrictive = upper.contains("RESTRICTIVE");

    // ON DENY <...> — everything after "ON DENY" up to RESTRICTIVE/TENANT/end.
    let on_deny_raw = {
        // Find "ON" followed by "DENY" in parts (not the "ON <collection>" earlier).
        let deny_pos = parts
            .windows(2)
            .position(|w| w[0].to_uppercase() == "ON" && w[1].to_uppercase() == "DENY");
        deny_pos.map(|pos| {
            // Collect tokens after DENY until RESTRICTIVE or TENANT.
            parts[pos + 2..]
                .iter()
                .take_while(|p| {
                    let u = p.to_uppercase();
                    u != "RESTRICTIVE" && u != "TENANT"
                })
                .copied()
                .collect::<Vec<_>>()
                .join(" ")
        })
    };

    let tenant_id_override = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok());

    NodedbStatement::CreateRlsPolicy {
        name,
        collection,
        policy_type,
        predicate_raw,
        is_restrictive,
        on_deny_raw,
        tenant_id_override,
    }
}

/// Strip at most one matched pair of outer parentheses.
fn strip_outer_parens(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with('(') && trimmed.ends_with(')') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let mut depth = 0i32;
        let mut balanced = true;
        for ch in inner.chars() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth < 0 {
                        balanced = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        if balanced && depth == 0 {
            return inner.trim().to_string();
        }
    }
    trimmed.to_string()
}

/// Fallback: extract predicate text from the uppercased SQL string when
/// USING appears without space-separated parts matching (e.g. `USING(x=1)`).
fn extract_using_from_upper(upper: &str) -> String {
    let Some(start) = upper.find("USING") else {
        return String::new();
    };
    let after = &upper[start + 5..].trim_start();
    if after.starts_with('(') {
        let mut depth = 0usize;
        let mut end = 0;
        for (i, ch) in after.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        after[1..end].trim().to_string()
    } else {
        String::new()
    }
}
