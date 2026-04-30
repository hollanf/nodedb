//! Parse CREATE/DROP/ALTER/DESCRIBE/SHOW for COLLECTION (and TABLE alias).
//!
//! `DROP COLLECTION` extensions (sqlparser 0.61 does not tokenize
//! these, hence custom-handled upper-case keyword scan):
//! - `PURGE` — hard-delete, skipping the retention window
//! - `CASCADE` — recursively drop dependents
//! - `CASCADE FORCE` — cascade through dynamic-SQL schedules
//!
//! `UNDROP COLLECTION <name>` restores a soft-deleted record.

use super::helpers::{extract_name_after_if_exists, extract_name_after_keyword};
use crate::ddl_ast::statement::{AlterCollectionOp, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE COLLECTION ") {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let name = match extract_name_after_keyword(parts, "COLLECTION") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let (engine, columns, options, flags, balanced_raw) =
                parse_collection_body(trimmed, &name)?;
            return Ok(Some(NodedbStatement::CreateCollection {
                name,
                if_not_exists,
                engine,
                columns,
                options,
                flags,
                balanced_raw,
            }));
        }
        if upper.starts_with("CREATE TABLE ") {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let name = match extract_name_after_keyword(parts, "TABLE") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let (engine, columns, options, flags, balanced_raw) =
                parse_collection_body(trimmed, &name)?;
            return Ok(Some(NodedbStatement::CreateTable {
                name,
                if_not_exists,
                engine,
                columns,
                options,
                flags,
                balanced_raw,
            }));
        }
        if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
            let name = match extract_name_after_keyword(parts, "COLLECTION")
                .or_else(|| extract_name_after_keyword(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::UndropCollection { name }));
        }
        if upper.starts_with("DROP COLLECTION ") || upper.starts_with("DROP TABLE ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "COLLECTION")
                .or_else(|| extract_name_after_if_exists(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            let purge = upper.contains(" PURGE");
            let cascade = upper.contains(" CASCADE");
            let cascade_force =
                upper.contains(" CASCADE FORCE") || upper.contains(" FORCE CASCADE");
            return Ok(Some(NodedbStatement::DropCollection {
                name,
                if_exists,
                purge,
                cascade: cascade || cascade_force,
                cascade_force,
            }));
        }
        if upper.starts_with("ALTER COLLECTION ") || upper.starts_with("ALTER TABLE ") {
            let name = match extract_name_after_keyword(parts, "COLLECTION")
                .or_else(|| extract_name_after_keyword(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            let operation = match parse_alter_operation(upper, parts, trimmed) {
                None => return Ok(None),
                Some(op) => op,
            };
            return Ok(Some(NodedbStatement::AlterCollection { name, operation }));
        }
        if upper.starts_with("DESCRIBE ") && !upper.starts_with("DESCRIBE SEQUENCE") {
            let name = match parts.get(1) {
                None => return Ok(None),
                Some(s) => s.to_string(),
            };
            return Ok(Some(NodedbStatement::DescribeCollection { name }));
        }
        if upper == "\\D" || upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
            return Ok(Some(NodedbStatement::ShowCollections));
        }
        Ok(None)
    })()
    .transpose()
}

// ── Body parsing ─────────────────────────────────────────────────────────────

/// Parse everything after the collection/table name into typed fields.
///
/// Returns `(engine, columns, options, flags, balanced_raw)`:
/// - `engine`: value of `engine=` from the WITH clause (lowercased), if present.
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: free-standing modifier keywords: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
/// - `balanced_raw`: raw interior of the `BALANCED ON (...)` clause, or `None`.
type CollectionBody = (
    Option<String>,
    Vec<(String, String)>,
    Vec<(String, String)>,
    Vec<String>,
    Option<String>,
);

fn parse_collection_body(trimmed: &str, name: &str) -> Result<CollectionBody, SqlError> {
    // Skip past the name to find the body.
    let lower = trimmed.to_lowercase();
    let name_lower = name.to_lowercase();
    let body = if let Some(pos) = lower.find(&name_lower) {
        trimmed[pos + name.len()..].trim()
    } else {
        return Ok((None, Vec::new(), Vec::new(), Vec::new(), None));
    };

    let upper_body = body.to_uppercase();

    // ── Column list ───────────────────────────────────────────────
    let columns = extract_column_pairs(body)?;

    // ── WITH clause ───────────────────────────────────────────────
    let (engine, options) = extract_with_options(body);

    // ── Free-standing flags ───────────────────────────────────────
    let mut flags: Vec<String> = Vec::new();
    if upper_body.contains("APPEND_ONLY") {
        flags.push("APPEND_ONLY".to_string());
    }
    if upper_body.contains("HASH_CHAIN") {
        flags.push("HASH_CHAIN".to_string());
    }
    if upper_body.contains("BITEMPORAL") {
        flags.push("BITEMPORAL".to_string());
    }

    // ── BALANCED ON (group_key = col, ...) ───────────────────────
    let balanced_raw = extract_balanced_raw(&upper_body, body);

    Ok((engine, columns, options, flags, balanced_raw))
}

/// Extract `(name, type)` pairs from the first parenthesised column list
/// in `body` (the text after the collection name). Returns an empty Vec
/// when no column list is present or parsing fails.
///
/// Handles nested parens for types like `VECTOR(128)`.
fn extract_column_pairs(body: &str) -> Result<Vec<(String, String)>, SqlError> {
    let paren_start = match body.find('(') {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };

    // Stop at the matching close paren (depth-aware).
    let mut depth = 0usize;
    let mut paren_end = None;
    for (i, b) in body.bytes().enumerate().skip(paren_start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let paren_end = match paren_end {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };

    let inner = &body[paren_start + 1..paren_end];
    let upper_inner = inner.to_uppercase();

    // If this looks like a WITH clause rather than a column list, skip.
    // WITH clauses start with known option keywords like ENGINE, PROFILE,
    // VECTOR_FIELD, PARTITION_BY, etc.
    if is_with_clause_inner(&upper_inner) {
        return Ok(Vec::new());
    }

    split_column_pairs(inner)
}

/// Heuristic: does the first token in the paren body look like a WITH-clause
/// key rather than a column name+type?
fn is_with_clause_inner(upper_inner: &str) -> bool {
    let first_tok = upper_inner.split_whitespace().next().unwrap_or("");
    matches!(
        first_tok.trim_end_matches(['=', '\'']),
        "ENGINE"
            | "PROFILE"
            | "VECTOR_FIELD"
            | "PARTITION_BY"
            | "DIM"
            | "METRIC"
            | "PAYLOAD_INDEXES"
            | "APPEND_ONLY"
            | "HASH_CHAIN"
            | "BITEMPORAL"
    )
}

/// Split the interior of a column-list paren into `(name, type)` pairs.
/// Uses top-level comma splitting (respects nested parens for VECTOR(n)).
fn split_column_pairs(inner: &str) -> Result<Vec<(String, String)>, SqlError> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (i, c) in inner.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                let token = inner[start..i].trim();
                if !token.is_empty()
                    && let Some(pair) = parse_col_token(token)?
                {
                    pairs.push(pair);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = inner[start..].trim();
    if !last.is_empty()
        && let Some(pair) = parse_col_token(last)?
    {
        pairs.push(pair);
    }
    Ok(pairs)
}

/// Parse a single column token like `"id BIGINT NOT NULL"` into `(name, type_str)`.
///
/// Captures only the name and bare type (including generic `VECTOR(128)`);
/// skips constraint keywords. Returns `Err` if the column name is a reserved
/// identifier and `None` for constraint-only clauses that should be skipped.
fn parse_col_token(token: &str) -> Result<Option<(String, String)>, SqlError> {
    use crate::reserved::check_identifier;

    let mut toks = token.split_whitespace();
    let raw_name = match toks.next() {
        None => return Ok(None),
        Some(s) => s,
    };

    // Skip constraint-only clauses (CONSTRAINT, PRIMARY KEY, UNIQUE, CHECK, ...).
    let upper_name = raw_name.to_uppercase();
    if matches!(
        upper_name.as_str(),
        "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "CHECK" | "FOREIGN" | "REFERENCES"
    ) {
        return Ok(None);
    }

    // Validate that the column name is not a reserved identifier.
    let name = check_identifier(raw_name)?;

    // Collect the column definition (bare type + modifiers like PRIMARY KEY, NOT NULL,
    // TIME_KEY, SPATIAL_INDEX). Downstream builders (build_strict_schema,
    // build_kv_collection_type, etc.) each strip to the bare type as needed via
    // parse_column_type_str. Stop only at DEFAULT and REFERENCES which start
    // default-value or FK sub-clauses that have no bearing on the stored schema.
    let mut type_parts: Vec<&str> = Vec::new();
    let mut in_paren = false;
    let mut hit_generated = false;
    for t in toks {
        let upper_t = t.to_uppercase();
        let stripped = upper_t.trim_end_matches(['(', ')', ',']);
        // Stop at REFERENCES and CONSTRAINT — sub-clauses with no bearing on the
        // stored schema.  UNIQUE and CHECK after the type are table-level keywords.
        // DEFAULT is intentionally included so schema builders can extract the
        // default expression.
        // GENERATED is NOT stopped here: we pass the raw text through so that
        // `build_strict_schema` can detect and store the generated expression.
        if !in_paren && matches!(stripped, "REFERENCES" | "CONSTRAINT") {
            break;
        }
        if !in_paren && matches!(stripped, "UNIQUE" | "CHECK") {
            break;
        }
        if !in_paren && stripped == "GENERATED" {
            hit_generated = true;
            // Stop the word-by-word iteration here.  We will append the original
            // raw text from "GENERATED" onwards below, preserving spaces inside
            // expressions like GENERATED ALWAYS AS ('café' || city).
            break;
        }
        if t.contains('(') {
            in_paren = true;
        }
        if t.contains(')') {
            in_paren = false;
        }
        type_parts.push(t);
    }

    if type_parts.is_empty() {
        return Ok(None);
    }

    let mut type_str = type_parts.join(" ");

    // When GENERATED ALWAYS AS was found, append the remainder of the original
    // token text verbatim so that downstream builders can parse the expression.
    if hit_generated {
        let upper_token = token.to_uppercase();
        if let Some(gen_pos) = upper_token.find("GENERATED") {
            type_str.push(' ');
            type_str.push_str(token[gen_pos..].trim());
        }
    }

    Ok(Some((name, type_str)))
}

/// Extract engine name and other key-value options from the `WITH (...)` clause.
///
/// Returns `(engine, other_options)` where `engine` is the value of the
/// `engine=` key (lowercased) and `other_options` is all other k=v pairs.
fn extract_with_options(body: &str) -> (Option<String>, Vec<(String, String)>) {
    let upper = body.to_uppercase();
    let with_pos = match upper.find(" WITH ").or_else(|| upper.find("WITH (")) {
        Some(p) => p,
        None => return (None, Vec::new()),
    };

    let after_with = body[with_pos..].trim_start();
    // Skip "WITH" keyword.
    let after_with = &after_with["WITH".len()..].trim_start();
    if !after_with.starts_with('(') {
        return (None, Vec::new());
    }

    // Find the matching close paren for the WITH clause.
    let mut depth = 0usize;
    let mut end = None;
    for (i, b) in after_with.bytes().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = match end {
        Some(e) => e,
        None => return (None, Vec::new()),
    };

    let inner = &after_with[1..end];
    let pairs = parse_with_kvs(inner);

    let mut engine: Option<String> = None;
    let mut other: Vec<(String, String)> = Vec::new();
    for (k, v) in pairs {
        if k.eq_ignore_ascii_case("engine") {
            engine = Some(v.to_lowercase());
        } else {
            other.push((k.to_lowercase(), v));
        }
    }
    (engine, other)
}

/// Split the interior of `WITH (...)` into `(key, value)` pairs.
/// Values may be quoted with `'` or `"`. Multi-value (ARRAY[...]) not supported here.
fn parse_with_kvs(inner: &str) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (i, c) in inner.char_indices() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                let token = inner[start..i].trim();
                if let Some(pair) = parse_kv_token(token) {
                    pairs.push(pair);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = inner[start..].trim();
    if !last.is_empty()
        && let Some(pair) = parse_kv_token(last)
    {
        pairs.push(pair);
    }
    pairs
}

/// Parse `key = 'value'`, `key = value`, or `key = ['a', 'b', ...]` token into
/// `(key, value)`.  For array-style values `[...]` the brackets are stripped so
/// callers receive the raw comma-separated interior (e.g. `'category', 'score'`).
fn parse_kv_token(token: &str) -> Option<(String, String)> {
    let eq_pos = token.find('=')?;
    let key = token[..eq_pos].trim().to_string();
    let val_raw = token[eq_pos + 1..].trim();

    // Array literal: strip outer `[` … `]` and pass the interior as the value.
    if val_raw.starts_with('[') {
        let inner = val_raw
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(val_raw)
            .trim();
        return Some((key, inner.to_string()));
    }

    let val = val_raw.trim_start_matches('\'').trim_start_matches('"');
    let end = val
        .find('\'')
        .or_else(|| val.find('"'))
        .unwrap_or(val.len());
    let value = val[..end].trim().to_string();
    Some((key, value))
}

/// Extract the raw inner text of a `BALANCED ON (group_key = col, ...)` clause.
///
/// Returns `None` when the clause is absent. The handler calls
/// `parse_balanced_clause_from_raw` with this string.
fn extract_balanced_raw(upper_body: &str, body: &str) -> Option<String> {
    let bal_pos = upper_body.find("BALANCED ON")?;
    let after = body[bal_pos + "BALANCED ON".len()..].trim_start();
    if !after.starts_with('(') {
        return None;
    }
    let mut depth = 0usize;
    let mut end = None;
    for (i, b) in after.bytes().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end?;
    Some(after[1..end].trim().to_string())
}

// ── ALTER COLLECTION sub-operation parsing ───────────────────────────────────

fn parse_alter_operation(upper: &str, parts: &[&str], trimmed: &str) -> Option<AlterCollectionOp> {
    // Operations handled exclusively by the collaborative dispatcher (raw-SQL path).
    // Return None so try_parse returns Ok(None), letting the router fall through.
    if upper.contains("ADD CONSTRAINT")
        || upper.contains("ADD PERIOD LOCK")
        || upper.contains("DROP PERIOD LOCK")
        || upper.contains("SET PERMISSION_TREE")
        || upper.contains("ADD TRANSITION CHECK")
    {
        return None;
    }

    // MATERIALIZED_SUM takes priority over ADD COLUMN.
    if upper.contains("MATERIALIZED_SUM") {
        return Some(AlterCollectionOp::AddMaterializedSum {
            raw_sql: trimmed.to_string(),
        });
    }

    if upper.contains("ADD COLUMN") || (upper.contains(" ADD ") && !upper.contains("MATERIALIZED"))
    {
        return parse_add_column(parts);
    }
    if upper.contains("DROP COLUMN") {
        return parse_drop_column(parts);
    }
    if upper.contains("RENAME COLUMN") {
        return parse_rename_column(parts);
    }
    if upper.contains("ALTER COLUMN") && upper.contains(" TYPE ") {
        return parse_alter_column_type(parts);
    }
    if upper.contains("OWNER TO") {
        let new_owner = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("TO"))
            .and_then(|i| parts.get(i + 1))
            .map(|s| s.to_string())?;
        return Some(AlterCollectionOp::OwnerTo { new_owner });
    }
    if upper.contains("SET RETENTION") {
        let value = extract_set_value(upper, "RETENTION")?;
        return Some(AlterCollectionOp::SetRetention { value });
    }
    if upper.contains("SET APPEND_ONLY") {
        return Some(AlterCollectionOp::SetAppendOnly);
    }
    if upper.contains("LAST_VALUE_CACHE") {
        let enabled =
            upper.contains("LAST_VALUE_CACHE = TRUE") || upper.contains("LAST_VALUE_CACHE=TRUE");
        return Some(AlterCollectionOp::SetLastValueCache { enabled });
    }
    if upper.contains("LEGAL_HOLD") {
        let enabled = upper.contains("LEGAL_HOLD = TRUE") || upper.contains("LEGAL_HOLD=TRUE");
        let tag = extract_tag_value(upper)?;
        return Some(AlterCollectionOp::SetLegalHold { enabled, tag });
    }
    // Fallback: unknown operation — store raw SQL so the legacy handler can attempt it.
    Some(AlterCollectionOp::AddMaterializedSum {
        raw_sql: trimmed.to_string(),
    })
}

fn parse_add_column(parts: &[&str]) -> Option<AlterCollectionOp> {
    // ALTER {COLLECTION|TABLE} <name> ADD [COLUMN] <col_name> <col_type> [NOT NULL] [DEFAULT expr]
    let add_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("ADD"))?;
    let col_start = if parts
        .get(add_idx + 1)
        .map(|p| p.eq_ignore_ascii_case("COLUMN"))
        .unwrap_or(false)
    {
        add_idx + 2
    } else {
        add_idx + 1
    };
    let column_name = parts.get(col_start)?.to_lowercase();
    let column_type = parts.get(col_start + 1)?.to_string();
    let not_null = parts[col_start..]
        .windows(2)
        .any(|w| w[0].eq_ignore_ascii_case("NOT") && w[1].eq_ignore_ascii_case("NULL"));
    let default_expr = parts[col_start..]
        .iter()
        .position(|p| p.eq_ignore_ascii_case("DEFAULT"))
        .and_then(|i| parts.get(col_start + i + 1))
        .map(|s| s.trim_end_matches(';').to_string());
    Some(AlterCollectionOp::AddColumn {
        column_name,
        column_type,
        not_null,
        default_expr,
    })
}

fn parse_drop_column(parts: &[&str]) -> Option<AlterCollectionOp> {
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))?;
    let column_name = parts.get(col_idx + 1)?.trim_end_matches(';').to_lowercase();
    Some(AlterCollectionOp::DropColumn { column_name })
}

fn parse_rename_column(parts: &[&str]) -> Option<AlterCollectionOp> {
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))?;
    let old_name = parts.get(col_idx + 1)?.to_lowercase();
    // Expect TO keyword.
    let to_tok = parts.get(col_idx + 2)?;
    if !to_tok.eq_ignore_ascii_case("TO") {
        return None;
    }
    let new_name = parts.get(col_idx + 3)?.trim_end_matches(';').to_lowercase();
    Some(AlterCollectionOp::RenameColumn { old_name, new_name })
}

fn parse_alter_column_type(parts: &[&str]) -> Option<AlterCollectionOp> {
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))?;
    let column_name = parts.get(col_idx + 1)?.to_lowercase();
    let type_idx = parts[col_idx..]
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TYPE"))
        .map(|i| col_idx + i)?;
    let new_type = parts.get(type_idx + 1)?.trim_end_matches(';').to_string();
    Some(AlterCollectionOp::AlterColumnType {
        column_name,
        new_type,
    })
}

fn extract_set_value(upper: &str, key: &str) -> Option<String> {
    let pattern = format!("{key} =");
    let pos = upper
        .find(&pattern)
        .or_else(|| upper.find(&format!("{key}=")))?;
    let after = upper[pos..].split('=').nth(1)?.trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .unwrap_or(value.len());
    let result = value[..end].to_string();
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

fn extract_tag_value(upper: &str) -> Option<String> {
    let pos = upper.find("TAG ")?;
    let after = upper[pos + 4..].trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .or_else(|| value.find(' '))
        .unwrap_or(value.len());
    if end == 0 {
        return None;
    }
    Some(value[..end].to_string())
}
