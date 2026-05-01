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
            let operation = match parse_alter_operation(upper, parts, trimmed, &name) {
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

    // Reject unsupported SQL constraint keywords with typed errors and migration hints.
    let upper_name = raw_name.to_uppercase();
    match upper_name.as_str() {
        "PRIMARY" => {
            // Table-level `PRIMARY KEY (col)` clause: the column name is not present here,
            // so we cannot wire `is_pk` on a specific column. Reject with a hint to use the
            // inline form instead, which `parse_column_type_str_full` already handles.
            return Err(SqlError::UnsupportedConstraint {
                feature: "PRIMARY KEY".to_string(),
                hint: "use the inline form on the column instead: \
                       `<colname> <TYPE> PRIMARY KEY`"
                    .to_string(),
            });
        }
        "UNIQUE" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "UNIQUE constraint".to_string(),
                hint: "use a UNIQUE secondary index: \
                       CREATE INDEX ... ON collection (field) UNIQUE"
                    .to_string(),
            });
        }
        "CHECK" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "CHECK constraint".to_string(),
                hint: "CHECK constraints are unsupported; enforce in application code \
                       or use a typed function in INSERT"
                    .to_string(),
            });
        }
        "FOREIGN" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "FOREIGN KEY constraint".to_string(),
                hint: "FOREIGN KEY enforcement is unsupported; \
                       enforce in application code"
                    .to_string(),
            });
        }
        "REFERENCES" => {
            return Err(SqlError::UnsupportedConstraint {
                feature: "REFERENCES constraint".to_string(),
                hint: "FOREIGN KEY enforcement is unsupported; \
                       enforce in application code"
                    .to_string(),
            });
        }
        "CONSTRAINT" => {
            // Named constraint: peek at the next token to determine kind.
            let mut rest = toks.clone();
            let _constraint_name = rest.next(); // skip the constraint name
            let kind_tok = rest.next().map(|t| t.to_uppercase()).unwrap_or_default();
            let (feature, hint) = match kind_tok.as_str() {
                "PRIMARY" => (
                    "CONSTRAINT ... PRIMARY KEY".to_string(),
                    "use the inline form on the column instead: \
                     `<colname> <TYPE> PRIMARY KEY`"
                        .to_string(),
                ),
                "UNIQUE" => (
                    "CONSTRAINT ... UNIQUE".to_string(),
                    "use a UNIQUE secondary index: \
                     CREATE INDEX ... ON collection (field) UNIQUE"
                        .to_string(),
                ),
                "CHECK" => (
                    "CONSTRAINT ... CHECK".to_string(),
                    "CHECK constraints are unsupported; enforce in application code \
                     or use a typed function in INSERT"
                        .to_string(),
                ),
                "FOREIGN" => (
                    "CONSTRAINT ... FOREIGN KEY".to_string(),
                    "FOREIGN KEY enforcement is unsupported; \
                     enforce in application code"
                        .to_string(),
                ),
                _ => (
                    format!("CONSTRAINT {}", kind_tok),
                    "named constraints are unsupported; \
                     use NodeDB-native enforcement (indexes, typeguards)"
                        .to_string(),
                ),
            };
            return Err(SqlError::UnsupportedConstraint { feature, hint });
        }
        _ => {}
    }

    // Validate that the column name is not a reserved identifier.
    let name = check_identifier(raw_name)?;

    // Collect the column definition (bare type + modifiers like NOT NULL, DEFAULT expr,
    // TIME_KEY, SPATIAL_INDEX). Downstream builders (build_strict_schema,
    // build_kv_collection_type, etc.) each strip to the bare type as needed via
    // parse_column_type_str.
    //
    // Inline constraint keywords (PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY, REFERENCES,
    // CONSTRAINT) appearing after the type are rejected with typed errors — they are
    // never silently absorbed into the type string.
    let mut type_parts: Vec<&str> = Vec::new();
    let mut in_paren = false;
    let mut hit_generated = false;
    for t in toks {
        let upper_t = t.to_uppercase();
        let stripped = upper_t.trim_end_matches(['(', ')', ',']);
        // GENERATED is NOT stopped here: we pass the raw text through so that
        // `build_strict_schema` can detect and store the generated expression.
        if !in_paren && stripped == "GENERATED" {
            hit_generated = true;
            // Stop the word-by-word iteration here.  We will append the original
            // raw text from "GENERATED" onwards below, preserving spaces inside
            // expressions like GENERATED ALWAYS AS ('café' || city).
            break;
        }
        // Reject inline constraint keywords — same error family as table-level constraints.
        // Note: "PRIMARY" (inline `col TYPE PRIMARY KEY`) is intentionally NOT rejected here;
        // it flows through to `parse_column_type_str_full` which extracts `is_pk` correctly.
        if !in_paren {
            match stripped {
                "UNIQUE" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "UNIQUE constraint".to_string(),
                        hint: "use a UNIQUE secondary index: \
                               CREATE INDEX ... ON collection (field) UNIQUE"
                            .to_string(),
                    });
                }
                "CHECK" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "CHECK constraint".to_string(),
                        hint: "CHECK constraints are unsupported; enforce in application code \
                               or use a typed function in INSERT"
                            .to_string(),
                    });
                }
                "FOREIGN" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "FOREIGN KEY constraint".to_string(),
                        hint: "FOREIGN KEY enforcement is unsupported; \
                               enforce in application code"
                            .to_string(),
                    });
                }
                "REFERENCES" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "REFERENCES constraint".to_string(),
                        hint: "FOREIGN KEY enforcement is unsupported; \
                               enforce in application code"
                            .to_string(),
                    });
                }
                "CONSTRAINT" => {
                    return Err(SqlError::UnsupportedConstraint {
                        feature: "CONSTRAINT clause".to_string(),
                        hint: "named constraints are unsupported; \
                               use NodeDB-native enforcement (indexes, typeguards)"
                            .to_string(),
                    });
                }
                _ => {}
            }
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

fn parse_alter_operation(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
    collection_name: &str,
) -> Option<AlterCollectionOp> {
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
        return parse_materialized_sum(upper, parts, trimmed, collection_name);
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
    None
}

/// Parse `ALTER COLLECTION <name> ADD [COLUMN] <col> ... MATERIALIZED_SUM SOURCE <src>
/// ON <join> VALUE <expr>` into a typed [`AlterCollectionOp::AddMaterializedSum`].
///
/// Returns `None` if any required keyword is absent — the router will surface a
/// parse error to the client.
fn parse_materialized_sum(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
    collection_name: &str,
) -> Option<AlterCollectionOp> {
    // Target column: token after ADD [COLUMN].
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .or_else(|| parts.iter().position(|p| p.eq_ignore_ascii_case("ADD")))?;
    let target_column = parts.get(col_idx + 1)?.to_lowercase();

    // Source collection: token after SOURCE.
    let source_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("SOURCE"))?;
    let source_collection = parts.get(source_idx + 1)?.to_lowercase();

    // Join column: extract from ON clause `source.col = target.id`.
    let on_pos = upper.find(" ON ")?;
    let after_on = &trimmed[on_pos + 4..];
    let join_column = extract_join_column(after_on, &source_collection)?;

    // Value expression: token(s) after VALUE keyword.
    let value_pos = upper.find(" VALUE ")?;
    let value_expr_raw = trimmed[value_pos + 7..].trim().trim_end_matches(';');
    let value_expr = extract_value_expr(value_expr_raw, &source_collection)?;

    Some(AlterCollectionOp::AddMaterializedSum {
        target_collection: collection_name.to_lowercase(),
        target_column,
        source_collection,
        join_column,
        value_expr,
    })
}

/// Extract the join column from `source.col = target.id` — returns the source side column.
fn extract_join_column(join_clause: &str, source_coll: &str) -> Option<String> {
    let eq_parts: Vec<&str> = join_clause.splitn(2, '=').collect();
    if eq_parts.len() != 2 {
        return None;
    }
    let left = eq_parts[0].trim().to_lowercase();
    let right = eq_parts[1].trim().to_lowercase();

    let prefix = format!("{source_coll}.");
    let col = if left.starts_with(&prefix) {
        left.strip_prefix(&prefix).unwrap_or(&left).to_string()
    } else if right.starts_with(&prefix) {
        right.strip_prefix(&prefix).unwrap_or(&right).to_string()
    } else {
        left.split('.').next_back().unwrap_or(&left).to_string()
    };

    Some(col.split_whitespace().next().unwrap_or(&col).to_string())
}

/// Extract value expression — simple column reference or qualified `source.column`.
/// Returns `None` for complex expressions that require a pre-computed column.
fn extract_value_expr(expr_str: &str, source_coll: &str) -> Option<String> {
    let lower = expr_str.trim().to_lowercase();
    let prefix = format!("{source_coll}.");
    let col_name = if lower.starts_with(&prefix) {
        lower.strip_prefix(&prefix).unwrap_or(&lower).to_string()
    } else {
        lower.to_string()
    };

    if col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Some(col_name)
    } else {
        // Complex expression — caller must use a pre-computed column.
        None
    }
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
