//! Parse CREATE INDEX / DROP INDEX / SHOW INDEX / REINDEX.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE UNIQUE INDEX ") || upper.starts_with("CREATE UNIQUE IND") {
            return Ok(Some(parse_create_index(true, upper, parts, trimmed)));
        }
        if upper.starts_with("CREATE INDEX ") {
            return Ok(Some(parse_create_index(false, upper, parts, trimmed)));
        }
        if upper.starts_with("DROP INDEX ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "INDEX") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::DropIndex {
                name,
                collection: None,
                if_exists,
            }));
        }
        if upper.starts_with("SHOW INDEX") {
            let collection = parts.get(2).map(|s| s.to_string());
            return Ok(Some(NodedbStatement::ShowIndexes { collection }));
        }
        if upper.starts_with("REINDEX ") {
            // Grammar: REINDEX [INDEX <name>] [CONCURRENTLY] <collection>
            //
            // Parsing strategy: walk parts starting at index 1.
            // Optional INDEX <name> occupies positions 1-2; optional CONCURRENTLY
            // is anywhere before the final token; the last token is the collection.
            let mut offset = 1usize;
            let mut index_name: Option<String> = None;
            let mut concurrent = false;

            // Skip optional TABLE keyword for backwards compat (REINDEX TABLE <coll>)
            if parts
                .get(offset)
                .map(|p| p.eq_ignore_ascii_case("TABLE"))
                .unwrap_or(false)
            {
                offset += 1;
            }

            // Consume optional INDEX <name>. The name is required when INDEX is
            // present and must not be the CONCURRENTLY keyword.
            if parts
                .get(offset)
                .map(|p| p.eq_ignore_ascii_case("INDEX"))
                .unwrap_or(false)
            {
                offset += 1;
                let name = parts.get(offset).ok_or_else(|| SqlError::Parse {
                    detail: "REINDEX INDEX requires an index name".to_string(),
                })?;
                if name.eq_ignore_ascii_case("CONCURRENTLY") {
                    return Err(SqlError::Parse {
                        detail: "REINDEX INDEX requires an index name before CONCURRENTLY"
                            .to_string(),
                    });
                }
                index_name = Some(name.to_lowercase());
                offset += 1;
            }

            // Consume optional CONCURRENTLY
            if parts
                .get(offset)
                .map(|p| p.eq_ignore_ascii_case("CONCURRENTLY"))
                .unwrap_or(false)
            {
                concurrent = true;
                offset += 1;
            }

            // Remaining token is the collection name
            let collection = match parts.get(offset) {
                None => return Ok(None),
                Some(s) => s.to_lowercase(),
            };

            return Ok(Some(NodedbStatement::Reindex {
                collection,
                index_name,
                concurrent,
            }));
        }
        Ok(None)
    })()
    .transpose()
}

/// Parse `CREATE [UNIQUE] INDEX [name] ON collection (field) [WHERE cond]
///         [COLLATE NOCASE]` into a typed `CreateIndex` variant.
fn parse_create_index(
    unique: bool,
    upper: &str,
    parts: &[&str],
    _trimmed: &str,
) -> NodedbStatement {
    // Skip "CREATE [UNIQUE] INDEX" prefix.
    let idx_offset: usize = if unique { 3 } else { 2 };

    // Detect whether an explicit name is present: if parts[idx_offset] is "ON",
    // the name was omitted; otherwise it is the index name.
    let (index_name, on_offset) = if parts
        .get(idx_offset)
        .map(|p| p.eq_ignore_ascii_case("ON"))
        .unwrap_or(false)
    {
        (None, idx_offset)
    } else {
        let name = parts
            .get(idx_offset)
            .map(|s| s.to_lowercase())
            .unwrap_or_default();
        (
            if name.is_empty() { None } else { Some(name) },
            idx_offset + 1,
        )
    };

    // parts[on_offset] should be "ON"; collection follows.
    let raw_collection_token = parts.get(on_offset + 1).copied().unwrap_or("");

    let (collection, field) = if let Some(paren_pos) = raw_collection_token.find('(') {
        // Inline form: `collection(field)`.
        let coll = raw_collection_token[..paren_pos].to_lowercase();
        let fld = raw_collection_token[paren_pos..]
            .trim_matches(|c| c == '(' || c == ')')
            .to_string();
        (coll, fld)
    } else if parts
        .get(on_offset + 2)
        .map(|p| p.eq_ignore_ascii_case("FIELDS"))
        .unwrap_or(false)
    {
        // FIELDS keyword form: ON collection FIELDS field
        let coll = raw_collection_token.to_lowercase();
        let fld = parts.get(on_offset + 3).copied().unwrap_or("").to_string();
        (coll, fld)
    } else {
        // Standard form: ON collection (field)
        let coll = raw_collection_token.to_lowercase();
        let fld = parts
            .get(on_offset + 2)
            .map(|s| s.trim_matches(|c| c == '(' || c == ')').to_string())
            .unwrap_or_default();
        (coll, fld)
    };

    // WHERE condition.
    let where_condition = if let Some(_pos) = upper.find(" WHERE ") {
        // Recompute position in original-case `_trimmed` ... but we have `upper` only.
        // Use byte offset from upper — safe because the WHERE keyword itself is ASCII.
        // The value after WHERE is the original-case remainder.
        // We need the original text; use `parts` to reconstruct.
        let where_tok_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("WHERE"));
        where_tok_idx.map(|i| parts[i + 1..].join(" ").trim_end_matches(';').to_string())
    } else {
        None
    };

    let case_insensitive = upper.contains("COLLATE NOCASE") || upper.contains("COLLATE CI");

    NodedbStatement::CreateIndex {
        unique,
        index_name,
        collection,
        field,
        case_insensitive,
        where_condition,
    }
}
