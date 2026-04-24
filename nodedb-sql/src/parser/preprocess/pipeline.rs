//! SQL pre-processing orchestrator: rewrite NodeDB-specific syntax into
//! standard SQL before handing to sqlparser-rs.
//!
//! Handles:
//! - `UPSERT INTO coll (cols) VALUES (vals)` → `INSERT INTO ...` + upsert flag
//! - `INSERT INTO coll { key: 'val', ... }` → `INSERT INTO coll (key) VALUES ('val')`
//! - `UPSERT INTO coll { ... }` → both rewrites combined
//! - `expr <-> expr` → `vector_distance(expr, expr)`
//! - `{ key: val }` in function args → JSON string literal

use super::function_args::rewrite_object_literal_args;
use super::object_literal_stmt::try_rewrite_object_literal;
use super::temporal::extract as extract_temporal;
use super::vector_ops::rewrite_arrow_distance;
use crate::error::SqlError;
use crate::temporal::TemporalScope;

/// Result of pre-processing a SQL string.
pub struct PreprocessedSql {
    /// The rewritten SQL (standard SQL that sqlparser can handle).
    pub sql: String,
    /// Whether the original statement was UPSERT (not INSERT).
    pub is_upsert: bool,
    /// Bitemporal qualifier extracted from `FOR SYSTEM_TIME` /
    /// `FOR VALID_TIME` / `__system_as_of__(...)`. Default when none
    /// was present.
    pub temporal: TemporalScope,
}

/// Pre-process a SQL string, rewriting NodeDB-specific syntax.
///
/// Returns `Ok(None)` if no rewriting was needed. Temporal clause parse
/// errors bubble up as `SqlError::Parse` so they surface to the caller
/// identically to sqlparser errors.
pub fn preprocess(sql: &str) -> Result<Option<PreprocessedSql>, SqlError> {
    let trimmed = sql.trim();

    // Extract temporal clauses first — they can appear in both SELECT and
    // INSERT...SELECT, and stripping them before the UPSERT/object-literal
    // rewrites keeps those rewriters pattern-free of NodeDB extensions.
    let (temporal_sql, temporal) =
        match extract_temporal(trimmed).map_err(|e| SqlError::Parse { detail: e.0 })? {
            Some(ex) => (ex.sql, ex.temporal),
            None => (trimmed.to_string(), TemporalScope::default()),
        };
    let any_temporal = temporal != TemporalScope::default();
    let upper = temporal_sql.to_uppercase();

    let is_upsert = upper.starts_with("UPSERT INTO ");

    if is_upsert {
        let rewritten = format!("INSERT INTO {}", &temporal_sql["UPSERT INTO ".len()..]);
        if let Some(result) = try_rewrite_object_literal(&rewritten) {
            return Ok(Some(PreprocessedSql {
                sql: result,
                is_upsert: true,
                temporal,
            }));
        }
        return Ok(Some(PreprocessedSql {
            sql: rewritten,
            is_upsert: true,
            temporal,
        }));
    }

    if upper.starts_with("INSERT INTO ")
        && let Some(result) = try_rewrite_object_literal(&temporal_sql)
    {
        return Ok(Some(PreprocessedSql {
            sql: result,
            is_upsert: false,
            temporal,
        }));
    }

    let mut sql_buf = temporal_sql;
    let mut any_rewrite = any_temporal;

    if sql_buf.contains("<->")
        && let Some(rewritten) = rewrite_arrow_distance(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if (sql_buf.contains("{ ") || sql_buf.contains("{f") || sql_buf.contains("{d"))
        && let Some(rewritten) = rewrite_object_literal_args(&sql_buf)
    {
        sql_buf = rewritten;
        any_rewrite = true;
    }

    if any_rewrite {
        return Ok(Some(PreprocessedSql {
            sql: sql_buf,
            is_upsert: false,
            temporal,
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::super::function_args::rewrite_object_literal_args;
    use super::*;

    fn pp(sql: &str) -> Option<PreprocessedSql> {
        super::preprocess(sql).unwrap()
    }

    #[test]
    fn passthrough_standard_sql() {
        assert!(pp("SELECT * FROM users").is_none());
        assert!(pp("INSERT INTO users (name) VALUES ('alice')").is_none());
        assert!(pp("DELETE FROM users WHERE id = 1").is_none());
    }

    #[test]
    fn upsert_rewrite() {
        let result = pp("UPSERT INTO users (name) VALUES ('alice')").unwrap();
        assert!(result.is_upsert);
        assert_eq!(result.sql, "INSERT INTO users (name) VALUES ('alice')");
    }

    #[test]
    fn object_literal_insert() {
        let result = pp("INSERT INTO users { name: 'alice', age: 30 }").unwrap();
        assert!(!result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("30"));
    }

    #[test]
    fn object_literal_upsert() {
        let result = pp("UPSERT INTO users { name: 'bob' }").unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.starts_with("INSERT INTO users ("));
        assert!(result.sql.contains("'bob'"));
    }

    #[test]
    fn batch_array_insert() {
        let result =
            pp("INSERT INTO users [{ name: 'alice', age: 30 }, { name: 'bob', age: 25 }]").unwrap();
        assert!(!result.is_upsert);
        assert!(result.sql.contains("VALUES"));
        assert!(result.sql.contains("'alice'"));
        assert!(result.sql.contains("'bob'"));
        assert!(result.sql.contains("30"));
        assert!(result.sql.contains("25"));
        let values_part = result.sql.split("VALUES").nth(1).unwrap();
        let row_count = values_part.matches('(').count();
        assert_eq!(row_count, 2, "should have 2 row groups: {}", result.sql);
    }

    #[test]
    fn batch_array_heterogeneous_keys() {
        let result =
            pp("INSERT INTO docs [{ id: 'a', name: 'Alice' }, { id: 'b', role: 'admin' }]")
                .unwrap();
        assert!(result.sql.contains("NULL"));
        assert!(result.sql.contains("'Alice'"));
        assert!(result.sql.contains("'admin'"));
    }

    #[test]
    fn batch_array_upsert() {
        let result =
            pp("UPSERT INTO users [{ id: 'u1', name: 'a' }, { id: 'u2', name: 'b' }]").unwrap();
        assert!(result.is_upsert);
        assert!(result.sql.contains("VALUES"));
    }

    #[test]
    fn arrow_distance_operator_select() {
        let result =
            pp("SELECT title FROM articles ORDER BY embedding <-> ARRAY[0.1, 0.2, 0.3] LIMIT 5")
                .unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[0.1, 0.2, 0.3])"),
            "got: {}",
            result.sql
        );
        assert!(!result.sql.contains("<->"));
    }

    #[test]
    fn arrow_distance_operator_where() {
        let result = pp("SELECT * FROM docs WHERE embedding <-> ARRAY[1.0, 2.0] < 0.5").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[1.0, 2.0])"),
            "got: {}",
            result.sql
        );
    }

    #[test]
    fn arrow_distance_no_match() {
        assert!(pp("SELECT * FROM users WHERE age > 30").is_none());
    }

    #[test]
    fn arrow_distance_with_alias() {
        let result = pp("SELECT embedding <-> ARRAY[0.1, 0.2] AS dist FROM articles").unwrap();
        assert!(
            result
                .sql
                .contains("vector_distance(embedding, ARRAY[0.1, 0.2]) AS dist"),
            "got: {}",
            result.sql
        );
    }

    #[test]
    fn fuzzy_object_literal_in_function() {
        let direct = rewrite_object_literal_args(
            "SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })",
        );
        assert!(direct.is_some(), "rewrite_object_literal_args should match");
        let rewritten = direct.unwrap();
        assert!(
            rewritten.contains("\"fuzzy\""),
            "direct rewrite should contain JSON, got: {}",
            rewritten
        );

        let result =
            pp("SELECT * FROM articles WHERE text_match(body, 'query', { fuzzy: true })").unwrap();
        assert!(
            !result.sql.contains("{ fuzzy"),
            "should not contain object literal, got: {}",
            result.sql
        );
    }

    #[test]
    fn fuzzy_object_literal_with_distance() {
        let result = pp(
            "SELECT * FROM articles WHERE text_match(title, 'test', { fuzzy: true, distance: 2 })",
        )
        .unwrap();
        assert!(result.sql.contains("\"fuzzy\""), "got: {}", result.sql);
        assert!(result.sql.contains("\"distance\""), "got: {}", result.sql);
    }

    #[test]
    fn object_literal_not_rewritten_outside_function() {
        let result = pp("INSERT INTO docs { name: 'Alice' }").unwrap();
        assert!(result.sql.contains("VALUES"), "got: {}", result.sql);
    }
}
