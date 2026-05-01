//! SQL identifier normalization.

use crate::error::{Result, SqlError};

/// Human-readable message for schema-qualified name rejections.
/// Defined once so all rejection sites produce consistent output.
pub const SCHEMA_QUALIFIED_MSG: &str = "schema-qualified names are not supported; NodeDB has no schema concept \
     — use 'users' not 'public.users'";

/// Normalize a SQL identifier: lowercase unquoted, preserve quoted.
pub fn normalize_ident(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Normalize a compound object name, rejecting schema-qualified forms.
///
/// Accepts a single-part name (plain identifier) and returns it normalized.
/// Rejects any name with more than one part (e.g. `public.users`,
/// `db.public.users`) with `SqlError::Unsupported`.
pub fn normalize_object_name_checked(name: &sqlparser::ast::ObjectName) -> Result<String> {
    if name.0.len() > 1 {
        // Build a human-readable representation of what was actually written.
        let qualified: String = name
            .0
            .iter()
            .map(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".");
        return Err(SqlError::Unsupported {
            detail: format!("'{qualified}': {SCHEMA_QUALIFIED_MSG}"),
        });
    }
    Ok(name
        .0
        .first()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .unwrap_or_default())
}

/// Extract table name and optional alias from a table factor.
///
/// Returns `Err` if the table name is schema-qualified.
pub fn table_name_from_factor(
    factor: &sqlparser::ast::TableFactor,
) -> Result<Option<(String, Option<String>)>> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            let table = normalize_object_name_checked(name)?;
            let alias_name = alias.as_ref().map(|a| normalize_ident(&a.name));
            Ok(Some((table, alias_name)))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;
    use sqlparser::ast::Statement;

    fn parse_object_name(sql: &str) -> sqlparser::ast::ObjectName {
        // Parse a `SELECT * FROM <name>` and extract the table ObjectName.
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        match &sel.from[0].relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name.clone(),
            other => panic!("expected table factor, got {other:?}"),
        }
    }

    #[test]
    fn plain_name_accepted() {
        let name = parse_object_name("SELECT * FROM users");
        assert_eq!(normalize_object_name_checked(&name).unwrap(), "users");
    }

    #[test]
    fn schema_qualified_two_parts_rejected() {
        let name = parse_object_name("SELECT * FROM public.users");
        let err = normalize_object_name_checked(&name).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("public.users") || msg.contains("schema-qualified"),
            "error should mention the qualified name or schema: {msg}"
        );
    }

    #[test]
    fn schema_qualified_three_parts_rejected() {
        // db.public.users — three-part name.
        // sqlparser may not parse this as a table name with three parts in all dialects,
        // but we can verify via a manually constructed ObjectName.
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
        let name = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("db")),
            ObjectNamePart::Identifier(Ident::new("public")),
            ObjectNamePart::Identifier(Ident::new("users")),
        ]);
        let err = normalize_object_name_checked(&name).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }
}
