//! SQL identifier normalization.

/// Normalize a SQL identifier: lowercase unquoted, preserve quoted.
pub fn normalize_ident(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Normalize a compound object name (e.g., `schema.table`) to its last part.
pub fn normalize_object_name(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .last()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .unwrap_or_default()
}

/// Extract table name and optional alias from a table factor.
pub fn table_name_from_factor(
    factor: &sqlparser::ast::TableFactor,
) -> Option<(String, Option<String>)> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            let table = normalize_object_name(name);
            let alias_name = alias.as_ref().map(|a| normalize_ident(&a.name));
            Some((table, alias_name))
        }
        _ => None,
    }
}
