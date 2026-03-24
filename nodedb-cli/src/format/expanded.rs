//! Expanded (vertical) display format, like psql's `\x` mode.
//!
//! Each row displays as a record with one field per line:
//! ```text
//! -[ RECORD 1 ]----
//! id    | u1
//! name  | Alice
//! age   | 30
//! ```

use nodedb_types::result::QueryResult;

use super::table::display_value;

/// Format a QueryResult in expanded/vertical mode.
pub fn format(qr: &QueryResult) -> String {
    if qr.columns.is_empty() && qr.rows.is_empty() {
        if qr.rows_affected > 0 {
            return format!("({} row(s) affected)\n", qr.rows_affected);
        }
        return "(empty result)\n".to_string();
    }

    let max_col_width = qr.columns.iter().map(|c| c.len()).max().unwrap_or(0);

    let mut out = String::new();

    for (row_idx, row) in qr.rows.iter().enumerate() {
        // Record header.
        let header = format!("-[ RECORD {} ]", row_idx + 1);
        let pad = 40usize.saturating_sub(header.len());
        out.push_str(&header);
        out.push_str(&"-".repeat(pad));
        out.push('\n');

        // Fields.
        for (col_idx, val) in row.iter().enumerate() {
            let col_name = qr.columns.get(col_idx).map(|s| s.as_str()).unwrap_or("?");
            let val_str = display_value(val);
            let pad = max_col_width.saturating_sub(col_name.len());
            out.push_str(col_name);
            out.push_str(&" ".repeat(pad));
            out.push_str(" | ");
            out.push_str(&val_str);
            out.push('\n');
        }
    }

    out.push_str(&format!("({} row(s))\n", qr.rows.len()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::value::Value;

    #[test]
    fn expanded_two_rows() {
        let qr = QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Value::String("u1".into()), Value::String("Alice".into())],
                vec![Value::String("u2".into()), Value::String("Bob".into())],
            ],
            rows_affected: 0,
        };
        let out = format(&qr);
        assert!(out.contains("-[ RECORD 1 ]"));
        assert!(out.contains("-[ RECORD 2 ]"));
        assert!(out.contains("id   | u1"));
        assert!(out.contains("name | Alice"));
        assert!(out.contains("(2 row(s))"));
    }

    #[test]
    fn expanded_empty() {
        let qr = QueryResult::empty();
        assert!(format(&qr).contains("empty result"));
    }
}
