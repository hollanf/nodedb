//! Parse CREATE/DROP/ALTER/SHOW ALERT + SHOW ALERT STATUS.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE ALERT ") {
            return Ok(Some(parse_create_alert(upper, trimmed)));
        }
        if upper.starts_with("DROP ALERT ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "ALERT") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::DropAlert { name, if_exists }));
        }
        if upper.starts_with("ALTER ALERT ") {
            // ALTER ALERT <name> ENABLE|DISABLE
            let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
            let action = parts.get(3).map(|s| s.to_uppercase()).unwrap_or_default();
            return Ok(Some(NodedbStatement::AlterAlert { name, action }));
        }
        if upper.starts_with("SHOW ALERT STATUS ") {
            let name = match parts.get(3) {
                None => return Ok(None),
                Some(s) => s.to_string(),
            };
            return Ok(Some(NodedbStatement::ShowAlertStatus { name }));
        }
        if upper.starts_with("SHOW ALERT") && !upper.starts_with("SHOW ALERT STATUS") {
            return Ok(Some(NodedbStatement::ShowAlerts));
        }
        Ok(None)
    })()
    .transpose()
}

/// Structural extraction for `CREATE ALERT`.
///
/// Extracts name, collection, optional WHERE filter, raw condition text,
/// GROUP BY columns, WINDOW duration string, fire/recover counts, severity,
/// and raw NOTIFY section text. Complex types (AlertCondition, NotifyTarget)
/// are built by the handler from these strings.
fn parse_create_alert(upper: &str, trimmed: &str) -> NodedbStatement {
    let prefix = "CREATE ALERT ";
    let after_prefix = &trimmed[prefix.len()..];
    let upper_after = &upper[prefix.len()..];

    // name = first whitespace token
    let name = after_prefix
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_lowercase();

    // collection = token after " ON "
    let collection = upper_after
        .find(" ON ")
        .map(|pos| {
            after_prefix[pos + 4..]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_lowercase()
        })
        .unwrap_or_default();

    // WHERE filter = text between "WHERE " and "CONDITION "
    let where_filter = extract_between(upper, trimmed, "WHERE ", "CONDITION ");

    // condition_raw = text after "CONDITION " up to next major clause boundary
    let condition_raw = extract_clause_text(
        upper,
        trimmed,
        "CONDITION ",
        &[
            "GROUP BY ",
            "WINDOW ",
            "FOR ",
            "RECOVER ",
            "SEVERITY ",
            "NOTIFY",
        ],
    )
    .unwrap_or_default();

    // GROUP BY columns
    let group_by = extract_group_by(upper, trimmed);

    // WINDOW duration string (contents of first single-quoted token after WINDOW)
    let window_raw = extract_quoted_after(upper, trimmed, "WINDOW ").unwrap_or_default();

    // FOR 'N consecutive windows'
    let fire_after = extract_consecutive_count(upper, "FOR ").unwrap_or(1).max(1);

    // RECOVER AFTER 'N consecutive windows'
    let recover_after = extract_consecutive_count(upper, "RECOVER AFTER ")
        .unwrap_or(1)
        .max(1);

    // SEVERITY '<level>'
    let severity =
        extract_quoted_after(upper, trimmed, "SEVERITY ").unwrap_or_else(|| "warning".to_string());

    // Raw NOTIFY section (everything after NOTIFY keyword)
    let notify_targets_raw = upper
        .find("NOTIFY")
        .map(|pos| {
            trimmed[pos + 6..]
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_string()
        })
        .unwrap_or_default();

    NodedbStatement::CreateAlert {
        name,
        collection,
        where_filter,
        condition_raw,
        group_by,
        window_raw,
        fire_after,
        recover_after,
        severity,
        notify_targets_raw,
    }
}

/// Extract text between two keyword boundaries (case-insensitive search, original-case result).
fn extract_between(upper: &str, sql: &str, start_kw: &str, end_kw: &str) -> Option<String> {
    let start = upper.find(start_kw)?;
    let after_start = start + start_kw.len();
    let end = upper[after_start..].find(end_kw)?;
    let text = sql[after_start..after_start + end].trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Extract text after `start_kw` up to the nearest of the `end_kws` boundaries.
fn extract_clause_text(upper: &str, sql: &str, start_kw: &str, end_kws: &[&str]) -> Option<String> {
    let start = upper.find(start_kw)?;
    let after = start + start_kw.len();
    let end = end_kws
        .iter()
        .filter_map(|kw| upper[after..].find(kw))
        .min()
        .unwrap_or(upper.len() - after);
    let text = sql[after..after + end].trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Extract GROUP BY column list (lowercased).
fn extract_group_by(upper: &str, sql: &str) -> Vec<String> {
    let kw = "GROUP BY ";
    let pos = match upper.find(kw) {
        Some(p) => p,
        None => return Vec::new(),
    };
    let after = &sql[pos + kw.len()..];
    // End at next major keyword
    let end_kws = ["WINDOW ", "FOR ", "RECOVER ", "SEVERITY ", "NOTIFY"];
    let end = end_kws
        .iter()
        .filter_map(|kw| upper[pos + 9..].find(kw))
        .min()
        .unwrap_or(after.len());
    after[..end]
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Extract a single-quoted value after a keyword.
fn extract_quoted_after(upper: &str, sql: &str, keyword: &str) -> Option<String> {
    let pos = upper.find(keyword)?;
    let after = &sql[pos + keyword.len()..];
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

/// Extract 'N consecutive windows' count after a keyword.
fn extract_consecutive_count(upper: &str, keyword: &str) -> Option<u32> {
    let pos = upper.find(keyword)?;
    let after = &upper[pos + keyword.len()..];
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    let inner = &after[start + 1..start + 1 + end];
    inner
        .split_whitespace()
        .next()
        .and_then(|n| n.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_alert_full() {
        let sql = "CREATE ALERT high_temp ON sensors \
                   WHERE device_type = 'compressor' \
                   CONDITION AVG(temperature) > 90.0 \
                   GROUP BY device_id \
                   WINDOW '5 minutes' \
                   FOR '3 consecutive windows' \
                   RECOVER AFTER '2 consecutive windows' \
                   SEVERITY 'critical' \
                   NOTIFY TOPIC 'alerts', WEBHOOK 'https://ops.example.com/alerts'";
        let upper = sql.to_uppercase();
        let stmt = parse_create_alert(&upper, sql);
        if let NodedbStatement::CreateAlert {
            name,
            collection,
            where_filter,
            condition_raw,
            group_by,
            window_raw,
            fire_after,
            recover_after,
            severity,
            notify_targets_raw,
        } = stmt
        {
            assert_eq!(name, "high_temp");
            assert_eq!(collection, "sensors");
            assert!(where_filter.as_deref().unwrap_or("").contains("compressor"));
            assert!(condition_raw.to_uppercase().contains("AVG(TEMPERATURE)"));
            assert_eq!(group_by, vec!["device_id"]);
            assert_eq!(window_raw, "5 minutes");
            assert_eq!(fire_after, 3);
            assert_eq!(recover_after, 2);
            assert_eq!(severity, "critical");
            assert!(notify_targets_raw.contains("alerts"));
        } else {
            panic!("expected CreateAlert");
        }
    }

    #[test]
    fn parse_create_alert_minimal() {
        let sql = "CREATE ALERT simple ON metrics CONDITION MAX(cpu) > 95.0 WINDOW '1 minute' SEVERITY 'warning'";
        let upper = sql.to_uppercase();
        let stmt = parse_create_alert(&upper, sql);
        if let NodedbStatement::CreateAlert {
            name,
            collection,
            where_filter,
            fire_after,
            recover_after,
            notify_targets_raw,
            ..
        } = stmt
        {
            assert_eq!(name, "simple");
            assert_eq!(collection, "metrics");
            assert!(where_filter.is_none());
            assert_eq!(fire_after, 1);
            assert_eq!(recover_after, 1);
            assert!(notify_targets_raw.is_empty());
        } else {
            panic!("expected CreateAlert");
        }
    }
}
